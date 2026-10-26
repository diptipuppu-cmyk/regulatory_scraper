'''
Shared scraper logic for every regulator.

A regulator scraper (for example MasScraper) only has to answer one question:
which items fall inside the scan period for this category ? It does that in discover().
Everything else is the same for every regulator and lives here:

  1. Browser setup and the pre flight browser check
  2. Retries, polite delay and the circuit breaker
  3. Opening item pages, collecting PDF links or an HTML snapshot
  4. Download, validation, the NEW, CHANGED or SKIPPED decision, versioned writes
  5. pdf_catalog and scraping_audit_log writes
  6. The run loop over categories, with CATEGORY rows and the final status

To add a regulator: write scrapers/<regulator>_scraper.py with a subclass that implements
validate_category() and discover(), add config/regulators/<regulator>_config.json, and
register both in config/common_config.json.
'''

import asyncio
import html
import json
from dataclasses import dataclass, asdict
from urllib.parse import urlsplit

from playwright.async_api import async_playwright
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from scrapers.tracking import (
    NEW, SKIPPED, SUCCESS, PARTIAL_SUCCESS, FAILED, DUPLICATE_IN_RUN,
    classify_document, make_document_id, normalize_url, normalize_text, now_sgt,
    remove_quietly, safe_file_name, sha256_bytes, versioned_path, write_file_verified, get_logger,
)

KNOWN_BROWSER_PATHS = ('/databricks/playwright-browsers', '/root/.cache/ms-playwright')

USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

RETRYABLE_HTTP = {429, 500, 502, 503, 504}


# =====================================================================
# Browser setup
# =====================================================================

def configure_browser_path():
    '''Point Playwright at the folder where the init script installed Chromium.
    Playwright reads PLAYWRIGHT_BROWSERS_PATH when it starts. If the cluster environment
    variable is missing, use the first known folder that contains a Chromium build.
    Returns the folder in use.'''
    import glob
    import os
    current = os.environ.get('PLAYWRIGHT_BROWSERS_PATH')
    candidates = ([current] if current else []) + [p for p in KNOWN_BROWSER_PATHS if p != current]
    for folder in candidates:
        if glob.glob(os.path.join(folder, 'chromium*')):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = folder
            return folder
    raise RuntimeError(
        'No Chromium build found in ' + ', '.join(candidates) + '. The init script '
        'scripts/playwright_init.sh did not run or failed: check that it is attached to the cluster, '
        'restart the cluster, and read the init script logs.')


async def check_browser():
    '''Launch and close Chromium once. Used as a pre flight check.'''
    folder = configure_browser_path()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        version = browser.version
        await browser.close()
    return folder, version


# =====================================================================
# Errors and counters
# =====================================================================

class RetryableError(Exception):
    '''Temporary problem (timeout, HTTP 429 or 5xx, page not rendered). Retried with backoff.'''


class PermanentError(Exception):
    '''Problem a retry will not fix (HTTP 404, invalid content, page layout changed).'''

    def __init__(self, message, category='PERMANENT'):
        super().__init__(message)
        self.category = category


@dataclass
class Counters:
    items_found: int = 0
    items_failed: int = 0
    documents_found: int = 0
    documents_new: int = 0
    documents_changed: int = 0
    documents_skipped: int = 0
    documents_failed: int = 0

    def add(self, other):
        for key, value in asdict(other).items():
            setattr(self, key, getattr(self, key) + value)

    def audit_fields(self):
        return {k: v for k, v in asdict(self).items() if k != 'items_failed'}


# =====================================================================
# Pure helpers shared by all regulators
# =====================================================================

def filter_pdf_links(links, category, base_url):
    '''Keep PDF links that pass the category keyword filters, one per normalised URL,
    and only on the regulator's own host.'''
    include = [k.lower() for k in category.get('pdf_include_keywords') or []]
    exclude = [k.lower() for k in category.get('pdf_exclude_keywords') or []]
    allowed_host = urlsplit(base_url).netloc.lower()
    kept, seen = [], set()
    for link in links:
        url = normalize_url(link.get('href') or '', base_url)
        parts = urlsplit(url)
        if parts.netloc != allowed_host or not parts.path.lower().endswith('.pdf'):
            continue
        haystack = (url + ' ' + (link.get('text') or '')).lower()
        if include and not any(k in haystack for k in include):
            continue
        if exclude and any(k in haystack for k in exclude):
            continue
        if url in seen:
            continue
        seen.add(url)
        kept.append({'url': url, 'text': ' '.join((link.get('text') or '').split())[:500]})
    return kept


def build_html_snapshot(item, body_html):
    '''Self contained HTML file holding the page body plus traceability metadata.'''
    title = html.escape(item['title'])
    url = html.escape(item['url'])
    return (
        '<!doctype html>\n<html lang=\'en\'><head><meta charset=\'utf8\'>'
        f'<title>{title}</title>'
        f'<meta name=\'source_url\' content=\'{url}\'>'
        f'<meta name=\'item_date\' content=\'{item["item_date"].isoformat()}\'>'
        f'<meta name=\'item_date_label\' content=\'{html.escape(item["item_date_label"])}\'>'
        '</head><body>'
        f'<h1>{title}</h1><p>Source: <a href=\'{url}\'>{url}</a></p>'
        f'{body_html}'
        '</body></html>'
    ).encode('utf8')


def html_file_name(item_url):
    slug = urlsplit(item_url).path.rstrip('/').split('/')[-1]
    return safe_file_name(slug, default='page') + '.html'


_PAGE_BODY_SCRIPT = '''(args) => {
    const [selectors, excluded] = args;
    for (const sel of selectors) {
        const nodes = [...document.querySelectorAll(sel)]
            .map(n => {
                const copy = n.cloneNode(true);
                for (const ex of excluded) { copy.querySelectorAll(ex).forEach(e => e.remove()); }
                return copy;
            })
            .filter(n => (n.textContent || '').trim().length > 0);
        if (nodes.length) {
            return {selector: sel,
                    html: nodes.map(n => n.innerHTML).join('\\n'),
                    text: nodes.map(n => n.textContent).join('\\n')};
        }
    }
    return null;
}'''

_LINKS_SCRIPT = 'els => els.map(a => ({href: a.href, text: (a.innerText || a.textContent || \'\')}))'


# =====================================================================
# Base scraper
# =====================================================================

class BaseScraper:
    '''Subclasses implement validate_category() and discover(). See the module docstring.'''

    # ---- config ----------------------------------------------------------

    @classmethod
    def validate_category(cls, category, base_host):
        '''Regulator specific checks for one category. Returns a list of problems.'''
        raise NotImplementedError

    @classmethod
    def validate_config(cls, config):
        '''Shared checks for every regulator config, plus the subclass category checks.'''
        problems = []
        for key in ('regulator_id', 'base_url', 'landing_url', 'categories'):
            if not config.get(key):
                problems.append(f'missing {key}')
        base_host = urlsplit(config.get('base_url') or '').netloc.lower()
        seen_ids = set()
        for index, category in enumerate(config.get('categories') or []):
            label = category.get('category_id') or f'categories[{index}]'
            for key in ('category_id', 'category_name'):
                if not category.get(key):
                    problems.append(f'{label}: missing {key}')
            if category.get('category_id') in seen_ids:
                problems.append(f'{label}: duplicate category_id')
            seen_ids.add(category.get('category_id'))
            if not category.get('download_pdfs', True) and not category.get('capture_html', False):
                problems.append(f'{label}: download_pdfs and capture_html are both false')
            problems += [f'{label}: {p}' for p in cls.validate_category(category, base_host)]
        for key in ('max_pages_per_category', 'max_retries'):
            if key in config and int(config[key]) < 1:
                problems.append(f'{key} must be at least 1')
        if problems:
            raise ValueError(f'Invalid scraper config for {config.get("regulator_id")}: ' + '; '.join(problems))
        return config

    @classmethod
    def load_config(cls, path):
        try:
            with open(path, 'r', encoding='utf8') as handle:
                config = json.load(handle)
        except FileNotFoundError:
            raise ValueError(f'Regulator config not found: {path}')
        except json.JSONDecodeError as error:
            raise ValueError(f'Regulator config {path} is not valid JSON: {error}')
        return cls.validate_config(config)

    # ---- discovery (regulator specific) ------------------------------------

    async def discover(self, page, category, scan_start, scan_end):
        '''Return (items, warning). Each item is a dict with url, title, item_date,
        item_date_label and item_reference. warning is None, or a note that is stored in
        error_details of the CATEGORY row without failing the category.'''
        raise NotImplementedError

    # ---- setup ---------------------------------------------------------------

    def __init__(self, config, run_id, raw_volume_path, catalog_repo, audit, logger=None):
        self.config = config
        self.regulator_id = config['regulator_id']
        self.base_url = config['base_url']
        self.run_id = run_id
        self.raw_volume_path = raw_volume_path.rstrip('/')
        self.catalog = catalog_repo
        self.audit = audit
        self.logger = logger or get_logger()

        self.delay = float(config.get('request_delay_seconds', 1.0))
        self.nav_timeout = int(config.get('navigation_timeout_ms', 60000))
        self.render_timeout = int(config.get('listing_render_timeout_ms', 30000))
        self.download_timeout = int(config.get('download_timeout_ms', 120000))
        self.max_retries = int(config.get('max_retries', 3))
        self.backoff = float(config.get('retry_backoff_seconds', 5))
        self.max_bytes = int(float(config.get('max_file_size_mb', 100)) * 1024 * 1024)
        self.max_consecutive_failures = int(config.get('max_consecutive_failures', 10))

        self.state = None
        self.hashes_seen_in_run = {}
        self.ids_seen_in_run = set()
        self.skipped_ids_to_touch = []
        self.consecutive_failures = 0

    # ---- retry, polite delay, circuit breaker ----------------------------------

    async def _with_retry(self, action, what):
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                result = await action()
                await asyncio.sleep(self.delay)
                return result
            except PermanentError:
                raise
            except (RetryableError, PlaywrightTimeoutError, PlaywrightError) as error:
                last_error = error
                if attempt == self.max_retries:
                    break
                wait = self.backoff * attempt
                self.logger.warning('%s failed (attempt %s of %s): %s. Retrying in %ss',
                                    what, attempt, self.max_retries, error, wait)
                await asyncio.sleep(wait)
        raise RetryableError(f'{what} failed after {self.max_retries} attempts: {last_error}')

    def _record_outcome(self, ok):
        '''Stop hammering the site when many documents fail in a row (blocked or down).'''
        self.consecutive_failures = 0 if ok else self.consecutive_failures + 1
        if self.consecutive_failures >= self.max_consecutive_failures:
            raise PermanentError(f'{self.consecutive_failures} consecutive failures, source looks '
                                 f'blocked or unavailable. Stopping this category.', 'SOURCE_UNAVAILABLE')

    # ---- browser primitives -----------------------------------------------------

    async def _open(self, page, url):
        response = await page.goto(url, wait_until='domcontentloaded', timeout=self.nav_timeout)
        status = response.status if response else None
        if status in RETRYABLE_HTTP:
            raise RetryableError(f'HTTP {status} for {url}')
        if status is not None and status >= 400:
            raise PermanentError(f'HTTP {status} for {url}', 'PAGE_NOT_ACCESSIBLE')

    async def _settle(self, page):
        '''Give background requests a moment to finish after the page has rendered.'''
        try:
            await page.wait_for_load_state('networkidle', timeout=10000)
        except PlaywrightTimeoutError:
            pass

    async def _goto(self, page, url):
        await self._with_retry(lambda: self._open(page, url), f'open {url}')

    async def _download(self, context, url):
        async def action():
            response = await context.request.get(url, timeout=self.download_timeout)
            if response.status in RETRYABLE_HTTP:
                raise RetryableError(f'HTTP {response.status}')
            if response.status != 200:
                raise PermanentError(f'HTTP {response.status} for {url}', 'DOWNLOAD_HTTP_ERROR')
            length = int(response.headers.get('content-length') or 0)
            if length > self.max_bytes:
                raise PermanentError(f'Content length {length} is above max_file_size_mb', 'FILE_TOO_LARGE')
            body = await response.body()
            return body, response.headers.get('content-type', '')
        return await self._with_retry(action, f'download {url}')

    async def _page_links(self, page):
        return await page.eval_on_selector_all('a[href]', _LINKS_SCRIPT)

    async def _page_body(self, page):
        return await page.evaluate(_PAGE_BODY_SCRIPT, [
            self.config.get('html_content_selectors') or ['main'],
            self.config.get('html_exclude_selectors') or [],
        ])

    # ---- item and document processing --------------------------------------------

    async def _process_item(self, page, context, category, item, counters):
        item_fields = {
            'category_id': category['category_id'], 'category_name': category['category_name'],
            'item_title': item['title'], 'item_date': item['item_date'],
            'item_date_label': item['item_date_label'], 'item_reference': item['item_reference'],
            'source_url': item['url'],
        }
        before = Counters(**asdict(counters))
        try:
            await self._goto(page, item['url'])
            await self._settle(page)

            documents = []
            if category.get('download_pdfs', True):
                links = filter_pdf_links(await self._page_links(page), category, self.base_url)
                documents += [('PDF', link) for link in links]
            if category.get('capture_html') or not documents:
                documents.append(('HTML', None))

            for content_format, link in documents:
                counters.documents_found += 1
                await self._process_document(page, context, category, item, item_fields,
                                             content_format, link, counters)

            failed_here = counters.documents_failed - before.documents_failed
            self.audit.add('ITEM', SUCCESS if failed_here == 0 else PARTIAL_SUCCESS, **item_fields,
                           documents_found=counters.documents_found - before.documents_found,
                           documents_new=counters.documents_new - before.documents_new,
                           documents_changed=counters.documents_changed - before.documents_changed,
                           documents_skipped=counters.documents_skipped - before.documents_skipped,
                           documents_failed=failed_here)
        except PermanentError as error:
            if error.category == 'SOURCE_UNAVAILABLE':
                self.audit.add('ITEM', FAILED, **item_fields, error_category=error.category,
                               error_details=str(error))
                counters.items_failed += 1
                raise
            self._item_failed(category, item, item_fields, counters, error)
        except Exception as error:
            self._item_failed(category, item, item_fields, counters, error)

    def _item_failed(self, category, item, item_fields, counters, error):
        counters.items_failed += 1
        self.logger.error('[%s] Item failed %s: %s', category['category_id'], item['url'], error)
        self.audit.add('ITEM', FAILED, **item_fields,
                       error_category=getattr(error, 'category', type(error).__name__),
                       error_details=str(error))
        self._record_outcome(False)

    async def _process_document(self, page, context, category, item, item_fields,
                                content_format, link, counters):
        if content_format == 'PDF':
            document_url, link_text = link['url'], link['text']
            file_name = safe_file_name(urlsplit(document_url).path.split('/')[-1], default='document.pdf')
        else:
            document_url, link_text = item['url'], None
            file_name = html_file_name(item['url'])

        document_id = make_document_id(self.regulator_id, document_url)
        file_fields = dict(item_fields, document_id=document_id, document_url=document_url,
                           content_format=content_format, file_name=file_name)
        try:
            if document_id in self.ids_seen_in_run:
                counters.documents_skipped += 1
                self.audit.add('FILE', SKIPPED, **file_fields, skip_reason=DUPLICATE_IN_RUN)
                return
            self.ids_seen_in_run.add(document_id)

            # 1. Content in memory, validated before any write
            if content_format == 'PDF':
                data, content_type = await self._download(context, document_url)
                if not data.startswith(b'%PDF'):
                    raise PermanentError(f'Response is not a PDF (content type {content_type})',
                                         'INVALID_CONTENT')
                content_hash = sha256_bytes(data)
            else:
                body = await self._page_body(page)
                min_chars = int(self.config.get('html_min_text_chars', 200))
                if not body or len(normalize_text(body['text'])) < min_chars:
                    raise PermanentError('Page body not found or too short', 'CONTENT_NOT_FOUND')
                data = build_html_snapshot(item, body['html'])
                content_hash = sha256_bytes(normalize_text(body['text']).encode('utf8'))

            if len(data) > self.max_bytes:
                raise PermanentError(f'File is {len(data)} bytes, above max_file_size_mb', 'FILE_TOO_LARGE')
            file_fields.update(content_hash_sha256=content_hash, file_size_bytes=len(data))

            # 2. Duplicate and change detection
            decision = classify_document(document_id, content_hash, self.state, self.hashes_seen_in_run)
            self.hashes_seen_in_run.setdefault(content_hash, document_id)

            if decision.status == SKIPPED:
                counters.documents_skipped += 1
                if document_id in self.state.by_id:
                    self.skipped_ids_to_touch.append(document_id)
                self.audit.add('FILE', SKIPPED, **file_fields, skip_reason=decision.skip_reason,
                               duplicate_of_document_id=decision.duplicate_of_document_id)
                self.logger.info('    SKIPPED %s (%s)', file_name, decision.skip_reason)
                self._record_outcome(True)
                return

            # 3. NEW or CHANGED: verified write, then catalog
            path = versioned_path(self.raw_volume_path, self.regulator_id, document_id,
                                  decision.new_version, file_name)
            write_file_verified(path, data, sha256_bytes(data))
            ts = now_sgt()
            record = dict(
                document_id=document_id, regulator_id=self.regulator_id,
                category_id=category['category_id'], content_format=content_format,
                item_title=item['title'], item_date=item['item_date'],
                item_date_label=item['item_date_label'], item_reference=item['item_reference'],
                source_url=item['url'], document_url=document_url, link_text=link_text,
                file_name=file_name, current_version=decision.new_version,
                content_hash_sha256=content_hash, file_size_bytes=len(data), raw_file_path=path,
                scrape_status=decision.status, first_seen_run_id=self.run_id, first_seen_sgt=ts,
                last_seen_run_id=self.run_id, last_seen_sgt=ts, last_changed_sgt=ts,
                copy_status='PENDING', copy_retry_count=0,
            )
            try:
                self.catalog.upsert_version(record)
            except Exception:
                if not remove_quietly(path):
                    self.logger.warning('Could not remove file after catalog failure: %s', path)
                raise
            self.state.apply(document_id, content_hash, decision.new_version)

            if decision.status == NEW:
                counters.documents_new += 1
            else:
                counters.documents_changed += 1
            self.audit.add('FILE', decision.status, **file_fields, version=decision.new_version,
                           raw_file_path=path)
            self.logger.info('    %s %s v%s', decision.status, file_name, decision.new_version)
            self._record_outcome(True)

        except PermanentError as error:
            self._document_failed(document_url, file_fields, counters, error)
            if error.category == 'SOURCE_UNAVAILABLE':
                raise
            self._record_outcome(False)
        except Exception as error:
            self._document_failed(document_url, file_fields, counters, error)
            self._record_outcome(False)

    def _document_failed(self, document_url, file_fields, counters, error):
        counters.documents_failed += 1
        self.logger.error('    FAILED %s: %s', document_url, error)
        self.audit.add('FILE', FAILED, **file_fields,
                       error_category=getattr(error, 'category', type(error).__name__),
                       error_details=str(error))

    # ---- run ---------------------------------------------------------------------

    async def run(self, scan_start, scan_end):
        totals = Counters()
        categories = [c for c in self.config['categories'] if c.get('enabled', True)]
        failed_categories = []

        self.state = self.catalog.load_state(self.regulator_id)
        self.logger.info('Catalog loaded: %s known documents for %s', len(self.state.by_id), self.regulator_id)

        browser = None
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=USER_AGENT, accept_downloads=False)
                page = await context.new_page()
                self.logger.info('Opening landing page %s', self.config['landing_url'])
                await self._goto(page, self.config['landing_url'])

                for category in categories:
                    counters = Counters()
                    status, error_category, error_details = SUCCESS, None, None
                    self.consecutive_failures = 0
                    self.logger.info('Category %s: %s', category['category_id'], category['category_name'])
                    try:
                        items, warning = await self.discover(page, category, scan_start, scan_end)
                        if warning:
                            error_category, error_details = 'LISTING_EMPTY', warning
                        counters.items_found = len(items)
                        for item in items:
                            await self._process_item(page, context, category, item, counters)
                    except Exception as error:
                        status = FAILED
                        error_category = getattr(error, 'category', type(error).__name__)
                        error_details = str(error)
                        failed_categories.append(category['category_id'])
                        self.logger.error('Category %s failed: %s', category['category_id'], error)

                    self.audit.add('CATEGORY', status,
                                   category_id=category['category_id'],
                                   category_name=category['category_name'],
                                   source_url=category.get('listing_url'),
                                   error_category=error_category, error_details=error_details,
                                   **counters.audit_fields())
                    self.audit.flush()
                    totals.add(counters)
                    self.logger.info('Category %s done: %s', category['category_id'], asdict(counters))
        finally:
            if browser is not None:
                try:
                    await browser.close()
                except Exception as error:
                    self.logger.warning('Browser close failed: %s', error)
            try:
                self.catalog.mark_seen(self.skipped_ids_to_touch, self.run_id, now_sgt())
            finally:
                self.audit.flush()

        if categories and len(failed_categories) == len(categories):
            status = FAILED
        elif failed_categories or totals.items_failed or totals.documents_failed:
            status = PARTIAL_SUCCESS
        else:
            status = SUCCESS
        return {'status': status, 'failed_categories': failed_categories, 'counters': asdict(totals)}
