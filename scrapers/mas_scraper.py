'''
MAS discovery: which MAS items fall inside the scan period for a category.

Everything after discovery (item pages, downloads, decisions, tables, the run loop) is shared
and lives in scrapers/base_scraper.py.

How MAS items are found (follows the intake tracker)
  Card listings (Regulations and Guidance, News)
    The category Entry URL (listing_url) is opened with the address options the page itself
    uses: rows=All (all results on one page), page=1, and date=start,end (the page's Custom
    date filter, only when filter_by_date_in_url is true). Each result card gives the type
    label, date, title and item link.
  Responses to Feedback
    The Consultations listing without a date filter. Cards whose MAS response date falls in
    the scan period are kept.
  Enforcement Actions
    The enforcement table opened with rows=All, filtered on Issue Date.

Zero items
  Nothing in the period is recorded as 0 items. A listing that shows no results even without
  a date filter is also recorded as 0 items, with a warning to check the Entry URL.

Safety checks that fail a category
  1. The page did not render cards, a table or the No results message in time.
  2. A card has a type label not in expected_card_types (the page ignored the filter).
  3. The page reports more results than could be collected.
'''

import re
from datetime import date, datetime
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from scrapers.base_scraper import BaseScraper, PermanentError, RetryableError
from scrapers.tracking import normalize_url

LISTING_TYPES = {'cards', 'table'}
DATE_SOURCES = {'listing_date', 'response_date', 'table_issue_date'}

LISTING_EMPTY_WARNING = ('Listing showed no results at all, even without a date filter. '
                         'Recorded as 0 items. Check that the Entry URL still works.')

_DATE_RE = re.compile(r'(\d{1,2}) ([A-Za-z]+) (\d{4})')
_RESPONSE_DATE_RE = re.compile(r'MAS response date:?\s*(\d{1,2} [A-Za-z]+ \d{4})', re.IGNORECASE)
_PUBLISHED_RE = re.compile(r'Published Date:?\s*(\d{1,2} [A-Za-z]+ \d{4})', re.IGNORECASE)
_CONSULTATION_NO_RE = re.compile(r'Consultation number:?\s*(\S+)', re.IGNORECASE)


# =====================================================================
# Pure helpers (unit tested without a browser)
# =====================================================================

def parse_display_date(text):
    '''First date written like 04 September 2026 in the text, or None.'''
    match = _DATE_RE.search(text or '')
    if not match:
        return None
    try:
        return datetime.strptime(' '.join(match.groups()), '%d %B %Y').date()
    except ValueError:
        return None


def build_listing_url(category, scan_start=None, scan_end=None, page_number=1, rows='All'):
    '''The category Entry URL plus rows, page and (for card listings that allow it) the scan
    period as the page date filter. Any rows, page or date already in the Entry URL is replaced.'''
    parts = urlsplit(category['listing_url'])
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if k not in ('rows', 'page', 'date')]
    query += [('rows', str(rows)), ('page', str(page_number))]
    use_date = (category.get('listing_type') == 'cards' and category.get('filter_by_date_in_url', True)
                and scan_start is not None and scan_end is not None)
    if use_date:
        query.append(('date', f'{scan_start.isoformat()}T00:00:00.000Z,{scan_end.isoformat()}T23:59:59.000Z'))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), ''))


def parse_card(card, category, base_url, scan_start, scan_end):
    '''Turn one listing card into an item dict, or None when it has no usable link or date,
    or falls outside the scan period. Raises PermanentError when the card type is not one the
    category expects, which means the page did not apply the content type filter.'''
    expected = category.get('expected_card_types') or []
    card_type = (card.get('type') or '').strip()
    if expected and card_type not in expected:
        raise PermanentError(f'Card type {card_type!r} is not in {expected}; the listing filter was not '
                             f'applied', 'LISTING_FILTER_IGNORED')

    url = normalize_url(card.get('href') or '', base_url)
    if not url or urlsplit(url).netloc != urlsplit(base_url).netloc.lower():
        return None
    text = card.get('text') or ''

    if category['date_source'] == 'response_date':
        match = _RESPONSE_DATE_RE.search(text)
        if not match:
            return None                       # consultation without a response yet
        item_date = parse_display_date(match.group(1))
        date_label = 'MAS Response Date'
    else:
        published = _PUBLISHED_RE.search(text)
        if published:
            item_date, date_label = parse_display_date(published.group(1)), 'Published Date'
        else:
            item_date, date_label = parse_display_date(text), 'Listing Date'

    if item_date is None or item_date < scan_start or item_date > scan_end:
        return None

    reference = _CONSULTATION_NO_RE.search(text)
    return {
        'url': url,
        'title': (card.get('title') or '').strip() or url,
        'item_date': item_date,
        'item_date_label': date_label,
        'item_reference': reference.group(1) if reference else None,
    }


def parse_table_row(row, base_url, scan_start, scan_end):
    '''Turn one Enforcement Actions table row into an item dict, or None when outside the scan
    period or without a usable link. Columns: Issue Date, Person/Company, Action Type, Title.'''
    cells = row.get('cells') or []
    item_date = parse_display_date(cells[0] if cells else '')
    url = normalize_url(row.get('href') or '', base_url)
    if item_date is None or not url or urlsplit(url).netloc != urlsplit(base_url).netloc.lower():
        return None
    if item_date < scan_start or item_date > scan_end:
        return None
    title = (row.get('title') or (cells[3] if len(cells) > 3 else '')).strip()
    action_type = cells[2].strip() if len(cells) > 2 else None
    return {
        'url': url,
        'title': title or url,
        'item_date': item_date,
        'item_date_label': 'Issue Date',
        'item_reference': action_type,
    }


# Scripts run inside Chromium

_CARDS_READY_SCRIPT = '''() => document.querySelectorAll('article.mas-search-card').length > 0
    || /No results were found/i.test((document.querySelector('main') || document.body).innerText)'''

_CARDS_SCRIPT = '''() => {
    const scope = document.querySelector('main') || document.body;
    const text = scope.innerText.replace(/\\s+/g, ' ');
    const showing = text.match(/Showing\\s+(\\d+)\\s*\\D\\s*(\\d+)\\s+of\\s+(\\d+)\\s+results/i);
    const cards = [...document.querySelectorAll('article.mas-search-card')].map(c => {
        const a = c.querySelector('.ola-field-title a') || c.querySelector('a[href]');
        const tag = c.querySelector('.mas-tag__text');
        return {type: tag ? tag.textContent.trim() : '',
                title: a ? a.textContent.trim() : '',
                href: a ? a.href : '',
                text: (c.innerText || '').replace(/\\s+/g, ' ').trim()};
    });
    return {cards: cards,
            total: showing ? parseInt(showing[3], 10) : null,
            no_results: /No results were found/i.test(text)};
}'''

_TABLE_READY_SCRIPT = '''() => document.querySelectorAll('table tbody tr').length > 0'''

_TABLE_SCRIPT = '''() => [...document.querySelectorAll('table tbody tr')].map(r => {
    const a = r.querySelector('a[href]');
    return {cells: [...r.cells].map(c => (c.innerText || '').trim()),
            href: a ? a.href : '',
            title: a ? a.textContent.trim() : ''};
})'''


# =====================================================================
# MAS scraper
# =====================================================================

class MasScraper(BaseScraper):

    @classmethod
    def validate_category(cls, category, base_host):
        problems = []
        for key in ('listing_type', 'listing_url', 'date_source'):
            if not category.get(key):
                problems.append(f'missing {key}')
        listing_type = category.get('listing_type')
        if listing_type and listing_type not in LISTING_TYPES:
            problems.append(f'listing_type must be one of {sorted(LISTING_TYPES)}')
        if category.get('date_source') and category['date_source'] not in DATE_SOURCES:
            problems.append(f'date_source must be one of {sorted(DATE_SOURCES)}')
        if listing_type == 'cards' and not category.get('expected_card_types'):
            problems.append('expected_card_types is required for card listings')
        if listing_type == 'table' and category.get('date_source') != 'table_issue_date':
            problems.append('table listings must use date_source table_issue_date')
        if category.get('date_source') == 'response_date' and category.get('filter_by_date_in_url', True):
            problems.append('response_date needs filter_by_date_in_url false '
                            '(the page date filter uses the published date)')
        if category.get('listing_url') and urlsplit(category['listing_url']).netloc.lower() != base_host:
            problems.append(f'listing_url must be on {base_host}')
        return problems

    # Reading listing pages

    async def _read_cards_page(self, page, url):
        '''Open one card listing page and return {cards, total, no_results}.'''
        async def action():
            await self._open(page, url)
            try:
                await page.wait_for_function(_CARDS_READY_SCRIPT, timeout=self.render_timeout)
            except PlaywrightTimeoutError:
                raise RetryableError(f'Listing did not show cards or No results in time: {url}')
            await self._settle(page)
            return await page.evaluate(_CARDS_SCRIPT)
        return await self._with_retry(action, f'read listing {url}')

    async def _read_table_page(self, page, url):
        async def action():
            await self._open(page, url)
            try:
                await page.wait_for_function(_TABLE_READY_SCRIPT, timeout=self.render_timeout)
            except PlaywrightTimeoutError:
                raise RetryableError(f'Table did not show any rows in time: {url}')
            await self._settle(page)
            return await page.evaluate(_TABLE_SCRIPT)
        return await self._with_retry(action, f'read table {url}')

    async def _collect_cards(self, page, category, scan_start, scan_end):
        '''All cards of a listing, following pages if the page still paginates.
        Returns (cards, listing_empty).'''
        max_pages = int(self.config.get('max_pages_per_category', 20))
        cards, seen_hrefs = [], set()
        for page_number in range(1, max_pages + 1):
            url = build_listing_url(category, scan_start, scan_end, page_number)
            result = await self._read_cards_page(page, url)
            new_cards = [c for c in result['cards'] if c.get('href') and c['href'] not in seen_hrefs]
            for card in new_cards:
                seen_hrefs.add(card['href'])
            cards += new_cards
            total = result.get('total')
            if result.get('no_results') and not cards:
                return [], True
            if not new_cards or total is None or len(cards) >= total:
                if total is not None and len(cards) < total:
                    raise PermanentError(f'Listing reports {total} results but only {len(cards)} could be '
                                         f'read: {url}', 'LISTING_INCOMPLETE')
                return cards, False
        raise PermanentError(f'More than {max_pages} listing pages, raise max_pages_per_category',
                             'PAGINATION_LIMIT')

    async def _listing_has_results(self, page, category):
        '''Same Entry URL without the date filter, 10 rows: does the listing work at all ?'''
        url = build_listing_url(category, rows=10)
        result = await self._read_cards_page(page, url)
        for card in result['cards']:
            parse_card(card, category, self.base_url, date.min, date.max)   # raises if wrong type
        return bool(result['cards'])

    async def discover(self, page, category, scan_start, scan_end):
        warning = None
        if category['listing_type'] == 'table':
            rows = await self._read_table_page(page, build_listing_url(category))
            items = [parse_table_row(r, self.base_url, scan_start, scan_end) for r in rows]
        else:
            cards, empty = await self._collect_cards(page, category, scan_start, scan_end)
            if empty and category.get('filter_by_date_in_url', True):
                if not await self._listing_has_results(page, category):
                    warning = LISTING_EMPTY_WARNING
            elif empty:
                warning = LISTING_EMPTY_WARNING
            items = [parse_card(c, category, self.base_url, scan_start, scan_end) for c in cards]

        unique, seen = [], set()
        for item in items:
            if item and item['url'] not in seen:
                seen.add(item['url'])
                unique.append(item)
        self.logger.info('[%s] %s items in scan period%s', category['category_id'], len(unique),
                         ' (listing empty, recorded as 0)' if warning else '')
        return unique, warning