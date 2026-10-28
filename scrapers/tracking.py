'''
Regulator agnostic tracking helpers shared by every scraper, the copy job and the
reconcile job.

Contents
  1. Logging, time, hashing, URL and path helpers
  2. Duplicate classification (pure function, unit tested)
  3. CatalogState       in memory view of pdf_catalog for one regulator
  4. Table contracts    expected columns and a startup schema check
  5. CatalogRepository  reads and writes pdf_catalog
  6. AuditWriter        buffered, append only writes to scraping_audit_log
  7. RunGuard           stops overlapping runs and closes abandoned ones
  8. Delta retry helper for concurrent write conflicts

Spark imports happen inside the functions that need them so the pure helpers can be
imported and tested without a Spark session.
'''

import hashlib
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

SGT = ZoneInfo('Asia/Singapore')
LOGGER_NAME = 'regulatory_scraper'

# Run status values used in scraping_audit_log
RUNNING = 'RUNNING'
SUCCESS = 'SUCCESS'
PARTIAL_SUCCESS = 'PARTIAL_SUCCESS'
FAILED = 'FAILED'
ABANDONED = 'ABANDONED'
TERMINAL_RUN_STATUSES = (SUCCESS, PARTIAL_SUCCESS, FAILED, ABANDONED)

# Document decisions
NEW = 'NEW'
CHANGED = 'CHANGED'
SKIPPED = 'SKIPPED'

SAME_URL_SAME_HASH = 'SAME_URL_SAME_HASH'
SAME_HASH_OTHER_URL = 'SAME_HASH_OTHER_URL'
DUPLICATE_IN_RUN = 'DUPLICATE_IN_RUN'


# =====================================================================
# 1. Logging, time, hashing, URL and path helpers
# =====================================================================

class _SgtFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, SGT).strftime('%Y%m%d %H:%M:%S SGT')


def get_logger():
    '''Logger that prints to the notebook and job output with SGT timestamps.'''
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_SgtFormatter('%(asctime)s | %(levelname)s | %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def now_sgt():
    '''Naive datetime holding Singapore wall clock time. Every notebook sets
    spark.sql.session.timeZone to Asia/Singapore, so Spark stores it unchanged.'''
    return datetime.now(SGT).replace(tzinfo=None)


def new_run_id(prefix):
    '''Readable and unique: prefix, SGT timestamp, short random suffix.'''
    return f'{prefix}_{now_sgt().strftime("%Y%m%d_%H%M%S")}_{uuid.uuid4().hex[:6]}'


SCAN_MODES = ('INCREMENTAL', 'CUSTOM', 'LAST_N_DAYS', 'PREVIOUS_MONTH', 'MONTH_TO_DATE')


def resolve_scan_period(mode, today, last_n_days='7', start_text='', end_text='',
                        last_successful_end=None, overlap_days=3, max_window_days=31):
    '''Scan period as (start date, end date), both inclusive, in SGT calendar days.

    INCREMENTAL      for the daily schedule. Starts overlap_days before the end date of the
                     last successful scrape and ends today, so missed days are covered
                     automatically. The overlap is free because the duplicate check marks
                     already stored documents SKIPPED.
    CUSTOM           for the initial bulk load and for reruns of a specific period.
    LAST_N_DAYS      fixed window ending today.
    PREVIOUS_MONTH   full previous calendar month.
    MONTH_TO_DATE    first of this month to today.
    '''
    from datetime import date as _date
    if mode == 'INCREMENTAL':
        if last_successful_end is None:
            raise ValueError('INCREMENTAL needs a previous successful scrape. Run the bulk load first '
                             'with scan_mode CUSTOM and scan_end_date set to today or yesterday.')
        overlap = int(overlap_days)
        if overlap < 0:
            raise ValueError('overlap_days must be 0 or more')
        start = min(last_successful_end - timedelta(days=overlap), today)
        end = today
        window = (end - start).days + 1
        if window > int(max_window_days):
            raise ValueError(f'INCREMENTAL window would be {window} days (from {start}), above '
                             f'max_window_days {max_window_days}. The scraper has not succeeded for a '
                             f'while: run a CUSTOM backfill from {start} to {end} first.')
    elif mode == 'PREVIOUS_MONTH':
        end = today.replace(day=1) - timedelta(days=1)
        start = end.replace(day=1)
    elif mode == 'MONTH_TO_DATE':
        start, end = today.replace(day=1), today
    elif mode == 'LAST_N_DAYS':
        days = int(last_n_days)
        if days < 1:
            raise ValueError('last_n_days must be at least 1')
        start, end = today - timedelta(days=days - 1), today
    elif mode == 'CUSTOM':
        if not start_text or not end_text:
            raise ValueError('CUSTOM scan_mode needs scan_start_date and scan_end_date')
        try:
            start, end = _date.fromisoformat(start_text), _date.fromisoformat(end_text)
        except ValueError:
            raise ValueError(f'Dates must be ISO format year, month, day. Got {start_text} and {end_text}')
    else:
        raise ValueError(f'Unknown scan_mode {mode}, expected one of {SCAN_MODES}')
    if start > end:
        raise ValueError(f'Scan start {start} is after scan end {end}')
    if end > today:
        raise ValueError(f'Scan end {end} is in the future')
    return start, end


def last_successful_scan_end(spark, audit_table, regulator_id):
    '''Watermark for INCREMENTAL: the latest scan_end_date of a SCRAPE run that covered
    every category. SUCCESS counts, and so does PARTIAL_SUCCESS when only individual
    documents failed (those stay visible in v_recent_failures). A run where a whole
    category failed does not move the watermark, so the next run scans that period again.'''
    row = spark.sql(f'''
        SELECT max(scan_end_date) AS last_end
        FROM {audit_table}
        WHERE stage = 'SCRAPE' AND record_type = 'RUN' AND regulator_id = :regulator_id
          AND (status = 'SUCCESS'
               OR (status = 'PARTIAL_SUCCESS' AND coalesce(error_category, '') <> 'CATEGORY_FAILURE'))
    ''', args={'regulator_id': regulator_id}).first()
    return row['last_end'] if row else None


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path, chunk_size=1024 * 1024):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(chunk_size), b''):
            digest.update(block)
    return digest.hexdigest()


def normalize_url(url, base_url=None):
    '''Absolute URL with lower case scheme and host, no query string, no fragment.
    MAS links the same PDF with and without ?sc_lang=en, so the query is dropped.'''
    absolute = urljoin(base_url, url) if base_url else url
    parts = urlsplit((absolute or '').strip())
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, '', ''))


def make_document_id(regulator_id, normalized_url):
    return sha256_bytes(f'{regulator_id}|{normalized_url}'.encode('utf8'))


_UNSAFE_CHARS = re.compile(r'[^A-Za-z0-9._\-]+')


def safe_file_name(name, default='document'):
    cleaned = _UNSAFE_CHARS.sub('_', name or '').strip('._')
    return cleaned[:180] or default


def versioned_path(volume_root, regulator_id, document_id, version, file_name):
    '''Every version gets its own folder, so nothing is ever overwritten.'''
    return f'{volume_root}/{regulator_id}/{document_id[:16]}/v{version}/{file_name}'


def normalize_text(text):
    '''Whitespace collapsed text. HTML snapshots are hashed on this, so layout only
    changes on a page do not create a new version.'''
    return re.sub(r'\s+', ' ', text or '').strip()


def write_file_verified(path, data, expected_hash):
    '''Write bytes, read them back and confirm the hash. Removes the file on mismatch.'''
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as handle:
        handle.write(data)
    written_hash = sha256_file(path)
    if written_hash != expected_hash:
        remove_quietly(path)
        raise IOError(f'Hash mismatch after writing {path}')
    return written_hash


def remove_quietly(path):
    try:
        os.remove(path)
        return True
    except OSError:
        return False


# =====================================================================
# 2. Duplicate classification
# =====================================================================

@dataclass
class Classification:
    status: str
    skip_reason: str = None
    duplicate_of_document_id: str = None
    new_version: int = None


def classify_document(document_id, content_hash, state, hashes_seen_in_run):
    '''Decide NEW, CHANGED or SKIPPED for one downloaded document.

    Order of checks
      1. Same content already handled earlier in this run    SKIPPED DUPLICATE_IN_RUN
      2. Same URL already catalogued with the same hash       SKIPPED SAME_URL_SAME_HASH
      3. Same content catalogued under a different URL        SKIPPED SAME_HASH_OTHER_URL
      4. Same URL already catalogued with a different hash    CHANGED, version plus 1
      5. Otherwise                                            NEW, version 1
    '''
    seen_document = hashes_seen_in_run.get(content_hash)
    if seen_document is not None:
        return Classification(SKIPPED, DUPLICATE_IN_RUN,
                              seen_document if seen_document != document_id else None)

    existing = state.by_id.get(document_id)
    if existing is not None and existing['content_hash_sha256'] == content_hash:
        return Classification(SKIPPED, SAME_URL_SAME_HASH)

    other_document = state.by_hash.get(content_hash)
    if other_document is not None and other_document != document_id:
        return Classification(SKIPPED, SAME_HASH_OTHER_URL, other_document)

    if existing is not None:
        return Classification(CHANGED, new_version=existing['current_version'] + 1)

    return Classification(NEW, new_version=1)


# =====================================================================
# 3. CatalogState
# =====================================================================

@dataclass
class CatalogState:
    by_id: dict = field(default_factory=dict)    # document_id to {content_hash_sha256, current_version}
    by_hash: dict = field(default_factory=dict)  # content_hash_sha256 to document_id

    def apply(self, document_id, content_hash, version):
        previous = self.by_id.get(document_id)
        if previous is not None and self.by_hash.get(previous['content_hash_sha256']) == document_id:
            del self.by_hash[previous['content_hash_sha256']]
        self.by_id[document_id] = {'content_hash_sha256': content_hash, 'current_version': version}
        self.by_hash.setdefault(content_hash, document_id)


# =====================================================================
# 4. Table contracts
# =====================================================================

CATALOG_COLUMNS = [
    ('document_id', 'string'), ('regulator_id', 'string'), ('category_id', 'string'),
    ('content_format', 'string'), ('item_title', 'string'), ('item_date', 'date'),
    ('item_date_label', 'string'), ('item_reference', 'string'), ('source_url', 'string'),
    ('document_url', 'string'), ('link_text', 'string'), ('file_name', 'string'),
    ('current_version', 'int'), ('content_hash_sha256', 'string'), ('file_size_bytes', 'bigint'),
    ('raw_file_path', 'string'), ('scrape_status', 'string'),
    ('first_seen_run_id', 'string'), ('first_seen_sgt', 'timestamp'),
    ('last_seen_run_id', 'string'), ('last_seen_sgt', 'timestamp'), ('last_changed_sgt', 'timestamp'),
    ('malware_scan_verdict', 'string'), ('scan_timestamp_sgt', 'timestamp'),
]

# Columns a CHANGED version overwrites. first_seen_* is preserved. The verdict is reset, so the new version is checked again.
CATALOG_UPDATE_ON_CHANGE = [
    'category_id', 'content_format', 'item_title', 'item_date', 'item_date_label', 'item_reference',
    'source_url', 'link_text', 'file_name', 'current_version', 'content_hash_sha256',
    'file_size_bytes', 'raw_file_path', 'scrape_status', 'last_seen_run_id', 'last_seen_sgt',
    'last_changed_sgt', 'malware_scan_verdict', 'scan_timestamp_sgt',
]

AUDIT_COLUMNS = [
    ('audit_id', 'string'), ('run_id', 'string'), ('record_type', 'string'), ('stage', 'string'),
    ('regulator_id', 'string'), ('event_time_sgt', 'timestamp'), ('status', 'string'),
    ('scan_start_date', 'date'), ('scan_end_date', 'date'),
    ('category_id', 'string'), ('category_name', 'string'),
    ('item_title', 'string'), ('item_date', 'date'), ('item_date_label', 'string'),
    ('item_reference', 'string'), ('source_url', 'string'),
    ('document_id', 'string'), ('document_url', 'string'), ('content_format', 'string'),
    ('file_name', 'string'), ('version', 'int'), ('content_hash_sha256', 'string'),
    ('file_size_bytes', 'bigint'), ('raw_file_path', 'string'), ('internal_file_path', 'string'),
    ('skip_reason', 'string'), ('duplicate_of_document_id', 'string'), ('malware_scan_verdict', 'string'),
    ('start_time_sgt', 'timestamp'), ('end_time_sgt', 'timestamp'),
    ('items_found', 'int'), ('documents_found', 'int'), ('documents_new', 'int'),
    ('documents_changed', 'int'), ('documents_skipped', 'int'), ('documents_copied', 'int'),
    ('documents_failed', 'int'), ('retry_count', 'int'), ('error_category', 'string'),
    ('error_details', 'string'),
]
_AUDIT_NAMES = [name for name, _ in AUDIT_COLUMNS]
_AUDIT_NAME_SET = set(_AUDIT_NAMES)
MAX_ERROR_CHARS = 4000

_SQL_TYPES = {'string': 'STRING', 'int': 'INT', 'bigint': 'BIGINT', 'date': 'DATE', 'timestamp': 'TIMESTAMP'}


def spark_schema(columns):
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                                   LongType, DateType, TimestampType)
    type_map = {'string': StringType(), 'int': IntegerType(), 'bigint': LongType(),
                'date': DateType(), 'timestamp': TimestampType()}
    return StructType([StructField(name, type_map[kind], True) for name, kind in columns])


def schema_problems(actual_types, expected_columns):
    '''Compare {column: simple type} from the table with the expected contract.
    Returns a list of readable problems (empty when the table matches).'''
    problems = []
    for name, kind in expected_columns:
        actual = actual_types.get(name)
        if actual is None:
            problems.append(f'missing column {name} {_SQL_TYPES[kind]}')
        elif actual != kind:
            problems.append(f'column {name} is {actual}, expected {kind}')
    return problems


def validate_table_schema(spark, table_name, expected_columns):
    '''Fail fast with an actionable message when the table does not match the code.'''
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f'Table {table_name} does not exist. Run ddl/04_create_pdf_catalog_and_audit.sql.')
    actual = {f.name: f.dataType.simpleString() for f in spark.table(table_name).schema.fields}
    problems = schema_problems(actual, expected_columns)
    if problems:
        missing = [p for p in problems if p.startswith('missing column ')]
        fix = ''
        if missing and len(missing) == len(problems):
            columns = ', '.join(p.replace('missing column ', '') for p in missing)
            fix = f' Fix: ALTER TABLE {table_name} ADD COLUMNS ({columns});'
        raise RuntimeError(f'Table {table_name} does not match the code: {"; ".join(problems)}.{fix}')


# =====================================================================
# 5. CatalogRepository
# =====================================================================

class CatalogRepository:

    def __init__(self, spark, table_name):
        self.spark = spark
        self.table_name = table_name

    def load_state(self, regulator_id):
        from pyspark.sql import functions as F
        rows = (self.spark.table(self.table_name)
                .where(F.col('regulator_id') == F.lit(regulator_id))
                .select('document_id', 'content_hash_sha256', 'current_version')
                .collect())
        state = CatalogState()
        for row in rows:
            state.apply(row['document_id'], row['content_hash_sha256'], row['current_version'])
        return state

    def upsert_version(self, record):
        '''Insert a NEW document or overwrite the current version of a CHANGED one.
        The merge also guards against a stale in memory state: an update only happens
        when the stored version is older than the incoming one.'''
        from delta.tables import DeltaTable

        full_record = {name: record.get(name) for name, _ in CATALOG_COLUMNS}
        source = self.spark.createDataFrame([full_record], schema=spark_schema(CATALOG_COLUMNS))

        def _merge():
            (DeltaTable.forName(self.spark, self.table_name).alias('t')
             .merge(source.alias('s'), 't.document_id = s.document_id')
             .whenMatchedUpdate(condition='t.current_version < s.current_version',
                                set={c: f's.{c}' for c in CATALOG_UPDATE_ON_CHANGE})
             .whenNotMatchedInsertAll()
             .execute())

        run_with_delta_retry(_merge, what=f'catalog merge {record.get("document_id")}')

    def mark_seen(self, document_ids, run_id, seen_at):
        '''One UPDATE per 500 ids at the end of a run, for every SKIPPED document.'''
        ids = sorted({value for value in document_ids if re.fullmatch(r'[0-9a-f]{64}', value or '')})
        for start in range(0, len(ids), 500):
            id_list = ', '.join(f"'{value}'" for value in ids[start:start + 500])
            sql = (f'UPDATE {self.table_name} '
                   f'SET last_seen_run_id = :run_id, last_seen_sgt = :seen_at '
                   f'WHERE document_id IN ({id_list})')
            run_with_delta_retry(lambda: self.spark.sql(sql, args={'run_id': run_id, 'seen_at': seen_at}),
                                 what='catalog mark_seen')


# =====================================================================
# 6. AuditWriter
# =====================================================================

def build_audit_row(base, record_type, status, fields):
    '''Pure function that shapes one audit row. Raises on unknown columns so a typo
    in a caller fails loudly instead of silently dropping data.'''
    unknown = set(fields) - _AUDIT_NAME_SET
    if unknown:
        raise ValueError(f'Unknown audit columns: {sorted(unknown)}')
    row = dict.fromkeys(_AUDIT_NAMES)
    row.update(base)
    row.update(fields)
    row['audit_id'] = str(uuid.uuid4())
    row['record_type'] = record_type
    row['status'] = status
    row['event_time_sgt'] = row.get('event_time_sgt') or now_sgt()
    if row.get('error_details') is not None:
        row['error_details'] = str(row['error_details'])[:MAX_ERROR_CHARS]
    return row


class AuditWriter:
    '''Collects audit rows in memory and appends them in one write per flush.
    Callers flush after each category and in finally blocks, so a crash loses at most
    the rows of the unit of work in progress.'''

    def __init__(self, spark, table_name, run_id, regulator_id, stage,
                 scan_start_date=None, scan_end_date=None):
        self.spark = spark
        self.table_name = table_name
        self.base = {'run_id': run_id, 'regulator_id': regulator_id, 'stage': stage,
                     'scan_start_date': scan_start_date, 'scan_end_date': scan_end_date}
        self.rows = []

    def add(self, record_type, status, **fields):
        self.rows.append(build_audit_row(self.base, record_type, status, fields))

    def flush(self):
        if not self.rows:
            return
        pending, self.rows = self.rows, []
        frame = self.spark.createDataFrame([tuple(r[name] for name in _AUDIT_NAMES) for r in pending],
                                           schema=spark_schema(AUDIT_COLUMNS))
        try:
            run_with_delta_retry(
                lambda: frame.write.format('delta').mode('append').saveAsTable(self.table_name),
                what='audit append')
        except Exception:
            self.rows = pending + self.rows
            raise


# =====================================================================
# 7. RunGuard
# =====================================================================

class RunGuard:
    '''Prevents two runs of the same stage and regulator from overlapping, and closes
    runs that died without writing their final RUN row.

    The job setting max_concurrent_runs = 1 is the first line of defence. This guard
    covers manual runs, a second job pointing at the same tables, and crashed runs.'''

    def __init__(self, spark, audit_table, regulator_id, stage, stale_after_hours):
        self.spark = spark
        self.audit_table = audit_table
        self.regulator_id = regulator_id
        self.stage = stage
        self.stale_after = timedelta(hours=float(stale_after_hours))

    def open_runs(self):
        '''Runs with a RUNNING row and no terminal row.'''
        rows = self.spark.sql(f'''
            SELECT run_id, min(event_time_sgt) AS started_sgt
            FROM {self.audit_table}
            WHERE record_type = 'RUN' AND stage = :stage AND regulator_id = :regulator_id
            GROUP BY run_id
            HAVING max(CASE WHEN status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'FAILED', 'ABANDONED')
                            THEN 1 ELSE 0 END) = 0
        ''', args={'stage': self.stage, 'regulator_id': self.regulator_id}).collect()
        return [(row['run_id'], row['started_sgt']) for row in rows]

    def close_stale(self, closed_by):
        '''Marks open runs older than stale_after as ABANDONED. The ABANDONED row carries
        the abandoned run_id so the run reads as closed from then on.
        Returns (closed run ids, still open recent runs).'''
        logger = get_logger()
        now = now_sgt()
        closed, recent = [], []
        for run_id, started in self.open_runs():
            if run_id == closed_by:
                continue
            if started is not None and now - started < self.stale_after:
                recent.append((run_id, started))
                continue
            logger.warning('Closing abandoned %s run %s started %s', self.stage, run_id, started)
            writer = AuditWriter(self.spark, self.audit_table, run_id, self.regulator_id, self.stage)
            writer.add('RUN', ABANDONED, start_time_sgt=started, end_time_sgt=now,
                       error_category='ABANDONED_RUN',
                       error_details=f'No final status after {self.stale_after}. Closed by {closed_by}.')
            writer.flush()
            closed.append(run_id)
        return closed, recent

    def check_can_start(self, current_run_id):
        '''Call before writing the RUNNING row of a new run.'''
        _, recent = self.close_stale(closed_by=current_run_id)
        if recent:
            listed = ', '.join(f'{run_id} (started {started})' for run_id, started in recent)
            raise RuntimeError(f'Another {self.stage} run for {self.regulator_id} is still in progress: '
                               f'{listed}. If it is dead it will be closed as ABANDONED after '
                               f'{self.stale_after}.')


# =====================================================================
# 8. Delta retry helper
# =====================================================================

def run_with_delta_retry(action, what='delta write', attempts=4, base_delay_seconds=3):
    '''Retries only Delta concurrent modification conflicts, which can happen if the
    scrape, copy and reconcile jobs touch the same table at the same moment.'''
    logger = get_logger()
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as error:
            text = f'{type(error).__name__} {error}'
            is_conflict = 'Concurrent' in text or 'DELTA_CONCURRENT' in text
            if not is_conflict or attempt == attempts:
                raise
            delay = base_delay_seconds * attempt
            logger.warning('Delta conflict on %s, attempt %s of %s, retrying in %ss',
                           what, attempt, attempts, delay)
            time.sleep(delay)
