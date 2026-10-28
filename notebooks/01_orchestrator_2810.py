# Databricks notebook source
# MAGIC %md
# MAGIC # Regulatory Web Scraper, Orchestrator (External ADB workspace)
# MAGIC
# MAGIC One notebook for every regulator. The job passes `regulator_id` (for example MAS or PDPC).
# MAGIC
# MAGIC 1. Reads job parameters and `config/common_config.json` (tables, volume path, scan defaults, regulator registry).
# MAGIC 2. Loads the regulator's own config and scraper class from the registry.
# MAGIC 3. Pre flight: configs valid, tables match the code, volume writable, Chromium launches, no other run for this regulator.
# MAGIC 4. Runs the scraper and records RUN, CATEGORY, ITEM and FILE rows in `scraping_audit_log`.
# MAGIC 5. Fails the task when the run is FAILED, or PARTIAL_SUCCESS when `fail_on_partial` is true in the common config.
# MAGIC
# MAGIC Cluster: single node job cluster, DBR 14.3 LTS, init script `scripts/playwright_init.sh`.

# COMMAND ----------

import os
import sys

_notebook_dir = os.getcwd()
_repo_root = os.path.dirname(_notebook_dir) if os.path.basename(_notebook_dir) == 'notebooks' else _notebook_dir
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from scrapers.tracking import (AUDIT_COLUMNS, CATALOG_COLUMNS, FAILED, PARTIAL_SUCCESS, RUNNING,
                               SCAN_MODES, AuditWriter, CatalogRepository, RunGuard, get_logger,
                               last_successful_scan_end, new_run_id, now_sgt, resolve_scan_period,
                               validate_table_schema)
from scrapers.settings import (ConfigError, get_regulator_entry, load_common_config,
                               load_scraper_class, resolve_path)
from scrapers.base_scraper import BaseScraper, check_browser

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job parameters
# MAGIC 1. `regulator_id` (required): MAS, PDPC and so on, as registered in the common config.
# MAGIC 2. `scan_mode`: DEFAULT uses the common config default (INCREMENTAL). Or INCREMENTAL, CUSTOM, LAST_N_DAYS, PREVIOUS_MONTH, MONTH_TO_DATE.
# MAGIC 3. `scan_start_date`, `scan_end_date`: only for CUSTOM, in ISO format (year, month, day).
# MAGIC 4. `last_n_days`: only for LAST_N_DAYS. Empty uses the common config default.
# MAGIC 5. `common_config_path`: which common config to use (one per environment if needed).
# MAGIC
# MAGIC Paths and table names always come from the common config, never from job parameters.

# COMMAND ----------

dbutils.widgets.text('regulator_id', '')
dbutils.widgets.dropdown('scan_mode', 'DEFAULT', ['DEFAULT'] + list(SCAN_MODES))
dbutils.widgets.text('scan_start_date', '')
dbutils.widgets.text('scan_end_date', '')
dbutils.widgets.text('last_n_days', '')
dbutils.widgets.text('common_config_path', 'config/common_config.json')

PARAM_REGULATOR = dbutils.widgets.get('regulator_id').strip()
PARAM_SCAN_MODE = dbutils.widgets.get('scan_mode').strip()
PARAM_START = dbutils.widgets.get('scan_start_date').strip()
PARAM_END = dbutils.widgets.get('scan_end_date').strip()
PARAM_LAST_N_DAYS = dbutils.widgets.get('last_n_days').strip()
COMMON_CONFIG_PATH = resolve_path(dbutils.widgets.get('common_config_path').strip(), _repo_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load configuration
# MAGIC Any problem here stops the notebook before anything is written.

# COMMAND ----------

try:
    common = load_common_config(COMMON_CONFIG_PATH)
    REGULATOR_ID, registry_entry = get_regulator_entry(common, PARAM_REGULATOR)
    ScraperClass = load_scraper_class(registry_entry, BaseScraper)
    REGULATOR_CONFIG_PATH = resolve_path(registry_entry['config_path'], _repo_root)
    config = ScraperClass.load_config(REGULATOR_CONFIG_PATH)
    if config['regulator_id'] != REGULATOR_ID:
        raise ConfigError(f'{REGULATOR_CONFIG_PATH} has regulator_id {config["regulator_id"]}, '
                          f'expected {REGULATOR_ID}')
except (ConfigError, ValueError) as error:
    logger.error('Configuration error: %s', error)
    raise

if not config.get('is_active', True):
    dbutils.notebook.exit(f'{REGULATOR_ID} is inactive in {REGULATOR_CONFIG_PATH}, nothing to do')

DEFAULTS = common['defaults']
CATALOG_TABLE = f'{common["catalog"]}.{common["schema"]}.pdf_catalog'
AUDIT_TABLE = f'{common["catalog"]}.{common["schema"]}.scraping_audit_log'
RAW_VOLUME_PATH = common['raw_volume_path'].rstrip('/')
FAIL_ON_PARTIAL = bool(DEFAULTS['fail_on_partial'])
STALE_RUN_HOURS = float(DEFAULTS['stale_run_hours'])

logger.info('Environment %s | regulator %s | scraper %s | tables %s.%s',
            common.get('environment'), REGULATOR_ID, ScraperClass.__name__, common['catalog'], common['schema'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve the scan period

# COMMAND ----------

try:
    SCAN_MODE = DEFAULTS['scan_mode'] if PARAM_SCAN_MODE in ('', 'DEFAULT') else PARAM_SCAN_MODE
    WATERMARK = None
    if SCAN_MODE == 'INCREMENTAL':
        validate_table_schema(spark, AUDIT_TABLE, AUDIT_COLUMNS)
        WATERMARK = last_successful_scan_end(spark, AUDIT_TABLE, REGULATOR_ID)
    SCAN_START, SCAN_END = resolve_scan_period(
        SCAN_MODE, now_sgt().date(),
        last_n_days=PARAM_LAST_N_DAYS or DEFAULTS['last_n_days'],
        start_text=PARAM_START, end_text=PARAM_END,
        last_successful_end=WATERMARK,
        overlap_days=DEFAULTS['overlap_days'],
        max_window_days=DEFAULTS['max_window_days'])
except ValueError as error:
    logger.error('Scan period error for %s: %s', REGULATOR_ID, error)
    raise

RUN_ID = new_run_id(f'scrape_{REGULATOR_ID.lower()}')
RUN_STARTED = now_sgt()
logger.info('Run %s | mode %s | scan period %s to %s | last successful end %s',
            RUN_ID, SCAN_MODE, SCAN_START, SCAN_END, WATERMARK)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre flight checks

# COMMAND ----------

try:
    validate_table_schema(spark, CATALOG_TABLE, CATALOG_COLUMNS)
    validate_table_schema(spark, AUDIT_TABLE, AUDIT_COLUMNS)

    if not os.path.isdir(RAW_VOLUME_PATH):
        raise RuntimeError(f'Raw volume path {RAW_VOLUME_PATH} is not reachable from this cluster')
    _probe = os.path.join(RAW_VOLUME_PATH, f'.write_probe_{RUN_ID}')
    with open(_probe, 'w') as handle:
        handle.write('ok')
    os.remove(_probe)

    BROWSER_PATH, BROWSER_VERSION = await check_browser()
    logger.info('Chromium %s found in %s', BROWSER_VERSION, BROWSER_PATH)

    RunGuard(spark, AUDIT_TABLE, REGULATOR_ID, 'SCRAPE', STALE_RUN_HOURS).check_can_start(RUN_ID)
except Exception as error:
    logger.error('Pre flight check failed for %s: %s', REGULATOR_ID, error)
    raise

logger.info('Pre flight checks passed')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the scraper

# COMMAND ----------

audit = AuditWriter(spark, AUDIT_TABLE, RUN_ID, REGULATOR_ID, 'SCRAPE', SCAN_START, SCAN_END)
catalog_repo = CatalogRepository(spark, CATALOG_TABLE)

audit.add('RUN', RUNNING, start_time_sgt=RUN_STARTED, source_url=config['landing_url'])
audit.flush()

result = {'status': FAILED, 'failed_categories': [], 'counters': {}}
run_error = None
try:
    scraper = ScraperClass(config, RUN_ID, RAW_VOLUME_PATH, catalog_repo, audit, logger)
    result = await scraper.run(SCAN_START, SCAN_END)
except Exception as error:
    run_error = error
    logger.exception('Run %s failed', RUN_ID)
finally:
    counters = result.get('counters') or {}
    failed_categories = result.get('failed_categories') or []
    final_status = FAILED if run_error else result['status']
    if run_error:
        error_category, error_details = type(run_error).__name__, str(run_error)
    elif failed_categories:
        error_category, error_details = 'CATEGORY_FAILURE', ', '.join(failed_categories)
    else:
        error_category, error_details = None, None
    try:
        audit.add('RUN', final_status, start_time_sgt=RUN_STARTED, end_time_sgt=now_sgt(),
                  source_url=config['landing_url'],
                  items_found=counters.get('items_found'), documents_found=counters.get('documents_found'),
                  documents_new=counters.get('documents_new'), documents_changed=counters.get('documents_changed'),
                  documents_skipped=counters.get('documents_skipped'),
                  documents_failed=counters.get('documents_failed'),
                  error_category=error_category, error_details=error_details)
        audit.flush()
    except Exception as audit_error:
        logger.error('Could not write the final RUN row for %s: %s. The reconcile task will close it '
                     'as ABANDONED.', RUN_ID, audit_error)
        final_status = FAILED
        error_details = f'{error_details or ""} Final RUN row not written: {audit_error}'.strip()

logger.info('Run %s finished with %s | %s', RUN_ID, final_status, counters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coverage summary for this run

# COMMAND ----------

try:
    display(spark.sql(f'''
        SELECT category_id, category_name, status, items_found, documents_found,
               documents_new, documents_changed, documents_skipped, documents_failed, error_details
        FROM {AUDIT_TABLE}
        WHERE run_id = :run_id AND record_type = 'CATEGORY'
        ORDER BY category_id
    ''', args={'run_id': RUN_ID}))

    display(spark.sql(f'''
        SELECT status, skip_reason, error_category, count(*) AS files
        FROM {AUDIT_TABLE}
        WHERE run_id = :run_id AND record_type = 'FILE'
        GROUP BY status, skip_reason, error_category
        ORDER BY status
    ''', args={'run_id': RUN_ID}))
except Exception as error:
    # The summary is informational only; it must not change the run outcome.
    logger.warning('Could not display the run summary: %s', error)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final status

# COMMAND ----------

# Task values are informational only. Databricks does not allow them inside a For each
# iteration (the normal way this notebook runs), so a failure here must not fail the run.
try:
    dbutils.jobs.taskValues.set(key='scrape_run_id', value=RUN_ID)
    dbutils.jobs.taskValues.set(key='scrape_status', value=final_status)
    dbutils.jobs.taskValues.set(key='regulator_id', value=REGULATOR_ID)
except Exception as error:
    logger.info('Task values not set (not supported inside a For each iteration): %s',
                str(error).splitlines()[0][:200])

if final_status == FAILED:
    raise RuntimeError(f'Run {RUN_ID} FAILED: {error_details}')
if final_status == PARTIAL_SUCCESS and FAIL_ON_PARTIAL:
    raise RuntimeError(f'Run {RUN_ID} PARTIAL_SUCCESS: {error_details or "document or item failures"}. '
                       f'See scraping_audit_log rows with status FAILED for this run_id.')

dbutils.notebook.exit(f'{RUN_ID} {final_status}')