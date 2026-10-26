# Databricks notebook source
# MAGIC %md
# MAGIC # Regulatory Web Scraper, Orchestrator (External ADB workspace)
# MAGIC
# MAGIC 1. Reads job parameters and resolves the scan period.
# MAGIC 2. Pre flight: config is valid, both tables match the code, raw volume is reachable, no other run is in progress.
# MAGIC 3. Runs the MAS scraper. Every document is classified against `pdf_catalog` as NEW, CHANGED or SKIPPED, and only NEW or CHANGED files are written.
# MAGIC 4. Writes RUN, CATEGORY, ITEM and FILE rows to `scraping_audit_log`.
# MAGIC 5. Fails the task when the run is FAILED, or PARTIAL_SUCCESS with `fail_on_partial` set to true.
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
                               new_run_id, now_sgt, resolve_scan_period, validate_table_schema)
from scrapers.mas_scraper import MasScraper, check_browser, load_config

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logger = get_logger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job parameters
# MAGIC `scan_mode`
# MAGIC 1. PREVIOUS_MONTH: the full previous calendar month in SGT (default, matches the monthly report).
# MAGIC 2. MONTH_TO_DATE: from the 1st of the current month to today.
# MAGIC 3. LAST_N_DAYS: the last `last_n_days` days up to today.
# MAGIC 4. CUSTOM: uses `scan_start_date` and `scan_end_date` in ISO format.

# COMMAND ----------

dbutils.widgets.text('catalog', 'ai_platform_dev')
dbutils.widgets.text('schema', 'regulatory_scraping')
dbutils.widgets.text('raw_volume_path', '/Volumes/ai_platform_dev/regulatory_scraping/raw')
dbutils.widgets.text('config_path', 'config/mas_config.json')
dbutils.widgets.dropdown('scan_mode', 'PREVIOUS_MONTH', list(SCAN_MODES))
dbutils.widgets.text('last_n_days', '7')
dbutils.widgets.text('scan_start_date', '')
dbutils.widgets.text('scan_end_date', '')
dbutils.widgets.dropdown('fail_on_partial', 'true', ['true', 'false'])
dbutils.widgets.text('stale_run_hours', '6')

CATALOG = dbutils.widgets.get('catalog')
SCHEMA = dbutils.widgets.get('schema')
RAW_VOLUME_PATH = dbutils.widgets.get('raw_volume_path').rstrip('/')
CONFIG_PATH = dbutils.widgets.get('config_path')
if not os.path.isabs(CONFIG_PATH):
    CONFIG_PATH = os.path.join(_repo_root, CONFIG_PATH)
FAIL_ON_PARTIAL = dbutils.widgets.get('fail_on_partial') == 'true'
STALE_RUN_HOURS = float(dbutils.widgets.get('stale_run_hours'))

CATALOG_TABLE = f'{CATALOG}.{SCHEMA}.pdf_catalog'
AUDIT_TABLE = f'{CATALOG}.{SCHEMA}.scraping_audit_log'


SCAN_START, SCAN_END = resolve_scan_period(
    dbutils.widgets.get('scan_mode'), now_sgt().date(), dbutils.widgets.get('last_n_days'),
    dbutils.widgets.get('scan_start_date').strip(), dbutils.widgets.get('scan_end_date').strip())

RUN_ID = new_run_id('scrape')
RUN_STARTED = now_sgt()
logger.info('Run %s | scan period %s to %s | config %s', RUN_ID, SCAN_START, SCAN_END, CONFIG_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre flight checks

# COMMAND ----------

config = load_config(CONFIG_PATH)
REGULATOR_ID = config['regulator_id']
if not config.get('is_active', True):
    dbutils.notebook.exit(f'{REGULATOR_ID} is inactive in config, nothing to do')

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
    scraper = MasScraper(config, RUN_ID, RAW_VOLUME_PATH, catalog_repo, audit, logger)
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
    audit.add('RUN', final_status, start_time_sgt=RUN_STARTED, end_time_sgt=now_sgt(),
              source_url=config['landing_url'],
              items_found=counters.get('items_found'), documents_found=counters.get('documents_found'),
              documents_new=counters.get('documents_new'), documents_changed=counters.get('documents_changed'),
              documents_skipped=counters.get('documents_skipped'), documents_failed=counters.get('documents_failed'),
              error_category=error_category, error_details=error_details)
    audit.flush()

logger.info('Run %s finished with %s | %s', RUN_ID, final_status, counters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coverage summary for this run

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final status

# COMMAND ----------

dbutils.jobs.taskValues.set(key='scrape_run_id', value=RUN_ID)
dbutils.jobs.taskValues.set(key='scrape_status', value=final_status)

if final_status == FAILED:
    raise RuntimeError(f'Run {RUN_ID} FAILED: {error_details}')
if final_status == PARTIAL_SUCCESS and FAIL_ON_PARTIAL:
    raise RuntimeError(f'Run {RUN_ID} PARTIAL_SUCCESS: {error_details or "document or item failures"}. '
                       f'See scraping_audit_log rows with status FAILED for this run_id.')

dbutils.notebook.exit(f'{RUN_ID} {final_status}')