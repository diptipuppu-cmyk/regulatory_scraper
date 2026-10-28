# Databricks notebook source
# MAGIC %md
# MAGIC # Scan Verdict Job (External ADB workspace)
# MAGIC
# MAGIC 1. Picks `pdf_catalog` rows with no malware scan verdict yet.
# MAGIC 2. Reads the Defender for Storage blob index tag of each raw file.
# MAGIC 3. Writes the verdict and the time to `pdf_catalog` (`malware_scan_verdict`, `scan_timestamp_sgt`).
# MAGIC 4. Files with no tag yet are left empty and checked again on the next run.
# MAGIC 5. Writes RUN and FILE rows (stage SCAN) to `scraping_audit_log`.
# MAGIC
# MAGIC Parameters: `regulator_id` (default ALL) and `max_files` (default 1000).
# MAGIC Run it before the internal copy job, for example daily at 09:30 SGT.
# MAGIC
# MAGIC Access: a Unity Catalog service credential (managed identity of the Access Connector). No keys or secrets.
# MAGIC Needs Databricks Runtime 15.4 LTS or above, and the libraries azure-storage-blob and azure-core,
# MAGIC which are included in the Databricks Runtime (no pip install).

# COMMAND ----------

import os
import sys

_notebook_dir = os.getcwd()
_repo_root = os.path.dirname(_notebook_dir) if os.path.basename(_notebook_dir) == 'notebooks' else _notebook_dir
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from scrapers.tracking import (FAILED, PARTIAL_SUCCESS, RUNNING, SUCCESS, AuditWriter, get_logger,
                               new_run_id, now_sgt, run_with_delta_retry)

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
logger = get_logger()

# COMMAND ----------

# Settings. Change them here if your storage or catalog is different.
CATALOG_TABLE = 'ai_platform_dev.regulatory_scraping.pdf_catalog'
AUDIT_TABLE = 'ai_platform_dev.regulatory_scraping.scraping_audit_log'
RAW_VOLUME_ROOT = '/Volumes/ai_platform_dev/regulatory_scraping/raw'
STORAGE_ACCOUNT = 'webscraperlanding'                  # storage account behind the raw volume
CONTAINER = 'raw'                                      # container behind the raw volume
VOLUME_PREFIX = 'documents'                            # folder in the container that the raw volume points to
SERVICE_CREDENTIAL = 'rcm_storage_service_cred'        # Unity Catalog service credential with tag read access
TAG_KEY = 'Malware Scanning scan result'

dbutils.widgets.text('regulator_id', 'ALL')            # ALL, or one regulator such as MAS
dbutils.widgets.text('max_files', '1000')
REGULATOR_ID = dbutils.widgets.get('regulator_id').strip().upper() or 'ALL'
MAX_FILES = int(dbutils.widgets.get('max_files'))
REG_FILTER = '' if REGULATOR_ID == 'ALL' else 'AND regulator_id = :regulator_id'
REG_ARGS = {} if REGULATOR_ID == 'ALL' else {'regulator_id': REGULATOR_ID}

RUN_ID = new_run_id('scan')
RUN_STARTED = now_sgt()

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

credential = dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL)
container = (BlobServiceClient(f'https://{STORAGE_ACCOUNT}.blob.core.windows.net', credential=credential)
             .get_container_client(CONTAINER))


def read_verdict(raw_file_path):
    '''Returns the scan result tag, or None if Defender has not tagged the file yet.'''
    if not raw_file_path.startswith(RAW_VOLUME_ROOT + '/'):
        raise ValueError(f'{raw_file_path} is outside {RAW_VOLUME_ROOT}')
    blob_name = VOLUME_PREFIX + raw_file_path[len(RAW_VOLUME_ROOT):]
    return container.get_blob_client(blob_name).get_blob_tags().get(TAG_KEY)


SET_VERDICT = f'''
    UPDATE {CATALOG_TABLE}
    SET malware_scan_verdict = :verdict, scan_timestamp_sgt = :scanned_at
    WHERE document_id = :document_id AND current_version = :version
'''

# COMMAND ----------

todo = spark.sql(f'''
    SELECT document_id, regulator_id, category_id, file_name, current_version, raw_file_path
    FROM {CATALOG_TABLE}
    WHERE malware_scan_verdict IS NULL {REG_FILTER}
    ORDER BY last_changed_sgt
    LIMIT {MAX_FILES}
''', args=REG_ARGS).collect()
logger.info('Scan run %s: %s files without a verdict', RUN_ID, len(todo))

audit = AuditWriter(spark, AUDIT_TABLE, RUN_ID, REGULATOR_ID, 'SCAN')
audit.add('RUN', RUNNING, start_time_sgt=RUN_STARTED, documents_found=len(todo))
audit.flush()

scanned = waiting = failed = not_clean = 0
for row in todo:
    fields = dict(regulator_id=row['regulator_id'], category_id=row['category_id'],
                  document_id=row['document_id'], file_name=row['file_name'],
                  version=row['current_version'], raw_file_path=row['raw_file_path'])
    try:
        verdict = read_verdict(row['raw_file_path'])
        if verdict is None:
            waiting += 1
            audit.add('FILE', 'WAITING', **fields, error_details='No scan result tag yet')
            continue
        run_with_delta_retry(lambda: spark.sql(SET_VERDICT, args={
            'verdict': verdict, 'scanned_at': now_sgt(),
            'document_id': row['document_id'], 'version': row['current_version']}), what='set verdict')
        scanned += 1
        if verdict != 'No threats found':
            not_clean += 1
            logger.warning('NOT CLEAN %s: %s', row['file_name'], verdict)
        audit.add('FILE', 'SCANNED', **fields, malware_scan_verdict=verdict)
    except Exception as error:
        failed += 1
        logger.error('FAILED %s: %s', row['file_name'], error)
        audit.add('FILE', FAILED, **fields, error_category=type(error).__name__, error_details=str(error))

run_status = FAILED if failed and not scanned else PARTIAL_SUCCESS if failed else SUCCESS
audit.add('RUN', run_status, start_time_sgt=RUN_STARTED, end_time_sgt=now_sgt(),
          documents_found=len(todo), documents_failed=failed,
          error_details=f'{scanned} scanned, {not_clean} not clean, {waiting} waiting, {failed} failed')
audit.flush()
logger.info('Scan run %s %s: %s scanned, %s not clean, %s waiting, %s failed',
            RUN_ID, run_status, scanned, not_clean, waiting, failed)

# COMMAND ----------

if not_clean:
    raise RuntimeError(f'{not_clean} files are not clean. Check pdf_catalog.malware_scan_verdict (security alert).')
if run_status == FAILED:
    raise RuntimeError(f'Scan run {RUN_ID} FAILED. See scraping_audit_log stage SCAN.')
dbutils.notebook.exit(f'{RUN_ID} {run_status} scanned={scanned} waiting={waiting} failed={failed}')
