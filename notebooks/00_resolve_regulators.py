# Databricks notebook source
# MAGIC %md
# MAGIC # Resolve regulators (first task of the scrape job)
# MAGIC
# MAGIC Turns the job parameter `regulators` into the list of regulators to scrape in this run,
# MAGIC and passes it to the next tasks as the task value `regulator_ids`.
# MAGIC
# MAGIC 1. `ALL` (default): every regulator with `enabled: true` in `config/common_config.json`.
# MAGIC 2. `MAS` or `PDPC`: only that regulator.
# MAGIC 3. `MAS,PDPC`: several regulators, comma separated.
# MAGIC
# MAGIC The For each tasks `scrape_regulators` and `reconcile_regulators` then run
# MAGIC `01_orchestrator` and `03_reconcile` once per regulator in the list.

# COMMAND ----------

import os
import sys

_notebook_dir = os.getcwd()
_repo_root = os.path.dirname(_notebook_dir) if os.path.basename(_notebook_dir) == 'notebooks' else _notebook_dir
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from scrapers.settings import ConfigError, load_common_config, resolve_path, resolve_regulator_ids
from scrapers.tracking import get_logger

logger = get_logger()

# COMMAND ----------

dbutils.widgets.text('regulators', 'ALL')
dbutils.widgets.text('common_config_path', 'config/common_config.json')

PARAM_REGULATORS = dbutils.widgets.get('regulators').strip()
COMMON_CONFIG_PATH = resolve_path(dbutils.widgets.get('common_config_path').strip(), _repo_root)

# COMMAND ----------

try:
    common = load_common_config(COMMON_CONFIG_PATH)
    REGULATOR_IDS = resolve_regulator_ids(common, PARAM_REGULATORS)
except (ConfigError, ValueError) as error:
    logger.error('Cannot resolve regulators from %r: %s', PARAM_REGULATORS, error)
    raise

logger.info('Parameter regulators %r resolved to %s', PARAM_REGULATORS or 'ALL', REGULATOR_IDS)

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(key='regulator_ids', value=REGULATOR_IDS)
except Exception as error:
    logger.error('Could not set the task value regulator_ids: %s', error)
    raise

dbutils.notebook.exit(','.join(REGULATOR_IDS))
