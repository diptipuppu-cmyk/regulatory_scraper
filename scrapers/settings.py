'''
Common configuration and the regulator registry.

config/common_config.json holds what every regulator shares (tables, volume path, scan
defaults) and the list of regulators with their config file and scraper class.
Each regulator's own website rules stay in config/regulators/<regulator>_config.json.

The orchestrator uses this module to:
  1. load and validate the common config;
  2. look up one regulator by its id (the regulator_id job parameter);
  3. import that regulator's scraper class.

The resolve notebook (00_resolve_regulators) uses it to turn the regulators job parameter
(ALL, MAS, PDPC or MAS,PDPC) into the list of regulators for one job run.
'''

import importlib
import json
import os
import re

REQUIRED_KEYS = ('catalog', 'schema', 'raw_volume_path', 'defaults', 'regulators')
REQUIRED_DEFAULTS = ('scan_mode', 'last_n_days', 'overlap_days', 'max_window_days',
                     'fail_on_partial', 'stale_run_hours')
REQUIRED_REGULATOR_KEYS = ('config_path', 'scraper_module', 'scraper_class')

_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_REGULATOR_ID = re.compile(r'^[A-Z0-9_]{2,20}$')


class ConfigError(Exception):
    '''Raised for any problem in the common config or the regulator registry.'''


def validate_common_config(config):
    problems = []
    for key in REQUIRED_KEYS:
        if key not in config or config[key] in (None, '', {}):
            problems.append(f'missing {key}')
    for key in ('catalog', 'schema'):
        value = config.get(key)
        if value and not _IDENTIFIER.match(str(value)):
            problems.append(f'{key} {value!r} is not a valid Unity Catalog name')
    if config.get('raw_volume_path') and not str(config['raw_volume_path']).startswith('/Volumes/'):
        problems.append('raw_volume_path must start with /Volumes/')

    defaults = config.get('defaults') or {}
    for key in REQUIRED_DEFAULTS:
        if key not in defaults:
            problems.append(f'defaults: missing {key}')

    for regulator_id, entry in (config.get('regulators') or {}).items():
        if not _REGULATOR_ID.match(regulator_id):
            problems.append(f'regulators: id {regulator_id!r} must be upper case letters, digits or underscore')
        for key in REQUIRED_REGULATOR_KEYS:
            if not (entry or {}).get(key):
                problems.append(f'regulators.{regulator_id}: missing {key}')
        module = (entry or {}).get('scraper_module') or ''
        if module and not module.startswith('scrapers.'):
            problems.append(f'regulators.{regulator_id}: scraper_module must be inside the scrapers package')

    if problems:
        raise ConfigError('Invalid common config: ' + '; '.join(problems))
    return config


def load_common_config(path):
    try:
        with open(path, 'r', encoding='utf8') as handle:
            config = json.load(handle)
    except FileNotFoundError:
        raise ConfigError(f'Common config not found: {path}')
    except json.JSONDecodeError as error:
        raise ConfigError(f'Common config {path} is not valid JSON: {error}')
    return validate_common_config(config)


def get_regulator_entry(config, regulator_id):
    '''Registry entry for one regulator. Fails clearly if unknown or disabled.'''
    regulator_id = (regulator_id or '').strip().upper()
    if not regulator_id:
        raise ConfigError('regulator_id job parameter is required, for example MAS')
    regulators = config['regulators']
    if regulator_id not in regulators:
        raise ConfigError(f'Unknown regulator_id {regulator_id}. Known: {sorted(regulators)}')
    entry = regulators[regulator_id]
    if not entry.get('enabled', False):
        raise ConfigError(f'Regulator {regulator_id} is disabled in the common config')
    return regulator_id, entry


def resolve_regulator_ids(config, regulators_text):
    '''Which regulators one job run should scrape, from the regulators job parameter.

    ALL (or empty)   every regulator with enabled true in the common config, in config order.
    MAS              one regulator.
    MAS,PDPC         several regulators, comma separated, in the order typed.

    A regulator named explicitly must exist and be enabled, otherwise the run stops, so a
    typo never turns into a silent run that scrapes nothing.'''
    text = (regulators_text or '').strip().upper()
    regulators = config['regulators']

    if text in ('', 'ALL'):
        ids = [rid for rid, entry in regulators.items() if (entry or {}).get('enabled', False)]
        if not ids:
            raise ConfigError('regulators is ALL but no regulator has enabled true in the common config')
        return ids

    ids = []
    for part in text.split(','):
        part = part.strip()
        if not part:
            continue
        if part == 'ALL':
            raise ConfigError('ALL cannot be combined with other regulators')
        regulator_id, _ = get_regulator_entry(config, part)
        if regulator_id not in ids:
            ids.append(regulator_id)
    if not ids:
        raise ConfigError(f'No regulator found in the regulators parameter {regulators_text!r}')
    return ids


def resolve_path(path, repo_root):
    return path if os.path.isabs(path) else os.path.join(repo_root, path)


def load_scraper_class(entry, base_class):
    '''Import the scraper class named in the registry and check it is a proper scraper.'''
    module_name, class_name = entry['scraper_module'], entry['scraper_class']
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise ConfigError(f'Cannot import scraper module {module_name}: {error}')
    scraper_class = getattr(module, class_name, None)
    if scraper_class is None:
        raise ConfigError(f'Scraper class {class_name} not found in {module_name}')
    if not isinstance(scraper_class, type) or not issubclass(scraper_class, base_class):
        raise ConfigError(f'{module_name}.{class_name} must be a subclass of {base_class.__name__}')
    return scraper_class
