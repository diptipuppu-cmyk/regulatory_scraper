CREATE WIDGET TEXT catalog_name DEFAULT "regulatory_catalog_dev";

-- Reference the widget using :catalog_name or getArgument()
CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog_name);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog_name).control
COMMENT 'Holds crawler metadata, config files, package wheels, and audit logs';

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog_name).gold_compliance
COMMENT 'Stores malware-scanned, verified documents and parsed regulatory text';

-- Creates catalog based on target environment prefix/suffix
CREATE CATALOG IF NOT EXISTS ${catalog_name};

-- Create schemas
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.control
COMMENT 'Holds crawler metadata, config files, package wheels, and audit logs';

CREATE SCHEMA IF NOT EXISTS ${catalog_name}.gold_compliance
COMMENT 'Stores malware-scanned, verified documents and parsed regulatory text';