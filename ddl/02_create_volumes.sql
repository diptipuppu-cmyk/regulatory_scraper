-- Managed Volumes for internal pipeline assets
CREATE VOLUME IF NOT EXISTS ${catalog_name}.control.init_scripts;
CREATE VOLUME IF NOT EXISTS ${catalog_name}.control.packages;
CREATE VOLUME IF NOT EXISTS ${catalog_name}.control.config;

-- External Volume pointing to the validated ADLS Gen2 trusted zone
CREATE EXTERNAL VOLUME IF NOT EXISTS ${catalog_name}.gold_compliance.trusted_docs
    LOCATION 'abfss://trusted@${storage_account}.dfs.core.windows.net/';