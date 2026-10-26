/* =====================================================================
   04_create_pdf_catalog_and_audit.sql
   Replaces 03_create_audit_tables.sql.
   Schema : ai_platform_dev.regulatory_scraping

   Before running in the lab:
     1. Confirm the rows in the current scraping_audit_log are not needed.
     2. Rename the old table so the new structure can be created:
        ALTER TABLE ai_platform_dev.regulatory_scraping.scraping_audit_log
          RENAME TO ai_platform_dev.regulatory_scraping.scraping_audit_log_legacy;
   ===================================================================== */


/* =====================================================================
   1. pdf_catalog
      One row per unique document (regulator plus normalised URL).
      PDF documents and HTML page snapshots (speeches, media releases,
      enforcement actions) both live here, told apart by content_format.
      Holds the CURRENT version and the copy state.
      The copy job reads only this table to decide what to pick up.
   ===================================================================== */
CREATE TABLE IF NOT EXISTS ai_platform_dev.regulatory_scraping.pdf_catalog (
    document_id            STRING    NOT NULL COMMENT 'sha256 of regulator_id and normalised document_url',
    regulator_id           STRING    NOT NULL,
    category_id            STRING    NOT NULL COMMENT 'Coverage checklist category from the regulator config',
    content_format         STRING    NOT NULL COMMENT 'PDF or HTML',

    item_title             STRING             COMMENT 'Title of the regulatory item (listing entry)',
    item_date              DATE               COMMENT 'Date used for the scan period filter',
    item_date_label        STRING             COMMENT 'Meaning of item_date as shown by MAS, for example Published Date',
    item_reference         STRING             COMMENT 'MAS content number where available, for example P017 2025',
    source_url             STRING    NOT NULL COMMENT 'Item detail page where the document was found',
    document_url           STRING    NOT NULL COMMENT 'Normalised document URL (query string removed)',
    link_text              STRING,
    file_name              STRING    NOT NULL,

    current_version        INT       NOT NULL COMMENT 'Starts at 1 and increases by 1 on each content change',
    content_hash_sha256    STRING    NOT NULL COMMENT 'Hash of the current version',
    file_size_bytes        BIGINT,
    raw_file_path          STRING    NOT NULL COMMENT 'Versioned path in the external raw volume',
    scrape_status          STRING    NOT NULL COMMENT 'NEW or CHANGED, describes the current version',

    first_seen_run_id      STRING,
    first_seen_sgt         TIMESTAMP,
    last_seen_run_id       STRING             COMMENT 'Updated on every run, including SKIPPED',
    last_seen_sgt          TIMESTAMP,
    last_changed_sgt       TIMESTAMP,

    malware_scan_verdict   STRING             COMMENT 'Defender for Storage verdict, copy only when clean',
    scan_timestamp_sgt     TIMESTAMP,

    copy_status            STRING    NOT NULL COMMENT 'PENDING, COPIED, FAILED, BLOCKED',
    copied_version         INT                COMMENT 'Version last copied to internal storage',
    copied_hash_sha256     STRING             COMMENT 'Hash verified after copy',
    internal_file_path     STRING,
    copied_at_sgt          TIMESTAMP,
    copy_retry_count       INT       NOT NULL,
    copy_error_details     STRING,

    CONSTRAINT pk_pdf_catalog PRIMARY KEY (document_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Current state of every scraped regulatory document. Source of truth for the copy job.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'
);


/* =====================================================================
   2. scraping_audit_log
      Append only, never updated. record_type tells the grain:
        RUN       one row at start (RUNNING) and one at end, per job run per stage
        CATEGORY  one row per coverage category per run (feeds the Coverage Log)
        ITEM      one row per regulatory item found in the scan period
        FILE      one row per document per run per stage

      stage  : SCRAPE (external workspace) or COPY (internal workspace)

      status values
        RUN               RUNNING, SUCCESS, PARTIAL_SUCCESS, FAILED
        CATEGORY          SUCCESS, FAILED
        ITEM              SUCCESS, PARTIAL_SUCCESS, FAILED
        FILE and SCRAPE   NEW, CHANGED, SKIPPED, FAILED
        FILE and COPY     COPIED, FAILED, BLOCKED

      skip_reason values (only when status = SKIPPED)
        SAME_URL_SAME_HASH   same document, content unchanged since last run
        SAME_HASH_OTHER_URL  identical content already catalogued under another URL
        DUPLICATE_IN_RUN     same URL or same content already handled earlier in this run
   ===================================================================== */
CREATE TABLE IF NOT EXISTS ai_platform_dev.regulatory_scraping.scraping_audit_log (
    audit_id                 STRING    NOT NULL,
    run_id                   STRING    NOT NULL,
    record_type              STRING    NOT NULL COMMENT 'RUN, CATEGORY, ITEM or FILE',
    stage                    STRING    NOT NULL COMMENT 'SCRAPE or COPY',
    regulator_id             STRING    NOT NULL,
    event_time_sgt           TIMESTAMP NOT NULL,
    status                   STRING    NOT NULL,

    scan_start_date          DATE,
    scan_end_date            DATE,
    category_id              STRING,
    category_name            STRING,

    item_title               STRING,
    item_date                DATE,
    item_date_label          STRING,
    item_reference           STRING,
    source_url               STRING,

    document_id              STRING,
    document_url             STRING,
    content_format           STRING,
    file_name                STRING,
    version                  INT,
    content_hash_sha256      STRING,
    file_size_bytes          BIGINT,
    raw_file_path            STRING,
    internal_file_path       STRING,
    skip_reason              STRING,
    duplicate_of_document_id STRING,
    malware_scan_verdict     STRING,

    start_time_sgt           TIMESTAMP,
    end_time_sgt             TIMESTAMP,
    items_found              INT,
    documents_found          INT,
    documents_new            INT,
    documents_changed        INT,
    documents_skipped        INT,
    documents_copied         INT,
    documents_failed         INT,

    retry_count              INT,
    error_category           STRING,
    error_details            STRING
)
USING DELTA
COMMENT 'Append only event trail for scraping and copy stages.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.appendOnly'                 = 'true'
);


/* =====================================================================
   3. Handy checks
   ===================================================================== */

/* Coverage Log for the latest scrape run */
/*
SELECT category_id, category_name, status, items_found, documents_found,
       documents_new, documents_changed, documents_skipped, documents_failed, error_details
FROM   ai_platform_dev.regulatory_scraping.scraping_audit_log
WHERE  record_type = 'CATEGORY'
  AND  run_id = (SELECT max(run_id) FROM ai_platform_dev.regulatory_scraping.scraping_audit_log
                 WHERE stage = 'SCRAPE' AND record_type = 'RUN')
ORDER  BY category_id;
*/

/* What the copy job will pick up next */
/*
SELECT document_id, category_id, file_name, current_version, copy_status, copy_retry_count
FROM   ai_platform_dev.regulatory_scraping.pdf_catalog
WHERE  copy_status IN ('PENDING', 'FAILED', 'BLOCKED')
ORDER  BY last_changed_sgt;
*/
