/* =====================================================================
   06_drop_copy_columns_one_time.sql
   Run ONCE on an existing pdf_catalog, AFTER the updated tracking.py and
   base_scraper.py are in the repo. Keeps all existing rows.
   New environments do not need this: 03_create_pdf_catalog_and_audit.sql
   already creates the table without the copy columns.
   ===================================================================== */

ALTER TABLE ai_platform_dev.regulatory_scraping.pdf_catalog
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE ai_platform_dev.regulatory_scraping.pdf_catalog
  DROP COLUMNS (copy_status, copied_version, copied_hash_sha256, internal_file_path,
                copied_at_sgt, copy_retry_count, copy_error_details);

DROP VIEW IF EXISTS ai_platform_dev.regulatory_scraping.v_copy_backlog;
