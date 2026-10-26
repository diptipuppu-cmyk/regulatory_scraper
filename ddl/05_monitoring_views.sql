/* =====================================================================
   05_monitoring_views.sql
   Views for the operations dashboard and queries for Databricks SQL alerts.
   Safe to run again at any time (CREATE OR REPLACE VIEW).
   Timestamps are stored as true instants (the notebooks write them with
   session time zone Asia/Singapore), so comparisons use current_timestamp().
   Schema : ai_platform_dev.regulatory_scraping
   ===================================================================== */


/* 1. One row per run with its final status (RUNNING when no final row yet) */
CREATE OR REPLACE VIEW ai_platform_dev.regulatory_scraping.v_run_history AS
WITH runs AS (
    SELECT run_id, stage, regulator_id,
           min(event_time_sgt)                                        AS started_sgt,
           max_by(status, event_time_sgt)                             AS final_status,
           max(CASE WHEN status <> 'RUNNING' THEN end_time_sgt END)   AS ended_sgt,
           max(scan_start_date)                                       AS scan_start_date,
           max(scan_end_date)                                         AS scan_end_date,
           max(items_found)                                           AS items_found,
           max(documents_found)                                       AS documents_found,
           max(documents_new)                                         AS documents_new,
           max(documents_changed)                                     AS documents_changed,
           max(documents_skipped)                                     AS documents_skipped,
           max(documents_copied)                                      AS documents_copied,
           max(documents_failed)                                      AS documents_failed,
           max_by(error_details, event_time_sgt)                      AS error_details
    FROM ai_platform_dev.regulatory_scraping.scraping_audit_log
    WHERE record_type = 'RUN'
    GROUP BY run_id, stage, regulator_id
)
SELECT *,
       round((unix_timestamp(ended_sgt) - unix_timestamp(started_sgt)) / 60.0, 1) AS duration_minutes
FROM runs;


/* 2. Coverage Log of the latest finished scrape run per regulator (Section 1 of the report) */
CREATE OR REPLACE VIEW ai_platform_dev.regulatory_scraping.v_latest_scrape_coverage AS
WITH latest AS (
    SELECT regulator_id, max_by(run_id, started_sgt) AS run_id
    FROM ai_platform_dev.regulatory_scraping.v_run_history
    WHERE stage = 'SCRAPE' AND final_status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'FAILED')
    GROUP BY regulator_id
)
SELECT a.regulator_id, a.run_id, a.scan_start_date, a.scan_end_date,
       a.category_id, a.category_name, a.status, a.items_found, a.documents_found,
       a.documents_new, a.documents_changed, a.documents_skipped, a.documents_failed,
       CASE WHEN a.status = 'SUCCESS' AND a.items_found = 0 THEN 'No items found in scan period'
            WHEN a.status = 'FAILED' THEN concat('Category failed: ', coalesce(a.error_details, ''))
            ELSE NULL END AS remarks
FROM ai_platform_dev.regulatory_scraping.scraping_audit_log a
JOIN latest l ON a.run_id = l.run_id
WHERE a.record_type = 'CATEGORY';


/* 3. Documents not yet copied, with age */
CREATE OR REPLACE VIEW ai_platform_dev.regulatory_scraping.v_copy_backlog AS
SELECT regulator_id, category_id, document_id, file_name, current_version, copy_status,
       copy_retry_count, copy_error_details, last_changed_sgt,
       round((unix_timestamp(current_timestamp())
              - unix_timestamp(last_changed_sgt)) / 3600.0, 1) AS age_hours
FROM ai_platform_dev.regulatory_scraping.pdf_catalog
WHERE copy_status <> 'COPIED';


/* 4. Failures in the last 14 days, all stages */
CREATE OR REPLACE VIEW ai_platform_dev.regulatory_scraping.v_recent_failures AS
SELECT event_time_sgt, stage, record_type, run_id, regulator_id, category_id, item_title,
       document_url, file_name, status, error_category, error_details
FROM ai_platform_dev.regulatory_scraping.scraping_audit_log
WHERE status IN ('FAILED', 'ABANDONED', 'ORPHAN', 'MISSING')
  AND event_time_sgt >= current_timestamp() - INTERVAL 14 DAYS;


/* =====================================================================
   Alert queries (Databricks SQL > Alerts). Each returns one row with
   value; trigger the alert when value > 0.
   ===================================================================== */

/* A1. No successful scrape in the last 35 days (monthly schedule plus margin) */
/*
SELECT CASE WHEN max(started_sgt) IS NULL
              OR max(started_sgt) < current_timestamp() - INTERVAL 35 DAYS
            THEN 1 ELSE 0 END AS value
FROM ai_platform_dev.regulatory_scraping.v_run_history
WHERE stage = 'SCRAPE' AND final_status IN ('SUCCESS', 'PARTIAL_SUCCESS');
*/

/* A2. Copy backlog older than 48 hours */
/*
SELECT count(*) AS value
FROM ai_platform_dev.regulatory_scraping.v_copy_backlog
WHERE age_hours > 48;
*/

/* A3. Latest scrape found zero items in every category (possible silent source change) */
/*
SELECT CASE WHEN sum(items_found) = 0 THEN 1 ELSE 0 END AS value
FROM ai_platform_dev.regulatory_scraping.v_latest_scrape_coverage;
*/

/* A4. Failed or abandoned runs in the last 24 hours */
/*
SELECT count(*) AS value
FROM ai_platform_dev.regulatory_scraping.v_run_history
WHERE final_status IN ('FAILED', 'ABANDONED')
  AND started_sgt >= current_timestamp() - INTERVAL 24 HOURS;
*/


/* =====================================================================
   Table maintenance (schedule weekly, for example as a SQL task)
   ===================================================================== */
/*
OPTIMIZE ai_platform_dev.regulatory_scraping.pdf_catalog;
OPTIMIZE ai_platform_dev.regulatory_scraping.scraping_audit_log;
VACUUM   ai_platform_dev.regulatory_scraping.pdf_catalog;
VACUUM   ai_platform_dev.regulatory_scraping.scraping_audit_log;
*/


/* =====================================================================
   Governance tags (agree the values with SMBC data governance first).
   Enforcement action pages name individuals, so the raw volume and the
   catalog hold personal data.
   ===================================================================== */
/*
ALTER TABLE  ai_platform_dev.regulatory_scraping.pdf_catalog        SET TAGS ('data_classification' = 'confidential', 'contains_personal_data' = 'true');
ALTER TABLE  ai_platform_dev.regulatory_scraping.scraping_audit_log SET TAGS ('data_classification' = 'confidential', 'contains_personal_data' = 'true');
ALTER VOLUME ai_platform_dev.regulatory_scraping.raw                SET TAGS ('data_classification' = 'confidential', 'contains_personal_data' = 'true');
*/
