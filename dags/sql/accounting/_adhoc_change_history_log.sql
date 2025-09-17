-- This log file serves as a record for modifications to be made when there are no file changes available for a PR.
-- Ex operations: Adhoc updates, full refreshes, dropping some backup tables, restoring a table from a backup.
-- Note: If a table is truncate and load, its preferable to retrigger the task through Airflow

-- Jan 15th 2025 : DA-34877 - Truncating the month_end.loyalty_points, the month_end.loyalty_points_balance and the
--                            month_end.loyalty_points_detail to perform a full refresh

--20250123 DA-35039 - udpate accounting.backup.breakage_savagex_us_breakage_report with
--                      backup table accounting.backup.breakage_savagex_us_breakage_report_20250123_163742

-- 20250301 DA-35537 (1) - Altering the column size of expired_to_date from NUMBER(4,0) to NUMBER(20,0) for
--                     breakage.jfb_sxf_membership_credit_breakage_report

-- 20250301 DA-35537 (2) - Altering the column size of expired_to_date from NUMBER(1,0) to NUMBER(20,0) for
--                     breakage.jfb_sxf_membership_credit_breakage_report_snapshot

-- 20250401 DA-35932 - Breakage by State Actual | backfill deferred_recognition_label
