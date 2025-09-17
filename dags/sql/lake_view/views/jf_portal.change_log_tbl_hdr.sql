CREATE VIEW IF NOT EXISTS lake_view.jf_portal.change_log_tbl_hdr AS
SELECT
   change_log_tbl_hdr_id,
   change_log_hdr_id,
   table_name,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.change_log_tbl_hdr;
