CREATE VIEW IF NOT EXISTS lake_view.jf_portal.change_log_tbl_dtl AS
SELECT
   change_log_tbl_i_dtld,
   change_log_tbl_hdr_id,
   tbl_id,
   column_name,
   old_value,
   new_value,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.change_log_tbl_dtl;
