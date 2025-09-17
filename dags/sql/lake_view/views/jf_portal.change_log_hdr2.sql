CREATE VIEW IF NOT EXISTS lake_view.jf_portal.change_log_hdr2 AS
SELECT
   change_log_hdr_id,
   doc_comment_id,
   doc_version,
   doc_type_id,
   doc_id,
   po_change_reason_id,
   user_name,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.change_log_hdr2;
