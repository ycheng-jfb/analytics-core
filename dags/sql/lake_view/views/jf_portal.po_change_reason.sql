CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_change_reason AS
SELECT
   po_change_reason_id,
   po_change_reason_code_id,
   po_id,
   version,
   doc_comment_id,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_change_reason;
