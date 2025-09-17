CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_change_reason_code AS
SELECT
   po_change_reason_code_id,
   reason,
   description,
   show,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_change_reason_code;
