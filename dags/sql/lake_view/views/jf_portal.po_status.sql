CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_status AS
SELECT
   po_status_id,
   description,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_status;
