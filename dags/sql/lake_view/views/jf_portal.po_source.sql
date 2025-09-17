CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_source AS
SELECT
   po_source_id,
   source,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_source;
