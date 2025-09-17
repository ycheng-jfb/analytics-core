CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_vendor_type AS
SELECT
   po_vendor_type_id,
   name,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_vendor_type;
