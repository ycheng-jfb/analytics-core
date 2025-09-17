CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_type AS
SELECT
   po_type_id,
   po_vendor_type_id,
   name,
   po_number_suffix,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_type;
