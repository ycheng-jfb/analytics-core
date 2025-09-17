CREATE VIEW IF NOT EXISTS lake_view.jf_portal.wholesaler AS
SELECT
   wholesaler_id,
   name,
   po_suffix,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.wholesaler;
