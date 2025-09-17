CREATE VIEW IF NOT EXISTS lake_view.jf_portal.freight_method AS
SELECT
   freight_method_id,
   name,
   code,
   geo,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.freight_method;
