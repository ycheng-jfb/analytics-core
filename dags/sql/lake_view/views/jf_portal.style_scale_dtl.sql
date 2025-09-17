CREATE VIEW IF NOT EXISTS lake_view.jf_portal.style_scale_dtl AS
SELECT
   style_scale_dtl_id,
   scale,
   name,
   "ORDER",
   sku_segment,
   user_create,
   user_update,
   date_create,
   date_update,
   mv2_oid,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.style_scale_dtl;
