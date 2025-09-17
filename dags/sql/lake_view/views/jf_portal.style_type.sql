CREATE VIEW IF NOT EXISTS lake_view.jf_portal.style_type AS
SELECT
   style_type_id,
   name,
   description,
   case_pack,
   sku_segment,
   user_create,
   user_update,
   date_create,
   date_update,
   round_to_case,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.style_type
WHERE
   NOT meta_is_deleted;
