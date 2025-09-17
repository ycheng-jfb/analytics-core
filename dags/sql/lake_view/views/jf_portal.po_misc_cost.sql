CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_misc_cost AS
SELECT
   po_misc_cost_id,
   po_id,
   description,
   misc_cost_id,
   allocation_id,
   misc_cost_type_id,
   cost_type,
   color,
   vendor_id,
   vendor_name,
   percentage,
   fixed_value,
   hidden,
   user_create,
   user_update,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_misc_cost
WHERE
   NOT meta_is_deleted;
