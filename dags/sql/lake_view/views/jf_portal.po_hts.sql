CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_hts AS
SELECT
   po_hts_id,
   hts_id,
   hts_code,
   po_id,
   color,
   duty,
   fixed_value,
   read_only,
   description,
   user_create,
   user_update,
   date_create,
   date_update,
   use_to_calc_duty,
   tariff,
   overwritten,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_hts
WHERE
   meta_is_deleted = 0;
