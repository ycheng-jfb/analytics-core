CREATE VIEW IF NOT EXISTS lake_view.jf_portal.department AS
SELECT
   department_id,
   department,
   division_id,
   email_from_addr,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.department;
