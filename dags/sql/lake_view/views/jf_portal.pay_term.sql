CREATE VIEW IF NOT EXISTS lake_view.jf_portal.pay_term AS
SELECT
   pay_term_id,
   name,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.pay_term;
