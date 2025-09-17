CREATE VIEW IF NOT EXISTS lake_view.jf_portal.inco_term AS
SELECT
   inco_term_id,
   name,
   code,
   geo,
   date_create,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.inco_term;
