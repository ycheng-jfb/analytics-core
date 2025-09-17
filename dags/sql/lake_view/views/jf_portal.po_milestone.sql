CREATE VIEW IF NOT EXISTS lake_view.jf_portal.po_milestone AS
SELECT
   po_milestone_id,
   milestone_event_id,
   po_id,
   name,
   "ORDER",
   date_est,
   date_actual,
   date_sched,
   date_complete,
   user_create,
   date_create,
   user_update,
   date_update,
   meta_create_datetime,
   meta_update_datetime
FROM
   lake.jf_portal.po_milestone
WHERE
   NOT meta_is_deleted;
