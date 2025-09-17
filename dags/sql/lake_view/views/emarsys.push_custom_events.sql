CREATE VIEW IF NOT EXISTS LAKE_VIEW.EMARSYS.PUSH_CUSTOM_EVENTS AS
SELECT
    csm.store_group,
    p.application_code,
    p.application_id,
    p.contact_id,
    p.customer_id,
    p.event_name,
    p.event_time,
    p.hardware_id,
    p.loaded_at,
    p.meta_create_datetime
FROM LAKE.EMARSYS.push_custom_events p
JOIN lake.emarsys.customer_store_mapping csm on csm.customer_id = p.customer_id;
