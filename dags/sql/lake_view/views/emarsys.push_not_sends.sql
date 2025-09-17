CREATE VIEW IF NOT EXISTS LAKE_VIEW.EMARSYS.PUSH_NOT_SENDS AS
SELECT
    csm.store_group,
    p.application_code,
    p.application_id,
    p.campaign_id,
    p.contact_id,
    p.customer_id,
    p.event_time,
    p.hardware_id,
    p.loaded_at,
    p.platform,
    p.program_id,
    p.push_token,
    p.reason,
    p.source_id,
    p.source_type,
    p.meta_create_datetime
FROM LAKE.EMARSYS.push_not_sends p
JOIN lake.emarsys.customer_store_mapping csm on csm.customer_id = p.customer_id;
