CREATE VIEW IF NOT EXISTS LAKE_VIEW.EMARSYS.PUSH_CAMPAIGNS AS
SELECT
    csm.store_group,
    p.application_id,
    p.campaign_id,
    p.created_at,
    p.customer_id,
    p.deleted_at,
    p.event_time,
    p.launched_at,
    p.loaded_at,
    p.name,
    p.push_internal_campaign_id,
    p.scheduled_at,
    p.segment_id,
    p.source,
    p.status,
    p.target,
    p.meta_create_datetime,
    TRIM(p.message[0]:v:f[0]:v, '"') as message_key,
    TRIM(p.message[0]:v:f[1]:v, '"') as message_value,
    TRIM(p.title[0]:v:f[0]:v, '"') as title_key,
    TRIM(p.title[0]:v:f[1]:v, '"') as title_value
FROM LAKE.EMARSYS.push_campaigns p
JOIN lake.emarsys.customer_store_mapping csm on csm.customer_id = p.customer_id;
