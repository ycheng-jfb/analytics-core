CREATE TRANSIENT TABLE IF NOT EXISTS reporting_prod.shared.marketing_channels_customer_clicks (
    customer_id NUMBER,
    store_group_id NUMBER,
    store_id NUMBER,
    contact_id VARCHAR,
    type VARCHAR,
    event_time TIMESTAMP_LTZ(3),
    source_table VARCHAR,
    source_store_id NUMBER,
    campaign_creative_id NUMBER,
    campaign_creative_name VARCHAR,
    message_id VARCHAR,
    message_name VARCHAR,
    subject VARCHAR,
    version_name VARCHAR
);
