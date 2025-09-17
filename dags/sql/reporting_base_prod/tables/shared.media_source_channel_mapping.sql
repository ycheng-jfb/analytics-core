DROP TABLE IF EXISTS reporting_base_prod.shared.media_source_channel_mapping;
CREATE TABLE reporting_base_prod.shared.media_source_channel_mapping (
    media_source_hash BIGINT,
    event_source VARCHAR(25),
    channel_type VARCHAR(25),
    channel VARCHAR(25),
    subchannel VARCHAR(25),
    vendor VARCHAR(25),
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    utm_campaign VARCHAR(255),
    gateway_type VARCHAR(255),
    gateway_sub_type VARCHAR(255),
    hdyh_value VARCHAR(255),
    seo_vendor VARCHAR(25)
);
