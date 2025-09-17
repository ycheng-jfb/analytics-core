
CREATE OR REPLACE VIEW reporting_base.shared.vw_session_media_channel AS
SELECT
    session_id,
    store_id,
    customer_id,
    session_local_datetime,
    media_source_hash,
    utm_source,
    utm_medium,
    utm_campaign,
    gateway_type,
    gateway_sub_type,
    seo_vendor,
    ip,
    channel,
    subchannel,
    cleansed_channel_type,
    cleansed_channel,
    cleansed_subchannel,
    last_non_direct_media_source_hash
FROM staging.session_media_channel;
