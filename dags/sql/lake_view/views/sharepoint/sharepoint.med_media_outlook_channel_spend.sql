CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_MEDIA_OUTLOOK_CHANNEL_SPEND AS
select month::date as month,
    media_outlook_type,
    store_brand_name,
    'NA' as region,
    channel,
    projected_spend,
    total_vip_cac_target,
    projected_percentage,
    top_down_channel_name,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.MED_SHAREPOINT_ACQUISITION_V1.MEDIA_FORECASTING_REPORT_MEDIA_OUTLOOK_CHANNEL_SPEND
union
select month::date as month,
    media_outlook_type,
    store_brand_name,
    'EU' as region,
    channel,
    projected_spend,
    total_vip_cac_target,
    projected_percentage,
    top_down_channel_name,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.MED_SHAREPOINT_ACQUISITION_V1.MEDIA_FORECASTING_REPORT_MEDIA_OUTLOOK_CHANNEL_SPEND_EU;
