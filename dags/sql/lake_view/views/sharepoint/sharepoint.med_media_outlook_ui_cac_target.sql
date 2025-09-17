CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_MEDIA_OUTLOOK_UI_CAC_TARGET AS
SELECT month::date as month,
    store_brand_name,
    'NA' as region,
    channel,
    member_segment,
    ui_cac_target,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_ACQUISITION_V1.MEDIA_FORECASTING_REPORT_MEDIA_OUTLOOK_UI_CAC_TARGET
union
SELECT month::date as month,
    store_brand_name,
    'EU' as region,
    channel,
    member_segment,
    ui_cac_target,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_ACQUISITION_V1.MEDIA_FORECASTING_REPORT_MEDIA_OUTLOOK_UI_CAC_TARGET_EU;
