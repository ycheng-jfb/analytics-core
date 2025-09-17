CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_GOOGLE_ADS_CONVERSION_MAPPING AS
select brand,
    country,
    region,
    mcc_id,
    conversion_event_type,
    conversion_event_name,
    count,
    click_through_conversion_window,
    engaged_view_conversion_window,
    view_through_conversion_window,
    conversion_id,
    conversion_label,
    conversion_tracker_id,
    start_date::date as start_date,
    end_date::date as end_date,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.MED_SHAREPOINT_ADMIN_V1.GOOGLE_ADS_CONVERSIONS_MAPPING_MCC_EU_CONSOLIDATION

UNION

select brand,
    country,
    region,
    mcc_id,
    conversion_event_type,
    conversion_event_name,
    count,
    click_through_conversion_window,
    engaged_view_conversion_window,
    view_through_conversion_window,
    conversion_id,
    conversion_label,
    conversion_tracker_id,
    start_date::date as start_date,
    end_date::date as end_date,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.MED_SHAREPOINT_ADMIN_V1.GOOGLE_ADS_CONVERSIONS_MAPPING_MCC_NA_CONSOLIDATION;
