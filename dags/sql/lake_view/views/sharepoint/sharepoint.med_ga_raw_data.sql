CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_GA_RAW_DATA as
SELECT name as name,
        id,
        total_hit_volume,
        billable_hit_volume,
        type,
        date::date as date,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.web_analytics_usage_summary_ga_raw_data;
