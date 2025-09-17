CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_BIG_QUERY_RESULT as
SELECT date:: date as date,
        spend,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.web_analytics_usage_summary_big_query;
