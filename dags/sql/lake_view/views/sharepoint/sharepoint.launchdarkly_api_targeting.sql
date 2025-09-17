CREATE OR REPLACE VIEW lake_view.sharepoint.launchdarkly_api_targeting AS
SELECT
    brand ,
    key,
    test_name,
    property,
    operator,
    value,
    convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.webanalytics_sharepoint_v1.launchdarkly_api_targeting_sheet_1
