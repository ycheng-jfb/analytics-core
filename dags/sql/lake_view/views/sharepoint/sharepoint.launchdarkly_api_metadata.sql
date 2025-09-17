CREATE OR REPLACE VIEW lake_view.sharepoint.launchdarkly_api_metadata AS
SELECT
    brand,
    domain,
    site_country,
    adjusted_activated_datetime_utc,
    adjusted_activated_datetime_pst,
    test_ratio,
    test_split,
    test_name,
    test_variation_name,
    flag,
    test_variation_value,
    assignment,
    convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.webanalytics_sharepoint_v1.launchdarkly_api_metadata_sheet_1;
