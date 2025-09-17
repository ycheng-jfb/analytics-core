CREATE OR REPLACE VIEW lake_view.sharepoint.launchdarkly_api_called_tests AS
SELECT
    brand,
    key,
    test_name,
    winning_variation,
    called_at_utc,
    called_at_pst,
    convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.webanalytics_sharepoint_v1.launchdarkly_api_called_tests_sheet_1;
