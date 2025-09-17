CREATE OR REPLACE VIEW lake_view.sharepoint.daily_cash_mapping_definition AS
SELECT rank,
       report_availability,
       business_unit,
       report_mapping,
       description,
       _fivetran_synced AS meta_create_datetime,
       _fivetran_synced AS meta_update_datetime
FROM lake_fivetran.fpa_nonconfidential_sharepoint_v1.daily_cash_report_mapping_documentation_sheet_1
    QUALIFY ROW_NUMBER() OVER(PARTITION BY rank ORDER BY _fivetran_synced desc) = 1;
