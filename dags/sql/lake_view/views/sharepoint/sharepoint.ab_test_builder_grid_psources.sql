CREATE OR REPLACE VIEW lake_view.sharepoint.ab_test_builder_grid_psources AS
SELECT psource_1,
       psource_2,
       psource_3,
       psource_4,
       psource_5,
       psource_6,
       test_label,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.webanalytics_sharepoint_v1.ab_test_builder_grid_psources_sheet_1;
