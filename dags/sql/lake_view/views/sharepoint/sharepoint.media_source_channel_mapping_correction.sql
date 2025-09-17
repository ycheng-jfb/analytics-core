CREATE OR REPLACE VIEW lake_view.sharepoint.media_source_channel_mapping_correction AS
SELECT channel_update,
       utm_campaign,
       utm_medium,
       subchannel_update,
       utm_source,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.webanalytics_sharepoint_v1.media_source_channel_mapping_correction_src_channel_mapping_correction ;
