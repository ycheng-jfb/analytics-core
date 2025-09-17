CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_CUSTOM_MEDIA_OUTLOOK AS
SELECT store_brand_name,
       region,
       month,
       custom_media_outlook::INT custom_media_outlook,
       custom_vips::INT custom_vips,
       custom_cac::INT custom_cac,
       date::DATE date,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_admin_v1.custom_media_outlook_conversion_scenarios_dashboard_custom_media_outlook;
