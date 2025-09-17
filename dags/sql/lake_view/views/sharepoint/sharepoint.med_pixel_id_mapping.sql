CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_PIXEL_ID_MAPPING AS
SELECT channel,
       store_id,
       pixel_id,
       pixel_name,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_admin_v1.pixel_id_mapping_table_pixel_id_mapping;
