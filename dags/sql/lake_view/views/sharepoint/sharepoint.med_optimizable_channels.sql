CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_OPTIMIZABLE_CHANNELS AS
SELECT store_brand_name,
       channel,
       is_optimizable::BOOLEAN is_optimizable,
       is_monthly_optimizable::BOOLEAN is_monthly_optimizable,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_admin_v1.top_down_optimizable_channels_top_down_optimizable_channels;
