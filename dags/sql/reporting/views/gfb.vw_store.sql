CREATE OR REPLACE VIEW reporting.gfb.vw_store
        (
         store_id,
         store_group_id,
         store_group,
         store_name,
         store_name_region,
         store_full_name,
         store_brand,
         store_brand_abbr,
         store_type,
         store_sub_type,
         is_core_store,
         store_country,
         store_region,
         store_division,
         store_division_abbr,
         store_currency,
         store_time_zone,
         store_retail_state,
         store_retail_city,
         store_retail_location,
         store_retail_zip_code,
         store_retail_location_code,
         store_retail_status,
         store_retail_region,
         store_retail_district,
         meta_create_datetime,
         meta_update_datetime
            )
AS
SELECT store_id,
       store_group_id,
       store_group,
       store_name,
       store_name_region,
       store_full_name,
       store_brand,
       store_brand_abbr,
       store_type,
       store_sub_type,
       is_core_store,
       store_country,
       store_region,
       store_division,
       store_division_abbr,
       store_currency,
       store_time_zone,
       store_retail_state,
       store_retail_city,
       store_retail_location,
       store_retail_zip_code,
       store_retail_location_code,
       store_retail_status,
       store_retail_region,
       store_retail_district,
       meta_create_datetime,
       meta_update_datetime
FROM edw.data_model.dim_store st
WHERE st.store_brand_abbr IN ('JF', 'SD', 'FK')
  AND st.store_full_name NOT LIKE '%(DM)%'
  AND st.store_full_name NOT LIKE '%Wholesale%'
  AND st.store_full_name NOT LIKE '%Heels.com%'
  AND st.store_full_name NOT LIKE '%Retail%'
  AND st.store_full_name NOT LIKE '%Sample%'
  AND st.store_full_name NOT LIKE '%SWAG%'
  AND st.store_full_name NOT LIKE '%PS%';
