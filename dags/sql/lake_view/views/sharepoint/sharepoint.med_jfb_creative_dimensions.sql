CREATE OR REPLACE VIEW lake_view.sharepoint.med_jfb_creative_dimensions AS
SELECT
       creative_code,
       designer_initials,
       date_created_mmyy_ date_created,
       store_brand jf_brand,
       ad_type,
       video_length,
       audio,
       first_3_seconds_visual,
       font_used,
       product_shot_type,
       influencer_name,
       template_type,
       asset_size,
       creative_concept,
       platform_or_creative_format,
       sku_1,
       sku_2,
       sku_3,
       sku_4,
       sku_5,
       merchandise_type,
       number_of_styles_shown,
       offer_promo,
       offer_timing,
       offer_lockup_placement,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_naming_generators_v1.jfb_creative_dimensions_generator_jfb_creative_naming_generator
WHERE decode(trim(creative_code),'',NULL,creative_code) IS NOT NULL
    AND _line > 1;

