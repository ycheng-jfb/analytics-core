CREATE OR REPLACE VIEW lake_view.sharepoint.med_fk_creative_dimensions AS
SELECT
       creative_code,
       designer_initials,
       date_created_mmyy_ date_created,
       ad_type,
       first_3_seconds_visual,
       shot_type,
       influencer_name,
       template,
       asset_size,
       adset_creative_concept,
       background,
       platform_or_creative_format,
       sku_1,
       sku_2,
       sku_3,
       sku_4,
       sku_5,
       merchandise_type,
       number_of_styles_shown,
       gender,
       offer_promo,
       offer_lockup,
       messaging_theme,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_naming_generators_v1.fk_creative_dimensions_generator_fk_creative_naming_generator
WHERE decode(trim(creative_code),'',NULL,creative_code) IS NOT NULL
    AND _line > 1;
