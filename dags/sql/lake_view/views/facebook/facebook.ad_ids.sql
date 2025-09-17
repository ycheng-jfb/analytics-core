CREATE OR REPLACE VIEW LAKE_VIEW.FACEBOOK.AD_IDS AS
SELECT DISTINCT ad_id
FROM(
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_30min_na_v1.ad_insights_by_hour
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_30min_eu_v1.ad_insights_by_hour
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_30min_sxf_v1.ad_insights_by_hour
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_8hr_v1.actions_by_hour_1_d
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_8hr_v1.actions_by_hour_7_d
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_2hr_v1.ad_insights_by_country
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_2hr_v1.actions_by_country_1_d
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_2hr_v1.actions_by_country_7_d
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_3hr_v1.ad_insights_by_platform
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_3hr_v1.actions_by_platform_1_d
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_3hr_v1.actions_by_platform_7_d
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_8hr_v1.ad_insights_by_age_gender
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_8hr_v1.actions_by_age_gender_1_d
    UNION ALL
    SELECT ad_id
    FROM lake_fivetran.med_facebook_ads_8hr_v1.actions_by_age_gender_7_d
    UNION ALL
    SELECT ad_id::varchar
    FROM lake.facebook.ad_creative
);
