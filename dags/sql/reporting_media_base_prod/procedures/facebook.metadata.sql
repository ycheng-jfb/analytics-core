-- LAKE_VIEW.FACEBOOK.AD_IDS view has the ad_ids from all the Fivetran tables
create or replace TEMPORARY table _ad_ids AS
SELECT ad_id
FROM LAKE_VIEW.FACEBOOK.AD_IDS;

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.facebook.metadata
 AS
    SELECT
        ACCOUNT_ID,
        ACCOUNT_NAME,
        CAMPAIGN_ID,
        CAMPAIGN_NAME,
        ADGROUP_ID,
        ADGROUP_NAME,
        AD_ID,
        AD_NAME,
        adgroup_is_active_flag,
        ad_is_active_flag,
        PLATFORM,
        POSITION,
        FACEBOOK_OBJECTIVE,
        OPTIMIZATION_GOAL,
        BILLING_EVENT,
        CREATIVE_TYPE,
        POST_TYPE,
        LINK_TO_POST,
        LINK_TO_INSTAGRAM_POST,
        IMAGE_URL,
        CURRENT_TIMESTAMP()::timestamp_ltz AS META_CREATE_DATETIME,
        CURRENT_TIMESTAMP()::timestamp_ltz AS META_UPDATE_DATETIME
    FROM (
        SELECT DISTINCT
            ad.account_id,
            ad.campaign_id,
            ad.adgroup_id,
            id.ad_id,
            account.account_name,
            adgroup.adgroup_name,
            campaign.campaign_name,
            ad.ad_name,
            adgroup.effective_status as adgroup_is_active_flag,
            ad.effective_status as ad_is_active_flag,
            smart.PLATFORM,
            smart.POSITION,
            smart.FACEBOOK_OBJECTIVE,
            smart.OPTIMIZATION_GOAL,
            smart.BILLING_EVENT,
            smart.CREATIVE_TYPE,
            smart.POST_TYPE,
            creative.LINK_TO_POST,
            creative.LINK_TO_INSTAGRAM_POST,
            creative.IMAGE_URL,
            creative.gateway_url
        FROM _ad_ids id

        LEFT JOIN (
            SELECT
                ad_id,
                ACCOUNT_ID,
                CAMPAIGN_ID,
                ADSET_ID as adgroup_id,
                ad_name,
                effective_status,
                ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY meta_update_datetime DESC) AS rn_ad
            FROM lake_view.facebook.AD_METADATA) AS ad
                    ON ad.ad_id = id.ad_id AND rn_ad = 1

        LEFT JOIN (
            SELECT
                account_id,
                account_name,
                ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY meta_update_datetime DESC) AS rn_account
            FROM lake_view.facebook.ACCOUNT_METADATA) AS account
                  ON account.account_id = ad.account_id AND rn_account = 1

        LEFT JOIN (
            SELECT
                campaign_id,
                campaign_name,
                ROW_NUMBER() OVER(PARTITION BY campaign_id ORDER BY meta_update_datetime DESC) AS rn_campaign
            FROM lake_view.facebook.CAMPAIGN_METADATA) AS campaign
                  ON campaign.campaign_id = ad.campaign_id AND rn_campaign = 1

        LEFT JOIN (
            SELECT
                adset_id,
                adset_name AS adgroup_name,
                effective_status,
                ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY meta_update_datetime DESC) AS rn_adgroup
            FROM lake_view.FACEBOOK.ADSET_METADATA) AS adgroup
                  ON adgroup.adset_id = ad.adgroup_id AND rn_adgroup = 1

        LEFT JOIN (
            SELECT
                AD_ID,
                PLATFORM,
                POSITION,
                FACEBOOK_OBJECTIVE,
                OPTIMIZATION_GOAL,
                BILLING_EVENT,
                CREATIVE_TYPE,
                POST_TYPE,
                --LINK_TO_POST,
                --LINK_TO_INSTAGRAM_POST,
                ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY meta_update_datetime DESC) AS rn_smartly_ad
            FROM lake.SMARTLY.MEDIA_SPEND) AS smart
                    ON smart.AD_ID = id.ad_id AND rn_smartly_ad = 1

        LEFT JOIN (
            SELECT
                AD_ID,
                'https://www.facebook.com/'||split_part(effective_object_story_id, '_', 2) link_to_post,
                instagram_permalink_url as link_to_instagram_post,
                COALESCE(IMAGE_URL,thumbnail_url) as image_url,
                parse_json(object_story_spec):link_data:link as gateway_url,
                ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY meta_update_datetime DESC) AS rn_creative
            FROM lake.FACEBOOK.AD_CREATIVE) AS creative
                    ON creative.AD_ID = id.ad_id AND rn_creative = 1
    );
