CREATE or replace temp table _fivetran_sync_details as
with connectors_details as(
    select connector_id,
        connector_name,
        case connector_name
            when 'med_tiktok_v1' then 4
            when 'med_snapchat_ad1_v1' then 4
            when 'med_google_ads_v1' then 4
            when 'med_sharepoint_spend_v1' then 4
            when 'med_pinterest_ad1_v1' then 4
            when 'med_twitter_ads_v1' then 4
            when 'med_google_sa_360_v1' then 4
            when 'med_facebook_ads_30min_eu_v1' then 3
            when 'med_facebook_ads_30min_na_v1' then 3
        END AS sla_hours
    from lake_view.fivetran_log.connector c
    where connector_name in ('med_tiktok_v1','med_snapchat_ad1_v1','med_google_ads_v1','med_sharepoint_spend_v1','med_pinterest_ad1_v1',
'med_twitter_ads_v1','med_google_sa_360_v1', 'med_facebook_ads_30min_eu_v1', 'med_facebook_ads_30min_na_v1')
    qualify row_number() over (partition by connector_name order by meta_update_datetime desc) = 1
)
SELECT c.connector_name,
     l.sync_id,
     CASE
           WHEN l.message_event = 'sync_end' AND l.event = 'INFO' AND
                time_stamp::DATE = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)::DATE THEN 'completed'
           WHEN l.message_event = 'sync_end' AND l.event = 'WARNING' AND
                time_stamp::DATE = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)::DATE THEN 'completed_with_warning'
           ELSE 'notcompleted' END                                                  AS sync_status,
     IFF(DATEDIFF(HOUR, time_stamp::TIMESTAMP_NTZ, SYSDATE()) <= sla_hours, 0, 1) AS data_refreshed,
     time_stamp::TIMESTAMP_NTZ AS last_refreshed_at,
     SYSDATE() AS CURRENT_TIME,
    CASE WHEN sync_status = 'completed' THEN 0
    ELSE 1 END AS error_connector
FROM connectors_details c
LEFT OUTER JOIN (
    SELECT connector_id,
        sync_id,
        time_stamp,
        message_event,
        event
    FROM lake_view.fivetran_log.log
    WHERE message_event = 'sync_end'
        QUALIFY row_number() OVER (PARTITION BY connector_id ORDER BY time_stamp DESC) = 1
) l ON c.connector_id = l.connector_id;


set spend_check_date = IFF(
        CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', CURRENT_TIMESTAMP())::DATE > CURRENT_DATE(),
        CURRENT_DATE(),
        DATEADD(DAY, -1, CURRENT_DATE()));


CREATE OR REPLACE TEMP TABLE _fb_account_level_check AS
WITH imp_accounts AS (
    SELECT account_id,
        SUM(coalesce(spend_usd,0)) last_day_spend
    FROM reporting_media_prod.facebook.facebook_optimization_dataset_hourly_by_ad
    WHERE date = DATEADD(DAY, -1, $spend_check_date)
        AND account_id IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
)
,fb_accounts AS (
    SELECT account_id,
        count(1) no_of_records,
        max(meta_update_datetime) last_refreshed_at
    FROM lake_view.facebook.ad_insights_by_hour
    WHERE DATE = $spend_check_date
    GROUP BY 1
    )
SELECT
    'Facebook' vendor,
    i.account_id,
    last_refreshed_at,
    no_of_records
FROM fb_accounts a
    RIGHT JOIN imp_accounts i ON i.account_id = a.account_id;


CREATE OR REPLACE TEMP TABLE _tiktok_account_level_check AS
WITH imp_accounts AS (
    SELECT account_id,
        SUM(coalesce(spend_usd,0)) last_day_spend
    FROM reporting_media_prod.tiktok.tiktok_optimization_dataset
    WHERE date = DATEADD(DAY, -1, $spend_check_date)
        AND account_id IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
)
,tiktok_accounts AS (
    SELECT advertiser_id account_id,
        count(1) no_of_records,
        max(meta_update_datetime) last_refreshed_at
    FROM lake_view.tiktok.daily_spend ds
    WHERE DATE = $spend_check_date
    GROUP BY 1
    )
SELECT
    'Tiktok' vendor,
    i.account_id,
    last_refreshed_at,
    no_of_records
FROM tiktok_accounts a
    RIGHT JOIN imp_accounts i ON i.account_id = a.account_id;


CREATE OR REPLACE TEMP TABLE _snapchat_account_level_check AS
WITH imp_accounts AS (
    SELECT account_id,
        SUM(coalesce(spend_usd,0)) last_day_spend
    FROM reporting_media_prod.snapchat.snapchat_optimization_dataset
    WHERE date = DATEADD(DAY, -1, $spend_check_date)
        AND account_id IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 3
)
,snapchat_accounts AS (
    SELECT ad_account_id account_id,
        count(1) no_of_records,
        max(pd.meta_update_datetime) last_refreshed_at
    FROM lake_view.snapchat.pixel_data_1day pd
    JOIN lake_view.snapchat.ad_metadata amd
      ON pd.ad_id = amd.ad_id
    JOIN lake_view.snapchat.adsquad_metadata asmd
      ON amd.ad_squad_id = asmd.adsquad_id
    JOIN lake_view.snapchat.campaign_metadata cmd
      ON cmd.CAMPAIGN_ID=asmd.campaign_id
    JOIN lake_view.sharepoint.med_account_mapping_media am ON am.source_id = cmd.AD_ACCOUNT_ID
        AND am.reference_column='account_id'
        AND am.source = 'Snapchat'
    JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
    JOIN edw_prod.reference.store_timezone tz on am.store_id = tz.store_id
    WHERE IFF(STORE_REGION='EU',convert_timezone(tz.time_zone,pd.start_time),pd.start_time)::date = $spend_check_date
    GROUP BY 1
    )
SELECT
    'Snapchat' vendor,
    i.account_id,
    last_refreshed_at,
    no_of_records
FROM snapchat_accounts a
         RIGHT JOIN imp_accounts i ON i.account_id = a.account_id;


CREATE OR REPLACE TEMP TABLE _pinterest_account_level_check AS
WITH imp_accounts AS (
    SELECT account_id,
        SUM(coalesce(spend_usd,0)) last_day_spend
    FROM reporting_media_prod.pinterest.pinterest_optimization_dataset
    WHERE date = DATEADD(DAY, -1, $spend_check_date)
        AND account_id IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 2
)
,pinterest_accounts AS (
    SELECT
        advertiser_id account_id,
        count(1) no_of_records,
        max(meta_update_datetime) last_refreshed_at
    FROM lake_view.pinterest.spend_ad1
    WHERE date::DATE = $spend_check_date
    GROUP BY 1
    )
SELECT
    'Pinterest' vendor,
    i.account_id,
    last_refreshed_at,
    no_of_records
FROM pinterest_accounts a
         RIGHT JOIN imp_accounts i ON i.account_id = a.account_id;


CREATE OR REPLACE TEMP TABLE _google_ads_account_level_check AS
WITH imp_accounts AS (
    SELECT account_id,
        SUM(coalesce(spend_usd,0)) last_day_spend
    FROM reporting_media_prod.google_ads.google_ads_optimization_dataset
    WHERE date = DATEADD(DAY, -1, $spend_check_date)
        AND account_id IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
)
,google_ads_accounts AS (
    SELECT
        external_customer_id account_id,
        count(1) no_of_records,
        max(meta_update_datetime) last_refreshed_at
    FROM lake_view.google_ads.ad_spend
    WHERE DATE = $spend_check_date
    GROUP BY 1
    )
SELECT
    'Google Ads' vendor,
    i.account_id,
    last_refreshed_at,
    no_of_records
FROM google_ads_accounts a
         RIGHT JOIN imp_accounts i ON i.account_id = a.account_id;

CREATE OR REPLACE TEMP TABLE _twitter_account_level_refresh_check AS
WITH imp_accounts AS (
    SELECT account_id,
        SUM(coalesce(spend_usd,0)) last_day_spend
    FROM reporting_media_prod.twitter.twitter_optimization_dataset
    WHERE date = DATEADD(DAY, -1, $spend_check_date)
        AND account_id IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 2
)
,twitter_accounts AS (
    SELECT
        account_id,
        count(1) no_of_records,
        max(meta_update_datetime) last_refreshed_at
    FROM lake_view.twitter.twitter_spend_and_conversion_metrics_by_promoted_tweet_id
    WHERE DATE = $spend_check_date
    GROUP BY 1
    )
SELECT
    'Twitter' vendor,
    i.account_id,
    last_refreshed_at,
    no_of_records
FROM twitter_accounts a
         RIGHT JOIN imp_accounts i ON i.account_id = a.account_id;


CREATE OR REPLACE TEMP TABLE _tatari_refresh_check AS
SELECT 'Tatari' vendor,
     sum(no_of_records) AS no_of_records,
     MAX(last_refreshed_at) AS last_refreshed_at
from (
    SELECT
         COUNT(1)                  no_of_records,
         MAX(meta_update_datetime) last_refreshed_at
    FROM lake.tatari.linear_spend_and_impressions ts
    WHERE spot_datetime::DATE = $spend_check_date
    UNION ALL
    SELECT
         COUNT(1)                  no_of_records,
         MAX(meta_update_datetime) last_refreshed_at
    FROM lake.tatari.streaming_spend_and_impression ts
    WHERE date::DATE = $spend_check_date
);


CREATE OR REPLACE TEMP TABLE _media_spend_variance AS
SELECT vendor,
       account_id::VARCHAR account_id,
       'Not Refreshed Today' AS sync_status,
       last_refreshed_at::TIMESTAMP_NTZ last_refreshed_at
FROM _fb_account_level_check
WHERE no_of_records IS NULL
UNION ALL
SELECT vendor,
       account_id::VARCHAR account_id,
       'Not Refreshed Today' AS sync_status,
       last_refreshed_at::TIMESTAMP_NTZ last_refreshed_at
FROM _google_ads_account_level_check
WHERE no_of_records IS NULL
UNION ALL
SELECT vendor,
       account_id,
       'Not Refreshed Today' AS sync_status,
       last_refreshed_at::TIMESTAMP_NTZ last_refreshed_at
FROM _pinterest_account_level_check
WHERE no_of_records IS NULL
UNION ALL
SELECT vendor,
       account_id::VARCHAR account_id,
       'Not Refreshed Today' AS sync_status,
       last_refreshed_at::TIMESTAMP_NTZ last_refreshed_at
FROM _snapchat_account_level_check
WHERE no_of_records IS NULL
UNION ALL
SELECT vendor,
       account_id::VARCHAR account_id,
       'Not Refreshed Today' AS sync_status,
       last_refreshed_at::TIMESTAMP_NTZ last_refreshed_at
FROM _tiktok_account_level_check
WHERE no_of_records IS NULL
UNION ALL
SELECT vendor,
       account_id,
       'Not Refreshed Today' AS sync_status,
       last_refreshed_at::TIMESTAMP_NTZ last_refreshed_at
FROM _twitter_account_level_refresh_check
WHERE no_of_records IS NULL
UNION ALL
SELECT vendor,
       NULL AS account_id,
       'Not Refreshed Today' AS sync_status,
       last_refreshed_at::TIMESTAMP_NTZ last_refreshed_at
FROM _tatari_refresh_check
WHERE no_of_records IS NULL
UNION ALL
SELECT connector_name connector_or_vendor,
       NULL AS account_id,
       sync_status,
       last_refreshed_at::TIMESTAMP_NTZ last_refreshed_at
FROM _fivetran_sync_details
WHERE error_connector=1;

SET report_version = IFF(hour(current_timestamp()) between 6 and 21, 'NA', 'EU');

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.dbo.media_spend_variance AS
SELECT v.*,
       ds.store_id,
       ds.store_brand_abbr || ds.store_country store,
       ds.store_region                         region
FROM _media_spend_variance v
         LEFT JOIN lake_view.sharepoint.med_account_mapping_media a
                   ON a.source_id = v.account_id AND include_in_cpa_report = 1
         LEFT JOIN edw_prod.data_model.dim_store ds ON ds.store_id = a.store_id
WHERE region IS NULL
   OR region = $report_version;
