SET ios_rollout = '2021-04-19';

SET start_date = '2019-06-01';

CREATE OR REPLACE TEMP TABLE brand_list AS
    SELECT * FROM VALUES ('fl'), ('flm'), ('sx'), ('jf'), ('sd'), ('fk'), ('scb'), ('yty') AS t(brand);

CREATE OR REPLACE TEMP TABLE _spend AS
(WITH _total_spend AS (SELECT CASE WHEN lower(st.store_brand_abbr) = 'fl' AND is_scrubs_flag = TRUE THEN 'scb'
                                   WHEN lower(st.store_brand_abbr) = 'fl' AND is_mens_flag = TRUE THEN 'flm'
                                   ELSE lower(st.store_brand_abbr) END AS brand,
                              cast(fmc.media_cost_date AS date)        AS date,
                              fmc.is_mens_flag                         AS is_fl_mens_flag,
                              fmc.is_scrubs_flag,
                              fmc.channel,
                              fmc.subchannel,
                              fmc.vendor,
                              fmc.spend_type,
                              sum(fmc.cost)                            AS cost,
                              sum(fmc.impressions)                     AS impressions,
                              sum(fmc.clicks)                          AS clicks
                       FROM reporting_media_prod.dbo.vw_fact_media_cost fmc
                            JOIN edw_prod.data_model.dim_store st ON st.store_id = fmc.store_id
                       WHERE lower(fmc.targeting) NOT IN ('vip retargeting', 'purchase retargeting', 'free alpha')
                         AND lower(st.store_country) = 'us'
                         AND brand IN (SELECT brand FROM brand_list)
                         AND fmc.media_cost_date >= $start_date
                         AND fmc.media_cost_date <= current_date()
                       GROUP BY brand, date, is_fl_mens_flag, is_scrubs_flag, fmc.channel, fmc.subchannel, fmc.vendor,
                                fmc.spend_type)

 SELECT brand,
        date,
        channel,
        subchannel,
        vendor,
        spend_type,
        CASE WHEN channel = 'influencers' AND subchannel != 'collaborator' THEN 'influencers'
            WHEN channel = 'physical partnerships' OR channel = 'varsity' THEN 'physical partnerships'

            WHEN lower(channel) = 'print' AND lower(subchannel) = 'billboard' THEN 'ooh'
            WHEN lower(channel) = 'print' AND lower(subchannel) = 'direct mail' THEN 'direct mail'

            WHEN channel = 'programmatic' AND lower(subchannel) in ('applovin','imdb','nift','rokt','twitch') THEN subchannel
            WHEN channel = 'programmatic'  THEN 'programmatic>other'

            WHEN channel = 'programmatic-gdn' AND subchannel = 'gdn' THEN 'gdn'
            WHEN channel = 'programmatic-gdn' AND subchannel IN ('demandgen', 'discovery') THEN 'demandgen'

            WHEN channel = 'testing' AND subchannel = 'liveintent' AND vendor = 'liveintent' THEN 'liveintent'
            WHEN channel = 'testing' AND subchannel = 'billboard' AND vendor = 'adquick' THEN 'ooh'

            WHEN lower(channel) = 'tv+streaming' AND lower(subchannel) = 'streaming' THEN  subchannel || '>' || vendor
            WHEN lower(channel) = 'tv+streaming' AND lower(subchannel) = 'amazon stv' THEN  'streaming>amazon'
            WHEN lower(channel) = 'tv+streaming' AND lower(subchannel) = 'tv' THEN subchannel
            WHEN lower(channel) = 'tv+streaming' AND lower(subchannel) ILIKE '%streaming%' AND (lower(subchannel) ILIKE 'roku%' OR lower(vendor) = 'roku') THEN 'streaming>roku'
            WHEN lower(channel) = 'tv+streaming' AND lower(subchannel) ILIKE '%streaming%' AND lower(subchannel) ILIKE 'ttd%' THEN 'streaming>tradedesk'

            WHEN channel = 'fb+ig' AND subchannel IN ('geistm', 'tubescience', 'narrative', 'agencywithin', 'peoplehype', 'sapphire') THEN channel || '>' || subchannel
            --channels to group up
            WHEN channel IN ('affiliate', 'non branded search', 'branded search', 'pinterest', 'youtube', 'tiktok', 'radio/podcast', 'shopping', 'reddit', 'snapchat',  'twitter', 'testing', 'print', 'fb+ig') THEN channel
            --everything else
            ELSE channel || '>' || subchannel END AS final_grouping,

        sum(cost)             AS spend,
        sum(clicks)           AS clicks,
        sum(impressions)      AS impressions
 FROM _total_spend t
 WHERE (CASE WHEN channel IN ('fb+ig', 'youtube') AND spend_type != 'credit' THEN 1 ELSE 0 END) = 0
   -- keep credits for fb+youtube even though we get main data from elsewhere
   AND (CASE WHEN brand IN ('sx', 'jf', 'fl', 'flm') AND channel = 'shopping' AND spend_type != 'fees' THEN 1 ELSE 0 END) = 0
 -- sx, jf, fl break out shopping but fees still come from fmc (above excludes non fees from shopping)
 GROUP BY date, brand, channel, subchannel, vendor, spend_type
 ORDER BY date, channel);

CREATE OR REPLACE TEMP TABLE _fbig AS
(WITH t1 AS (SELECT meta.*,
                    CASE WHEN contains(platform_position, 'reels') THEN 'reels'
                         WHEN contains(platform_position, 'story') OR contains(platform_position, 'stories')
                             THEN 'story'
                         WHEN contains(platform_position, 'feed') THEN 'feed'
                         ELSE 'other' END                      AS consolidated_position,
                    CASE WHEN dbo.ad_type = 'GIF' THEN 'video'
                         WHEN dbo.ad_type = 'DABA' THEN 'static'
                         ELSE lower(dbo.ad_type) END           AS cleaned_ad_type,
                    CASE WHEN contains(lower(ad_name), '_vid') OR contains(lower(ad_name), '.mp4') OR
                              contains(lower(adgroup_name), '_vid') THEN 'video'
                         ELSE 'static' END                     AS ad_type_backfill,
                    ifnull(cleaned_ad_type, ad_type_backfill)  AS adtype,
--                     concat(consolidated_position, ' ', adtype) AS grouping,
                    adtype AS grouping
             FROM reporting_media_prod.facebook.facebook_optimization_dataset_platform meta
                  LEFT JOIN reporting_media_base_prod.dbo.creative_dimensions dbo
                            ON reporting_media_base_prod.dbo.fn_text_to_underscore(meta.ad_name, '_Z-') =
                               dbo.creative_code)
    , t2 AS (SELECT lower(store_brand_abbr)                      AS brand,
                    date,
                    channel,
                    NULL                                         AS subchannel,
                    CASE WHEN lower(funnel_tier) = 'upper funnel' THEN 'fb+ig>upper funnel'
                         WHEN lower(is_dynamic) != 'not dynamic' THEN 'fb+ig>dynamic'
                         ELSE concat(channel, '>', grouping) END AS final_grouping,
                    sum(spend_with_vendor_fees_usd)              AS spend,
                    sum(clicks)                                  AS clicks,
                    sum(impressions)                             AS impressions,
                    sum(pixel_vip_click_7d + pixel_vip_view_1d)  AS pixel_vips
             FROM t1
             WHERE lower(store_brand_abbr) IN ('fl', 'flm')
               AND lower(account_name) NOT ILIKE '%organic%'
               AND lower(country) = 'us'
               AND date >= $start_date
               AND date <= current_date()
             GROUP BY 1, 2, 3, 4, 5
             ORDER BY 1, 2, 3)
    , t3 AS (SELECT lower(store_brand_abbr)                                   AS brand,
                    date,
                    channel,
                    subchannel,
                    CASE WHEN lower(is_dynamic) != 'not dynamic' THEN 'fb+ig>dynamic'
                         WHEN brand = 'flm' AND subchannel = 'paidinfluencers' THEN 'fb+ig>influencers'
                         WHEN brand = 'flm' AND is_advantage_shopping = 'Advantage+ Shopping' THEN 'fb+ig>asc'

                         WHEN brand != 'flm' AND lower(funnel_tier) = 'upper funnel' THEN 'fb+ig>upper funnel'
                         WHEN brand != 'flm' AND lower(vendor) = 'tubescience' THEN 'fb+ig>tubescience'
                         WHEN brand != 'flm' AND lower(vendor) = 'narrative' THEN 'fb+ig>narrative'
                         WHEN brand != 'flm' AND lower(vendor) = 'agencywithin' THEN 'fb+ig>agencywithin'
                         WHEN brand != 'flm' AND lower(vendor) = 'peoplehype' THEN 'fb+ig>peoplehype'
                         WHEN brand != 'flm' AND lower(vendor) = 'geistm' THEN 'fb+ig>geist'
                         WHEN brand != 'flm' AND lower(vendor) = 'sapphire' THEN 'fb+ig>sapphire'
                         WHEN brand != 'flm' AND subchannel = 'paidinfluencers' AND account_name ILIKE '%collab%'
                             THEN 'fb+ig>collaborators'
                         WHEN brand != 'flm' AND (subchannel = 'paidinfluencers') THEN 'fb+ig>influencers'
                         ELSE 'fb+ig' END                                     AS final_grouping,

                    sum(spend_with_vendor_fees_usd)                           AS spend,
                    sum(clicks)                                               AS clicks,
                    sum(impressions)                                          AS impressions,
                    CASE WHEN lower(store_brand_abbr) IN ('flm', 'sx') AND date <= $ios_rollout
                             THEN sum(pixel_subscribe_click_7d + pixel_subscribe_view_1d)
                         ELSE sum(pixel_vip_click_7d + pixel_vip_view_1d) END AS pixel_vips

             FROM reporting_media_prod.facebook.facebook_optimization_dataset_country
             WHERE lower(store_brand_abbr) IN (SELECT brand FROM brand_list WHERE brand NOT IN ('fl', 'flm'))
               AND lower(account_name) NOT IN ('shoedazzle_us_vipret')
               AND lower(account_name) NOT ILIKE '%organic%'
               AND lower(country) = 'us'
               AND date >= $start_date
               AND date <= current_date()
               AND (CASE WHEN lower(store_brand_abbr) = 'fk' AND lower(campaign_name) ILIKE '%afterpay%' THEN 1
                         ELSE 0 END) = 0 -- ignore fk afterpay
             GROUP BY 1, 2, 3, 4, 5
             ORDER BY 1, 2)

 SELECT *
 FROM t2
 UNION ALL
 SELECT *
 FROM t3);

CREATE OR REPLACE TEMP TABLE _yt AS
(SELECT lower(store_brand_abbr)         AS brand,
        date,
        channel,
        subchannel,
        CASE
            --SPLIT PATHS HERE FOR BRANDS WITH PROSPECTING/RETARGETING SPLIT
            WHEN lower(store_brand_abbr) IN ('sx', 'flm') AND member_segment ILIKE '%retarget%'THEN 'youtube>retargeting'
            WHEN lower(store_brand_abbr) IN ('sx', 'flm') THEN 'youtube>prospecting'
            ELSE 'youtube' END          AS final_grouping,
        sum(spend_with_vendor_fees_usd) AS spend,
        sum(clicks)                     AS clicks,
        sum(impressions)                AS impressions
 --         ,sum(iff(date >= '2023-06-06', pixel_vip_30x7x1_click + pixel_vip_30x7x1_view,
--                 pixel_vip_3x3_click + pixel_vip_3x3_view)) AS pixel_vips
 FROM reporting_media_prod.google_ads.google_ads_optimization_dataset
 WHERE channel = 'youtube'
   AND brand IN (SELECT brand FROM brand_list)
   AND lower(country) = 'us'
   AND date >= $start_date
   AND date <= current_date()
 GROUP BY 1, 2, 3, 4, 5
 ORDER BY 1);

CREATE OR REPLACE TEMP TABLE _shopping AS
(SELECT lower(store_brand_abbr)  AS brand,
        date,
        CASE
             WHEN lower(store_brand_abbr) IN ('fl', 'flm') AND DATE < '2023-12-26' THEN 'shopping>grouped'
             WHEN lower(store_brand_abbr) IN ('fl', 'flm') AND campaign_name ILIKE ANY ('%nb%', '%non%') THEN 'shopping>nonbrand'
             WHEN lower(store_brand_abbr) IN ('fl', 'flm') THEN 'shopping>brand'
             WHEN lower(store_brand_abbr) IN ('sx', 'jf') AND campaign_name ILIKE '%performancemax%' THEN 'pmax'
             ELSE 'shopping' END AS final_grouping,
        sum(spend_usd)           AS spend,
        sum(clicks)              AS clicks,
        sum(impressions)         AS impressions
 FROM reporting_media_prod.google_ads.search_optimization_dataset
 WHERE lower(store_brand_abbr) IN ('sx', 'jf', 'fl', 'flm')
   AND lower(country) = 'us'
   AND channel = 'shopping'
   AND date >= $start_date
 GROUP BY brand, date, final_grouping
 ORDER BY brand, date DESC);

CREATE OR REPLACE TEMP TABLE _pixel_vips AS
(SELECT CASE WHEN lower(store_brand_abbr) = 'flm' OR campaign_name ILIKE 'FLMUS%' THEN 'flm'
             ELSE lower(store_brand_abbr) END AS brand,
        date,
        CASE WHEN channel_optimization = 'gdn' THEN 'google display'
             WHEN channel_optimization = 'discovery' THEN 'google discovery'
             WHEN channel_optimization = 'tv+streaming' AND subchannel = 'bpm streaming' THEN 'streaming>blisspointmedia'
             WHEN channel_optimization = 'tv+streaming' AND subchannel = 'tatari streaming' THEN 'streaming>tatari'
             WHEN channel_optimization = 'tv+streaming' THEN 'tv'
             WHEN channel_optimization = 'programmatic' THEN 'ttd programmatic'
            --shopping
             WHEN brand IN ('fl') AND channel_optimization = 'shopping' AND campaign_name ILIKE ANY ('%nb%', '%non%') THEN 'shopping>nonbrand'
             WHEN brand IN ('fl') AND channel_optimization = 'shopping' THEN 'shopping>brand'
             WHEN brand IN ('sx', 'jf') AND channel_optimization = 'shopping' AND campaign_name ILIKE ANY ('%pmax%', '%performancemax%') THEN 'pmax'
            --youtube
             WHEN brand IN ('sx', 'flm', 'fl') AND channel = 'youtube' AND member_segment ILIKE '%retarget%' THEN 'youtube>retargeting'
             WHEN brand IN ('sx', 'flm', 'fl') AND channel = 'youtube' THEN 'youtube>prospecting'
             ELSE channel_optimization END    AS final_grouping,
        sum(ui_optimization_pixel_vip)        AS pixel_vips
 FROM reporting_media_prod.dbo.all_channel_optimization
 WHERE date >= '2021-01-01'
   AND date < cast(current_date() AS date)
   AND country = 'US'
   AND channel_optimization IN
       ('non branded search', 'pinterest', 'programmatic', 'gdn', 'shopping', 'tiktok', 'discovery', 'tv+streaming',
        'twitter', 'youtube', 'snapchat')
   AND (CASE WHEN lower(store_brand_abbr) = 'fk' AND lower(campaign_name) ILIKE '%afterpay%' THEN 1 ELSE 0 END) =
       0 -- ignore fk afterpay
 GROUP BY brand, date, final_grouping);

CREATE OR REPLACE TEMP TABLE _branded_streaming AS
(SELECT brand, date, classification AS final_grouping, spend, impressions, 0 AS clicks
 FROM (SELECT lower(store_brand_abbr)                                                                AS brand,
              date,
              CASE WHEN ad_name ILIKE 'BRAND%' THEN subchannel || '>' || 'brand' ELSE subchannel END AS classification,
              sum(spend)                                                                             AS spend,
              sum(impressions)                                                                       AS impressions
       FROM reporting_media_base_prod.tatari.daily_spend_by_creative
       WHERE brand IN (SELECT brand FROM brand_list)
         AND subchannel <> 'tv'
         AND date >= $start_date
         AND date <= current_date()
       GROUP BY 1, 2, 3
       ORDER BY 1)
 WHERE classification = 'streaming>brand');

CREATE OR REPLACE TEMP TABLE _branded_search AS
(SELECT CASE WHEN lower(storebrand) = 'fabletics' AND (isscrubscustomer = TRUE OR isscrubsgateway = TRUE) THEN 'scb'
             WHEN lower(storebrand) = 'fabletics' AND ismalecustomersessions = '1' THEN 'flm'
             WHEN lower(storebrand) = 'fabletics' THEN 'fl'
             WHEN lower(storebrand) = 'savage x' THEN 'sx'
             WHEN lower(storebrand) = 'fabkids' THEN 'fk'
             WHEN lower(storebrand) = 'justfab' THEN 'jf'
             WHEN lower(storebrand) = 'shoedazzle' THEN 'sd'
             ELSE 'exclude' END    AS brand,
        sessionlocaldate           AS date,
        'branded search excl vips' AS final_grouping,
        sum(sessions)              AS clicks
 FROM reporting_base_prod.shared.session_single_view_media
 WHERE membershipstate != 'VIP'
   AND storecountry = 'US'
   AND channel = 'branded search'
   AND sessionlocaldate >= $start_date
   AND sessionlocaldate <= current_date()
   AND brand IN (SELECT brand FROM brand_list)
 GROUP BY brand, date
 ORDER BY 1);

CREATE OR REPLACE TEMP TABLE _avg_daily_discount AS
(SELECT CASE WHEN lower(st.store_brand_abbr) = 'fl' AND dc.is_scrubs_customer = TRUE THEN 'scb'
             WHEN lower(st.store_brand_abbr) = 'fl' AND dc.gender = 'M' THEN 'flm'
             ELSE lower(st.store_brand_abbr) END AS brand,
        to_date(activation_local_datetime)       AS date,
        'activating_discount_percent'            AS final_grouping,
        avg(order_discount_percent)              AS signal
 FROM edw_prod.data_model.fact_activation fa
      LEFT JOIN edw_prod.data_model.dim_store st ON fa.store_id = st.store_id
      LEFT JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = fa.customer_id
 WHERE lower(st.store_country) = 'us'
   AND date <= current_date()
   AND date >= $start_date
   AND is_retail_vip = FALSE
   AND brand IN (SELECT brand FROM brand_list)
 GROUP BY brand, date);

CREATE OR REPLACE TEMP TABLE _inventory_score AS
    SELECT brand, to_date(date) AS date, concat('pct_top_', item_status, '_skus_oos_7d') as final_grouping, pct_top_skus_oos AS signal
    FROM reporting_media_prod.dbo.inventory_score_aggregates
    UNION ALL
    SELECT brand, to_date(date) AS date, concat('pct_top_', item_status, '_skus_oos_30d') as final_grouping, pct_top_skus_oos_30d AS signal
    FROM reporting_media_prod.dbo.inventory_score_aggregates;

CREATE OR REPLACE TEMP TABLE _influencer_impact_score AS
(SELECT 'fl' AS brand, to_date(date) AS date, 'influencers' AS final_grouping, sum(impact_score) AS impact_score, sum(engagements) AS engagements, sum(impressions) AS impressions
 FROM reporting_media_base_prod.influencers.influencer_impact_score_daily_cleaned_flna
 WHERE date >= $start_date
   AND date <= current_date()
   AND influencer_classification <> 'Collaborator'
 GROUP BY brand, date
 UNION ALL
 SELECT 'flm' AS brand, to_date(date) AS date, 'influencers' AS final_grouping, sum(impact_score) AS impact_score, sum(engagements) AS engagements, sum(impressions) AS impressions
 FROM reporting_media_base_prod.influencers.influencer_impact_score_daily_cleaned_flmna
 WHERE date >= $start_date
   AND date <= current_date()
   AND influencer_classification <> 'Collaborator'
 GROUP BY brand, date
 UNION ALL
 SELECT 'sx' AS brand, to_date(date) AS date, 'influencers' AS final_grouping, sum(impact_score) AS impact_score, sum(engagements) AS engagements, sum(impressions) AS impressions
 FROM reporting_media_base_prod.influencers.influencer_impact_score_daily_cleaned_sxna
 WHERE date >= $start_date
   AND date <= current_date()
   AND publisher_id != '1538076'
 GROUP BY brand, date
 UNION ALL
 SELECT 'jf' AS brand, to_date(date) AS date, 'influencers' AS final_grouping, sum(impact_score) AS impact_score, sum(engagements) AS engagements, sum(impressions) AS impressions
 FROM reporting_media_base_prod.influencers.influencer_impact_score_daily_cleaned_jfna
 WHERE date >= $start_date
   AND date <= current_date()
   AND influencer_classification <> 'Collaborator'
 GROUP BY brand, date
 UNION ALL
 SELECT 'sd' AS brand, to_date(date) AS date, 'influencers' AS final_grouping, sum(impact_score) AS impact_score, sum(engagements) AS engagements, sum(impressions) AS impressions
 FROM reporting_media_base_prod.influencers.influencer_impact_score_daily_cleaned_sdna
 WHERE date >= $start_date
   AND date <= current_date()
   AND influencer_classification <> 'Collaborator'
 GROUP BY brand, date);

CREATE OR REPLACE TEMP TABLE _vip_data AS
(WITH _data  AS (SELECT CASE WHEN lower(store_brand_abbr) = 'fl' AND is_fl_scrubs_customer = TRUE THEN 'scb'
                             WHEN lower(store_brand_abbr) = 'fl' AND is_fl_mens_vip = TRUE THEN 'flm'
                             ELSE lower(store_brand_abbr) END                                                         AS brand,
                        date,
                        sum(CASE WHEN is_fk_free_trial = FALSE THEN total_vips_on_date END)                           AS vip_conversions,
                        sum(CASE WHEN is_fk_free_trial = FALSE THEN vips_from_leads_m1 END)                           AS vip_conversions_m1,
                        sum(CASE WHEN channel = 'affiliate' AND is_fk_free_trial = FALSE
                                     THEN total_vips_on_date END)                                                     AS affiliate,
                        sum(CASE WHEN channel = 'affiliate' AND is_fk_free_trial = FALSE
                                     THEN vips_from_leads_m1 END)                                                     AS affiliate_m1,
                        sum(primary_leads)                                                                            AS leads
                 FROM reporting_media_prod.attribution.cac_by_lead_channel_daily
                 WHERE retail_customer = FALSE
                   AND lower(country) = 'us'
                   AND brand IN (SELECT brand FROM brand_list)
                   AND date >= $start_date
                   AND date <= current_date()
                 GROUP BY brand, date
                 ORDER BY date)
    , _pivot AS (SELECT brand, date, lower(final_grouping) AS final_grouping, signal
                 FROM _data UNPIVOT (signal FOR final_grouping IN (vip_conversions, vip_conversions_m1, affiliate, affiliate_m1, leads)))
 SELECT brand,
        date,
        final_grouping,
        CASE WHEN final_grouping IN ('affiliate', 'affiliate_m1') THEN signal ELSE NULL END AS pixel_vips,
        CASE WHEN final_grouping IN ('affiliate', 'affiliate_m1') THEN NULL ELSE signal END AS signal
 FROM _pivot
 ORDER BY date DESC);

CREATE OR REPLACE TEMP TABLE _rih_posts AS
(SELECT DISTINCT 'sx' AS brand, to_date(datesubmitted) AS date, 'rih_posts' AS final_grouping, 1 AS signal
 FROM lake.creatoriq.campaign_activity
 WHERE campaignid = '289057'
   AND datesubmitted >= $start_date
 ORDER BY 1);

-- CREATE OR REPLACE TEMP TABLE _fk_crosspromo AS
-- (SELECT brand, date, 'crosspromo' AS final_grouping, sum(sends) AS sends, sum(opens) AS opens, sum(clicks) AS clicks
--  FROM (WITH valid_campaign_ids AS (SELECT DISTINCT campaign_id, store_group, max(name) AS max_name
--                                    FROM lake_view.emarsys.email_campaigns_v2
--                                    WHERE NOT contains(lower(name), 'copy')
--                                      AND name ILIKE ANY ('%crosspromo%')
--                                      AND name ILIKE ANY ('%fk%', '%fabkids%')
--                                    GROUP BY campaign_id, store_group)
--        SELECT 'fk'                 AS brand,
--               i.store_group,
--               i.max_name,
--               i.campaign_id,
--               to_date(s.send_date) AS date,
--               ifnull(s.sends, 0)   AS sends,
--               ifnull(o.opens, 0)   AS opens,
--               ifnull(c.clicks, 0)  AS clicks,
--               'emarsys'            AS sourced
--        FROM valid_campaign_ids i
--             LEFT JOIN (SELECT campaign_id,
--                               store_group,
--                               to_date(event_time)        AS send_date,
--                               count(DISTINCT contact_id) AS sends
--                        FROM lake_view.emarsys.email_sends
--                        WHERE campaign_id IN (SELECT DISTINCT campaign_id FROM valid_campaign_ids)
--                        GROUP BY 1, 2, 3) s ON i.campaign_id = s.campaign_id AND i.store_group = s.store_group
--             LEFT JOIN (SELECT campaign_id, store_group, count(DISTINCT contact_id) AS opens
--                        FROM lake_view.emarsys.email_opens
--                        WHERE campaign_id IN (SELECT DISTINCT campaign_id FROM valid_campaign_ids)
--                        GROUP BY 1, 2) o ON i.campaign_id = o.campaign_id AND i.store_group = o.store_group
--             LEFT JOIN (SELECT campaign_id, store_group, count(DISTINCT contact_id) AS clicks
--                        FROM lake_view.emarsys.email_clicks
--                        WHERE campaign_id IN (SELECT DISTINCT campaign_id FROM valid_campaign_ids)
--                        GROUP BY 1, 2) c ON i.campaign_id = c.campaign_id AND i.store_group = c.store_group
--
--        WHERE s.send_date >= $start_date
--        UNION ALL
--        SELECT 'fk'               AS brand,
--               store_name         AS store_group,
--               campaign_name      AS name,
--               c_id               AS campaign_id,
--               to_date(send_date) AS date,
--               sum(sent)          AS sends,
--               sum(opens)         AS opens,
--               sum(clicks)        AS clicks,
--               'sailthru'         AS sourced
--        FROM lake_view.sailthru.campaign_data
--        WHERE campaign_name ILIKE ANY ('%crosspromo%')
--          AND campaign_name ILIKE ANY ('%fk%', '%fabkids%')
--          AND send_date >= $start_date
--        GROUP BY store_group, campaign_name, campaign_id, send_date)
--  GROUP BY brand, date);

-- CREATE OR REPLACE TEMP TABLE _agedleads AS
-- (SELECT brand, date, 'agedlead' AS final_grouping, sum(sends) AS sends, sum(opens) AS opens, sum(clicks) AS clicks
--  FROM (WITH valid_campaign_ids AS (SELECT DISTINCT campaign_id, store_group, max(name) AS campaign_name
--                                    FROM lake_view.emarsys.email_campaigns_v2
--                                    WHERE NOT contains(lower(name), 'copy')
--                                      AND lower(store_group) = 'fabkids_us'
--                                      AND name ILIKE ('%mkt%')
--                                      AND name ILIKE ANY ('%lead%', '%agedlead%')
--                                      AND NOT lower(name) ILIKE ANY ('%free%', '%|_ft|_%') ESCAPE '|'
--                                    GROUP BY campaign_id, store_group)
--        SELECT 'fk'                 AS brand,
--               i.store_group,
--               i.campaign_name,
--               i.campaign_id,
--               to_date(s.send_date) AS date,
--               ifnull(s.sends, 0)   AS sends,
--               ifnull(o.opens, 0)   AS opens,
--               ifnull(c.clicks, 0)  AS clicks,
--               'emarsys'            AS sourced
--        FROM valid_campaign_ids i
--             LEFT JOIN (SELECT campaign_id,
--                               store_group,
--                               to_date(event_time)        AS send_date,
--                               count(DISTINCT contact_id) AS sends
--                        FROM lake_view.emarsys.email_sends
--                        WHERE campaign_id IN (SELECT DISTINCT campaign_id FROM valid_campaign_ids)
--                          AND lower(store_group) = 'fabkids_us'
--                        GROUP BY 1, 2, 3) s ON i.campaign_id = s.campaign_id AND i.store_group = s.store_group
--             LEFT JOIN (SELECT campaign_id, store_group, count(DISTINCT contact_id) AS opens
--                        FROM lake_view.emarsys.email_opens
--                        WHERE campaign_id IN (SELECT DISTINCT campaign_id FROM valid_campaign_ids)
--                          AND lower(store_group) = 'fabkids_us'
--                        GROUP BY 1, 2) o ON i.campaign_id = o.campaign_id AND i.store_group = o.store_group
--             LEFT JOIN (SELECT campaign_id, store_group, count(DISTINCT contact_id) AS clicks
--                        FROM lake_view.emarsys.email_clicks
--                        WHERE campaign_id IN (SELECT DISTINCT campaign_id FROM valid_campaign_ids)
--                          AND lower(store_group) = 'fabkids_us'
--                        GROUP BY 1, 2) c ON i.campaign_id = c.campaign_id AND i.store_group = c.store_group
--        WHERE s.send_date > $start_date
--        UNION ALL
--        SELECT 'fk'               AS brand,
--               store_name         AS store_group,
--               campaign_name      AS name,
--               c_id               AS campaign_id,
--               to_date(send_date) AS date,
--               sum(sent)          AS sends,
--               sum(opens)         AS opens,
--               sum(clicks)        AS clicks,
--               'sailthru'         AS sourced
--        FROM lake_view.sailthru.campaign_data
--        WHERE campaign_name ILIKE ANY ('%mkt%')
--          AND campaign_name ILIKE ANY ('%lead%', '%agedlead%')
--          AND NOT lower(campaign_name) ILIKE ANY ('%free%', '%|_ft|_%') ESCAPE
--                  '|' AND lower(store_group) = 'fabkids us' AND send_date >= $start_date
--        GROUP BY store_group, campaign_name, campaign_id, send_date)
--  GROUP BY brand, date);


CREATE OR REPLACE TEMP TABLE _channels AS
(SELECT brand,
        date,
        final_grouping,
        sum(spend)                                 AS spend_incl_credits,
        sum(iff(spend_type = 'credit', spend, 0))  AS credits,
        sum(iff(spend_type != 'credit', spend, 0)) AS spend,
        sum(clicks)                                AS clicks,
        sum(impressions)                           AS impressions,
        NULL                                       AS pixel_vips
 FROM _spend
 GROUP BY brand, date, final_grouping
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        sum(spend)       AS spend_incl_credits,
        NULL             AS credits,
        sum(spend)       AS spend,
        sum(clicks)      AS clicks,
        sum(impressions) AS impressions,
        NULL             AS pixel_vips
 FROM _yt
 GROUP BY brand, date, final_grouping
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        NULL            AS spend_incl_credits,
        NULL            AS credits,
        NULL            AS spend,
        NULL            AS clicks,
        NULL            AS impressions,
        sum(pixel_vips) AS pixel_vips
 FROM _pixel_vips
 GROUP BY brand, date, final_grouping
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        sum(spend)       AS spend_incl_credits,
        NULL             AS credits,
        sum(spend)       AS spend,
        sum(clicks)      AS clicks,
        sum(impressions) AS impressions,
        NULL             AS pixel_vips
 FROM _shopping
 GROUP BY brand, date, final_grouping
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        sum(spend)       AS spend_incl_credits,
        NULL             AS credits,
        sum(spend)       AS spend,
        sum(clicks)      AS clicks,
        sum(impressions) AS impressions,
        sum(pixel_vips)  AS pixel_vips
 FROM _fbig
 GROUP BY brand, date, final_grouping
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        sum(spend)       AS spend_incl_credits,
        NULL             AS credits,
        sum(spend)       AS spend,
        sum(clicks)      AS clicks,
        sum(impressions) AS impressions,
        NULL             AS pixel_vips
 FROM _branded_streaming
 GROUP BY brand, date, final_grouping);


CREATE OR REPLACE TEMP TABLE _signals AS
(SELECT brand,
        date,
        final_grouping,
        clicks,
        NULL AS pixel_vips,
        NULL AS sends,
        NULL AS opens,
        NULL AS signal,
        NULL AS impact_score,
        NULL AS engagements,
        NULL AS impressions
 FROM _branded_search
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        NULL AS clicks,
        pixel_vips,
        NULL AS sends,
        NULL AS opens,
        signal,
        NULL AS impact_score,
        NULL AS engagements,
        NULL AS impressions
 FROM _vip_data
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        NULL AS clicks,
        NULL AS pixel_vips,
        NULL AS sends,
        NULL AS opens,
        signal,
        NULL AS impact_score,
        NULL AS engagements,
        NULL AS impressions
 FROM _avg_daily_discount
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        NULL AS clicks,
        NULL AS pixel_vips,
        NULL AS sends,
        NULL AS opens,
        signal,
        NULL AS impact_score,
        NULL AS engagements,
        NULL AS impressions
 FROM _inventory_score
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        NULL AS clicks,
        NULL AS pixel_vips,
        NULL AS sends,
        NULL AS opens,
        NULL AS signal,
        impact_score,
        engagements,
        impressions
 FROM _influencer_impact_score
 UNION ALL
 SELECT brand,
        date,
        final_grouping,
        NULL AS clicks,
        NULL AS pixel_vips,
        NULL AS sends,
        NULL AS opens,
        signal,
        NULL AS impact_score,
        NULL AS engagements,
        NULL AS impressions
 FROM _rih_posts
--  UNION ALL
--  SELECT brand, date, final_grouping, clicks, NULL AS pixel_vips, sends, opens, NULL AS signal, NULL AS impact_score, NULL AS engagements, NULL AS impressions
--  FROM _fk_crosspromo
--  UNION ALL
--  SELECT brand, date, final_grouping, clicks, NULL AS pixel_vips, sends, opens, NULL AS signal, NULL AS impact_score, NULL AS engagements, NULL AS impressions
--  FROM _agedleads
 );

-- use role tfg_media_admin;
CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.mmm_raw_data AS
-- CREATE OR REPLACE TRANSIENT TABLE work media mmm_raw_data AS
(WITH _t1 AS (SELECT brand,
                     date,
                     final_grouping,
                     spend_incl_credits,
                     credits,
                     spend,
                     impressions,
                     clicks,
                     pixel_vips,
                     NULL AS sends,
                     NULL AS opens,
                     NULL AS signal,
                     NULL AS impact_score,
                     NULL AS engagements
              FROM _channels
              UNION ALL
              SELECT brand,
                     date,
                     final_grouping,
                     NULL AS spend_incl_credits,
                     NULL AS credits,
                     NULL AS spend,
                     impressions,
                     clicks,
                     pixel_vips,
                     sends,
                     opens,
                     signal,
                     impact_score,
                     engagements
              FROM _signals)
 SELECT brand,
        date,
        final_grouping,
        sum(spend_incl_credits) AS spend_incl_credits,
        sum(credits)            AS credits,
        sum(spend)              AS spend,
        sum(impressions)        AS impressions,
        sum(clicks)             AS clicks,
        sum(pixel_vips)         AS pixel_vips,
        sum(sends)              AS sends,
        sum(opens)              AS opens,
        sum(signal)             AS signal,
        sum(impact_score)       AS impact_score,
        sum(engagements)        AS engagements
 FROM _t1
 GROUP BY brand, date, final_grouping
 ORDER BY brand, date, final_grouping);
