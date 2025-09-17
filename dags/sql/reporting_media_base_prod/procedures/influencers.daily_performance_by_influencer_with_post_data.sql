USE SCHEMA reporting_media_base_prod.public;

-- notes: this script has (and needs) several layers of bounds to try and stay conservative in our estimates for
-- influencers. It discards periods that are outliers several different times, starting with comparing the period
-- to the total upper bound, then to the influencer specific upper bound

SET update_datetime = CURRENT_TIMESTAMP();

SET lookback_date = '2022-01-01';

SET ma_window = 59;

SET halflife = 20;

SET span = 60;

-- CREATE OR REPLACE TEMPORARY TABLE _daily_mmm_pct_of_base AS
--     SELECT brand,
--            to_date(date)                                                          AS date,
--            sum(attributed_vips_with_base)                                         AS attributed_vips_to_base,
--            avg("vip conversions exlc affiliate")                                  AS total_vips,
--            CASE WHEN total_vips > 0 THEN attributed_vips_to_base / total_vips END AS pct_of_vips_to_base
--     FROM reporting_media_base_prod.attribution.mmm_outputs
--     WHERE feature_type = 'base'
--       AND modeled_metric != 'not modeled'
--       AND date <= current_date()
--     GROUP BY brand, date, feature_type
--     ORDER BY date DESC;

-- create temp mapping table
CREATE OR REPLACE TEMPORARY TABLE _mapping AS
    SELECT DISTINCT *
    FROM lake_view.sharepoint.med_influencer_mapping
    WHERE media_partner_id IS NOT NULL
      AND ciq_publisher_id IS NOT NULL;


CREATE OR REPLACE TEMPORARY TABLE _collaborators AS
    SELECT DISTINCT LOWER(business_unit_abbr) AS brand, influencer_name, media_partner_id AS mpid
    FROM reporting_media_base_prod.influencers.daily_spend
    WHERE channel = 'Collaborator'
      AND media_partner_id IS NOT NULL;


CREATE OR REPLACE TEMPORARY TABLE _in_hdyh AS
    SELECT DISTINCT UPPER(SPLIT_PART(m.media_partner_id, '|', 0))               AS mpid,
                    TO_DATE(timestamp)                                          AS date,
                    c.hdyh,
                    store_group_label,
                    country_code,
                    store_group_id,
                    CASE WHEN sub_brand = 'flw' THEN 'fl'
                         WHEN sub_brand = 'flm' THEN 'flm'
                         WHEN sub_brand = 'scrubs' THEN 'scb'
                         WHEN sub_brand = 'yitty' THEN 'yty'
                         WHEN store_group_label ILIKE 'Savage X%' THEN 'sx' END AS brand,
                    TRUE                                                        AS in_hdyh
    FROM reporting_media_base_prod.influencers.hdyh_answers_historical_new c
         LEFT JOIN reporting_media_base_prod.influencers.influencer_historical_mpid_hdyh_mapping m
                   ON LOWER(m.hdyh) = LOWER(c.hdyh)
    WHERE mpid IS NOT NULL
      AND brand IS NOT NULL
      AND country_code = 'US';


CREATE OR REPLACE TEMPORARY TABLE _vips AS
(WITH _daily_performance AS (SELECT business_unit,
                                    LOWER(store_brand_abbr)                                                 AS brand,
                                    region,
                                    media_partner_id                                                        AS mpid,
                                    channel_tier,
                                    date                                                                    AS lead_date,
                                    SUM(upfront_cost)                                                       AS upfront_cost,
                                    SUM(organic_clicks)                                                     AS organic_clicks,

                                    SUM(hdyh_leads_raw)                                                     AS hdyh_leads_raw,
                                    SUM(click_through_leads)                                                AS click_leads_raw,
                                    SUM(click_through_leads + hdyh_leads_raw)                               AS total_leads_raw,


                                    SUM(hdyh_vips_raw)                                                      AS hdyh_vips_raw,
                                    SUM(click_through_vips_from_leads_30d)                                  AS click_vips_from_leads_30d,
                                    SUM(click_through_vips_same_session_not_from_lead_pool)                 AS click_vips_same_session_not_from_lead_pool,
                                    SUM(click_through_vips_from_leads_30d +
                                        click_through_vips_same_session_not_from_lead_pool)                 AS click_vips_raw,
                                    SUM(click_through_vips_from_leads_30d +
                                        click_through_vips_same_session_not_from_lead_pool +
                                        hdyh_vips_raw)                                                      AS total_vips_raw,

                                    SUM(fbig_spend)                                                         AS facebook_spend,
                                    SUM(fbig_pixel_vips)                                                    AS facebook_pixel_vips,
                                    SUM(youtube_spend)                                                      AS youtube_spend,
                                    SUM(youtube_pixel_vips)                                                 AS youtube_pixel_vips,
                                    SUM(pinterest_spend)                                                    AS pinterest_spend,
                                    SUM(pinterest_pixel_vips)                                               AS pinterest_pixel_vips,
                                    SUM(snapchat_spend)                                                     AS snapchat_spend,
                                    SUM(snapchat_pixel_vips)                                                AS snapchat_pixel_vips,
                                    SUM(tiktok_spend)                                                       AS tiktok_spend,
                                    SUM(tiktok_pixel_vips)                                                  AS tiktok_pixel_vips
                             FROM reporting_media_prod.influencers.daily_performance_by_influencer
                             WHERE country = 'US'
                             GROUP BY business_unit, brand, region, mpid, channel_tier, lead_date)

 SELECT v.*,
        s.seasonal_effects_pct_of_total,
        hdyh_leads_raw * (1 - ZEROIFNULL(GREATEST(0, s.seasonal_effects_pct_of_total))) AS hdyh_leads_adjusted,
        hdyh_vips_raw * (1 - ZEROIFNULL(GREATEST(0, s.seasonal_effects_pct_of_total)))  AS hdyh_vips_adjusted
 FROM _daily_performance v
      LEFT JOIN reporting_media_base_prod.influencers.hdyh_seasonal_effects s
                ON v.lead_date = s.date AND v.brand = s.brand);


-- join influencer data with mapping to bring in media partner ID exclude if they are inactive or overwritten to be excluded
CREATE OR REPLACE TEMPORARY TABLE _post_data AS


    WITH _net_weights_orig AS (SELECT brand,
                                      socialnetwork,
                                      vips_per_socialnetwork_scaled                               AS socialnetwork_weight,
                                      AVG(socialnetwork_weight) OVER (PARTITION BY socialnetwork) AS socialnetwork_weight_backfill,
                                      MIN(socialnetwork_weight) OVER ()                           AS socialnetwork_weight_min
                               FROM reporting_media_base_prod.influencers.impact_score_socialnetwork_weights
                               WHERE last_updated = (SELECT MAX(last_updated)
                                                     FROM reporting_media_base_prod.influencers.impact_score_socialnetwork_weights)),
         _net_weights      AS (SELECT brand,
                                      socialnetwork,
                                      COALESCE(socialnetwork_weight, socialnetwork_weight_backfill,
                                               socialnetwork_weight_min) AS socialnetwork_weight
                               FROM _net_weights_orig),
         _metric_weights   AS (SELECT *
                               FROM reporting_media_base_prod.influencers.impact_score_metric_weights
                               WHERE last_updated = (SELECT MAX(last_updated)
                                                     FROM reporting_media_base_prod.influencers.impact_score_metric_weights))
    SELECT TO_VARCHAR(d.media_partner_id)              AS media_partner_id,
           m.*,
           i.comments_weight,
           i.impressions_weight,
           i.likes_weight,
           i.reach_weight,
           i.replies_weight,
           i.saves_weight,
           i.shares_weight,
           i.tapsback_weight,
           i.views_weight,
           n.socialnetwork_weight,
           IFNULL(modeled_inc_comments * comments_weight, 0) + IFNULL(modeled_inc_impressions * impressions_weight, 0) +
           IFNULL(modeled_inc_likes * likes_weight, 0) + IFNULL(modeled_inc_reach * reach_weight, 0) +
           IFNULL(modeled_inc_replies * replies_weight, 0) + IFNULL(modeled_inc_saves * saves_weight, 0) +
           IFNULL(modeled_inc_shares * shares_weight, 0) + IFNULL(modeled_inc_tapsback * tapsback_weight, 0) +
           IFNULL(modeled_inc_views * views_weight, 0) AS impact_weighted_sum,
           socialnetwork_weight * impact_weighted_sum  AS modeled_inc_impact_score,
           IFF(m.socialnetwork = 'youtube', 1, 0)      AS youtube,
           IFF(m.socialnetwork = 'tiktok', 1, 0)       AS tiktok,
           IFF(m.socialnetwork = 'instagram', 1, 0)    AS instagram,
           IFF(m.socialnetwork = 'facebook', 1, 0)     AS facebook,
           IFF(m.socialnetwork = 'pinterest', 1, 0)    AS pinterest,
           IFF(m.socialnetwork = 'twitter', 1, 0)      AS twitter,
           IFF(m.socialnetwork = 'twitch', 1, 0)       AS twitch
    FROM reporting_media_base_prod.influencers.campaign_activity_modeled m
         LEFT JOIN _mapping d ON d.ciq_publisher_id = m.publisherid AND m.brand = d.store_name_abbr
         LEFT JOIN _metric_weights i ON i.brand = m.brand
         LEFT JOIN _net_weights n ON n.socialnetwork = m.socialnetwork
    --dedupe instagram stories so that we only keep data from the first frame??
--     WHERE (posttype != 'story' OR ig_story_frame_sequence=1)
;



CREATE OR REPLACE TEMPORARY TABLE _combined AS
    WITH RECURSIVE
        daterange       AS (SELECT DATE('2019-01-01') AS date -- Start date
                            UNION ALL
                            SELECT DATEADD(DAY, 1, date)
                            FROM daterange
                            WHERE date <= CURRENT_DATE()),
        _dates          AS (SELECT date FROM daterange),
        _combos         AS (SELECT date, brand, mpid, influencer_cleaned_name
                            FROM (SELECT DISTINCT date FROM _dates) d
                                 JOIN (SELECT DISTINCT store_name_abbr  AS brand,
                                                       media_partner_id AS mpid,
                                                       influencer_cleaned_name FROM _mapping) i
                            ORDER BY date DESC, mpid),
        _impact         AS (SELECT brand,
                                   date,
                                   media_partner_id,
                                   MIN(authenticated)            AS authenticated,
                                   SUM(modeled_inc_impact_score) AS impact_score_orig,
                                   SUM(modeled_inc_impressions)  AS impressions,
                                   SUM(modeled_inc_engagements)  AS engagements,
                                   SUM(modeled_inc_likes)        AS likes,
                                   SUM(modeled_inc_comments)     AS comments,
                                   SUM(modeled_inc_shares)       AS shares,
                                   SUM(modeled_inc_saves)        AS saves,
                                   SUM(modeled_inc_views)        AS views,
                                   SUM(modeled_inc_replies)      AS replies,
                                   SUM(modeled_inc_reach)        AS reach,
                                   SUM(modeled_inc_tapsback)     AS tapsback,
                                   SUM(modeled_inc_earnedmedia)  AS earnedmedia,
                                   MAX(youtube)                  AS youtube_bool,
                                   MAX(instagram)                AS instagram_bool,
                                   MAX(tiktok)                   AS tiktok_bool,
                                   MAX(pinterest)                AS pinterest_bool,
                                   MAX(twitter)                  AS twitter_bool,
                                   MAX(twitch)                   AS twitch_bool,
                                   MAX(facebook)                 AS facebook_bool
                            FROM _post_data
                            GROUP BY brand, date, media_partner_id),
        _impact_cleaned AS (SELECT *,
                                   AVG(impact_score_orig) OVER (PARTITION BY brand, media_partner_id)    AS impact_score_mean,
                                   STDDEV(impact_score_orig) OVER (PARTITION BY brand, media_partner_id) AS impact_score_std,
                                   IFF((impact_score_orig >= (impact_score_mean + 3 * impact_score_std)) OR
                                       (impact_score_orig <= (impact_score_mean - 3 * impact_score_std)),
                                       impact_score_mean, impact_score_orig)                             AS impact_score
                            FROM _impact)
    SELECT c.*,
           channel_tier,
           IFNULL(v.hdyh_leads_raw, 0)                      AS hdyh_leads_raw,
           IFNULL(v.hdyh_vips_raw, 0)                       AS hdyh_vips_raw,
           seasonal_effects_pct_of_total,
           IFNULL(v.hdyh_leads_adjusted, 0)                 AS hdyh_leads_adj,
           IFNULL(v.click_leads_raw, 0)                     AS click_leads,
           IFNULL(v.hdyh_vips_adjusted, 0)                  AS hdyh_vips_adj,
           v.click_vips_same_session_not_from_lead_pool,
           v.click_vips_from_leads_30d,
           i.authenticated,
           i.impact_score_orig,
           i.impact_score,
           i.impact_score_mean,
           i.impact_score_std,
           IFF(i.impressions < 0, 0, i.impressions)         AS impressions,
           IFF(i.engagements < 0, 0, i.engagements)         AS engagements,
           IFF(i.likes < 0, 0, i.likes)                     AS likes,
           IFF(i.comments < 0, 0, i.comments)               AS comments,
           IFF(i.shares < 0, 0, i.shares)                   AS shares,
           IFF(i.saves < 0, 0, i.saves)                     AS saves,
           IFF(i.views < 0, 0, i.views)                     AS views,
           IFF(i.replies < 0, 0, i.replies)                 AS replies,
           IFF(i.reach < 0, 0, i.reach)                     AS reach,
           IFF(i.tapsback < 0, 0, i.tapsback)               AS tapsback,
           IFF(i.earnedmedia < 0, 0, i.earnedmedia)         AS earnedmedia,
           h.in_hdyh                                        AS in_hdyh_historical,
           COALESCE(in_hdyh, IFF(hdyh_leads_raw > 0, 1, 0)) AS final_in_hdyh,
--            m.attributed_vips_to_base,
--            m.total_vips,
--            m.pct_of_vips_to_base,
           youtube_bool,
           instagram_bool,
           tiktok_bool,
           pinterest_bool,
           twitter_bool,
           twitch_bool,
           facebook_bool
    FROM _combos c
         LEFT JOIN _vips v ON v.lead_date = c.date AND LOWER(v.mpid) = LOWER(c.mpid) AND v.brand = c.brand
         LEFT JOIN _impact_cleaned i
                   ON i.date = c.date AND LOWER(i.media_partner_id) = LOWER(c.mpid) AND i.brand = c.brand
         LEFT JOIN _in_hdyh h ON LOWER(h.mpid) = LOWER(c.mpid) AND h.date = c.date AND c.brand = h.brand
--          LEFT JOIN _daily_mmm_pct_of_base m ON m.brand = c.brand AND m.date = c.date
    WHERE (LOWER(c.mpid) IN (SELECT DISTINCT LOWER(media_partner_id) FROM _post_data) OR
           LOWER(c.mpid) IN (SELECT DISTINCT LOWER(mpid) FROM _vips))
      AND c.mpid NOT IN (SELECT DISTINCT mpid FROM _collaborators)
    ORDER BY c.date DESC;


CREATE OR REPLACE TEMPORARY TABLE _periods AS
(WITH _t1 AS (SELECT *,
                     --period starts if today in hdyh but yesterday not in hdyh
                     CASE WHEN final_in_hdyh = 1 AND
                               LAG(final_in_hdyh) OVER (PARTITION BY mpid, brand ORDER BY date) = 0 THEN 1
                          ELSE 0 END                                                AS first_day_of_calibration,
                     -- period ends if today in hdyh and tommorrow not in hdyh
                     CASE WHEN final_in_hdyh = 1 AND
                               LEAD(final_in_hdyh) OVER (PARTITION BY mpid, brand ORDER BY date) = 0 THEN 1
                          ELSE 0 END                                                AS last_day_of_calibration,
                     CASE WHEN first_day_of_calibration = 1 THEN date ELSE NULL END AS period_start,
                     CASE WHEN last_day_of_calibration = 1 THEN date ELSE NULL END  AS period_end
              FROM _combined),
      _t2 AS (SELECT *,
                     CASE WHEN final_in_hdyh = 1 THEN LAST_VALUE(period_start IGNORE NULLS)
                                                                 OVER ( PARTITION BY brand, mpid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) END      AS period_start_filled,
                     CASE WHEN final_in_hdyh = 1 THEN LAST_VALUE(period_end IGNORE NULLS)
                                                                 OVER ( PARTITION BY brand, mpid ORDER BY date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) END AS period_end_filled
              FROM _t1
              ORDER BY date)
 SELECT *
 FROM _t2);


--sum up calibration periods
CREATE OR REPLACE TEMP TABLE _calibrations AS
(SELECT brand,
        mpid,
        period_start_filled,
        period_end_filled,
        MIN(authenticated)                                                                                 AS period_authenticated,
        COUNT(date)                                                                                        AS days_within_period,
        SUM(hdyh_leads_adj)                                                                                AS period_hdyh_leads,
        SUM(hdyh_vips_adj)                                                                                 AS period_hdyh_vips,
        SUM(impact_score_orig)                                                                             AS period_impact_score_orig,
        SUM(impact_score)                                                                                  AS period_impact_score,
        SUM(impressions)                                                                                   AS period_impressions,
        SUM(engagements)                                                                                   AS period_engagements,
        SUM(likes)                                                                                         AS period_likes,
        SUM(comments)                                                                                      AS period_comments,
        SUM(shares)                                                                                        AS period_shares,
        SUM(saves)                                                                                         AS period_saves,
        SUM(views)                                                                                         AS period_views,
        SUM(replies)                                                                                       AS period_replies,
        SUM(reach)                                                                                         AS period_reach,
        SUM(tapsback)                                                                                      AS period_tapsback,
        SUM(earnedmedia)                                                                                   AS period_earnedmedia,
        MAX(youtube_bool)                                                                                  AS period_youtube_bool,
        MAX(instagram_bool)                                                                                AS period_instagram_bool,
        MAX(tiktok_bool)                                                                                   AS period_tiktok_bool,
        MAX(pinterest_bool)                                                                                AS period_pinterest_bool,
        MAX(twitter_bool)                                                                                  AS period_twitter_bool,
        MAX(twitch_bool)                                                                                   AS period_twitch_bool,
        MAX(facebook_bool)                                                                                 AS period_facebook_bool,
        period_hdyh_leads / NULLIFZERO(period_impact_score)                                                AS leads_per_impact_rate,
        period_hdyh_vips / NULLIFZERO(period_impact_score)                                                 AS vips_per_impact_rate,
        IFF(period_hdyh_leads >= (ZEROIFNULL(period_impressions) + ZEROIFNULL(period_views)), TRUE,
            FALSE)                                                                                         AS discard_period_on_too_few_impressions_for_leads
 FROM _periods
 WHERE final_in_hdyh = 1
 GROUP BY brand, mpid, period_start_filled, period_end_filled);


--logic to discard outlier periods
CREATE OR REPLACE TEMP TABLE _valid_calibrations AS
(WITH _totals                     AS (SELECT brand,
                                             MEDIAN(leads_per_impact_rate) AS total_leads_per_impact_rate_mean,
                                             STDDEV(leads_per_impact_rate) AS total_leads_per_impact_rate_std,
                                             MEDIAN(vips_per_impact_rate)  AS total_vips_per_impact_rate_mean,
                                             STDDEV(vips_per_impact_rate)  AS total_vips_per_impact_rate_std
                                      FROM _calibrations
                                      WHERE discard_period_on_too_few_impressions_for_leads = FALSE
                                        AND period_start_filled >= $lookback_date
                                      GROUP BY brand)
    , _discard_on_total_threshold AS (SELECT c.*,
                                             total_leads_per_impact_rate_mean,
                                             total_leads_per_impact_rate_std,
                                             total_vips_per_impact_rate_mean,
                                             total_vips_per_impact_rate_std,
                                             IFF(leads_per_impact_rate >=
                                                 (total_leads_per_impact_rate_mean + 1 * total_leads_per_impact_rate_std) OR
                                                 leads_per_impact_rate <=
                                                 (total_leads_per_impact_rate_mean - 1 * total_leads_per_impact_rate_std) OR
                                                 vips_per_impact_rate >=
                                                 (total_vips_per_impact_rate_mean + 1 * total_vips_per_impact_rate_std) OR
                                                 vips_per_impact_rate <=
                                                 (total_vips_per_impact_rate_mean - 1 * total_vips_per_impact_rate_std),
                                                 TRUE, FALSE) AS discard_period_on_total_threshold
                                      FROM _calibrations c
                                           LEFT JOIN _totals t ON t.brand = c.brand)
    , _infl_totals                AS (SELECT brand,
                                             mpid,
                                             MEDIAN(leads_per_impact_rate) AS leads_per_impact_rate_mean,
                                             STDDEV(leads_per_impact_rate) AS leads_per_impact_rate_std,
                                             MEDIAN(vips_per_impact_rate)  AS vips_per_impact_rate_mean,
                                             STDDEV(vips_per_impact_rate)  AS vips_per_impact_rate_std
                                      FROM _discard_on_total_threshold
                                      WHERE discard_period_on_too_few_impressions_for_leads = FALSE
                                        AND discard_period_on_total_threshold = FALSE
                                        AND period_start_filled >= $lookback_date
                                      GROUP BY brand, mpid)
    , _final_calibrations         AS (SELECT c.*,
                                             leads_per_impact_rate_mean,
                                             leads_per_impact_rate_std,
                                             vips_per_impact_rate_mean,
                                             vips_per_impact_rate_std,
                                             IFF(leads_per_impact_rate >=
                                                 (leads_per_impact_rate_mean + 1 * leads_per_impact_rate_std) OR
                                                 leads_per_impact_rate <=
                                                 (leads_per_impact_rate_mean - 3 * leads_per_impact_rate_std) OR
                                                 vips_per_impact_rate >=
                                                 (vips_per_impact_rate_mean + 1 * vips_per_impact_rate_std) OR
                                                 vips_per_impact_rate <=
                                                 (vips_per_impact_rate_mean - 3 * vips_per_impact_rate_std), TRUE,
                                                 FALSE) AS discard_period_on_infl_threshold
                                      FROM _discard_on_total_threshold c
                                           LEFT JOIN _infl_totals t ON c.brand = t.brand AND c.mpid = t.mpid)
    , _final                      AS (SELECT p.*,
                                             days_within_period,
                                             period_hdyh_leads,
                                             period_hdyh_vips,
                                             period_impact_score_orig,
                                             period_impact_score,
                                             period_authenticated,
                                             --metrics
                                             period_impressions,
                                             period_engagements,
                                             period_likes,
                                             period_comments,
                                             period_shares,
                                             period_saves,
                                             period_views,
                                             period_replies,
                                             period_reach,
                                             period_tapsback,
                                             period_earnedmedia,
                                             --socialnetworks
                                             period_youtube_bool,
                                             period_pinterest_bool,
                                             period_instagram_bool,
                                             period_twitter_bool,
                                             period_tiktok_bool,
                                             period_twitch_bool,
                                             period_facebook_bool,
                                             --calculations
                                             leads_per_impact_rate,
                                             total_leads_per_impact_rate_mean,
                                             total_leads_per_impact_rate_std,
                                             leads_per_impact_rate_mean,
                                             leads_per_impact_rate_std,
                                             vips_per_impact_rate,
                                             total_vips_per_impact_rate_mean,
                                             total_vips_per_impact_rate_std,
                                             vips_per_impact_rate_mean,
                                             vips_per_impact_rate_std,
                                             discard_period_on_too_few_impressions_for_leads,
                                             discard_period_on_total_threshold,
                                             discard_period_on_infl_threshold,
                                             IFF(discard_period_on_too_few_impressions_for_leads OR
                                                 discard_period_on_total_threshold OR discard_period_on_infl_threshold,
                                                 FALSE, IFF(final_in_hdyh = TRUE, TRUE, FALSE)) AS valid_period,
                                             IFF(valid_period, leads_per_impact_rate, NULL)     AS leads_per_impact_rate_valid,
                                             IFF(valid_period, vips_per_impact_rate, NULL)      AS vips_per_impact_rate_valid
                                      FROM _periods p
                                           LEFT JOIN _final_calibrations c ON c.brand = p.brand AND c.mpid = p.mpid AND
                                                                              c.period_start_filled =
                                                                              p.period_start_filled AND
                                                                              c.period_end_filled = p.period_end_filled
                                      ORDER BY date)
 SELECT *
 FROM _final);



--this next section takes the exponential moving average of the calibrations
--it first calculates alpha based on the set span or halflife of the calibrations(defined above)
--then it calculates the weight for each day
--then it multiplies the weight by the conversion rate for each day
--then it sums those (numerator) and the weights (denominator) to divide them
CREATE OR REPLACE TEMP TABLE _averaged_valid_calibrations AS
(WITH _weights  AS (SELECT *,
--                            1 - exp(-ln(2) / $halflife)                                         AS alpha,
                           2 / ($span + 1)                                                     AS alpha,
                           ROW_NUMBER() OVER (PARTITION BY mpid, brand ORDER BY date DESC) - 1 AS days_ago,
                           --make sure to only sum the weights that will be multiplied by a valid period conversion rate
                           IFF(valid_period, POWER(1 - alpha, days_ago), NULL)                 AS weight
                    FROM _valid_calibrations),


      _averaged AS (SELECT *,
                           --create avg and median so that we can fill in gaps when an influencer has not been in hdyh yet
                           AVG(leads_per_impact_rate_valid) OVER (PARTITION BY brand)                                            AS avg_leads_per_impact_rate_valid,
                           AVG(vips_per_impact_rate_valid) OVER (PARTITION BY brand)                                             AS avg_vips_per_impact_rate_valid,
                           MEDIAN(leads_per_impact_rate_valid) OVER (PARTITION BY brand)                                         AS median_leads_per_impact_rate_valid,
                           MEDIAN(vips_per_impact_rate_valid) OVER (PARTITION BY brand)                                          AS median_vips_per_impact_rate_valid,
                           leads_per_impact_rate_valid * weight                                                                  AS weighted_leads_per_impact_rate_valid,
                           vips_per_impact_rate_valid * weight                                                                   AS weighted_vips_per_impact_rate_valid,
                           -- exponential moving average
                           SUM(weight)
                               OVER (PARTITION BY mpid, brand ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)    AS weight_sum,
                           SUM(weighted_leads_per_impact_rate_valid)
                               OVER (PARTITION BY mpid, brand ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)    AS leads_per_impact_rate_valid_weighted_sum,
                           SUM(weighted_vips_per_impact_rate_valid)
                               OVER (PARTITION BY mpid, brand ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)    AS vips_per_impact_rate_valid_weighted_sum,
                           leads_per_impact_rate_valid_weighted_sum / weight_sum                                                 AS leads_per_impact_rate_valid_exp_ma,
                           vips_per_impact_rate_valid_weighted_sum / weight_sum                                                  AS vips_per_impact_rate_valid_exp_ma,
                           -- simple moving average
                           AVG(leads_per_impact_rate_valid)
                               OVER ( PARTITION BY mpid, brand ORDER BY date ROWS BETWEEN $ma_window PRECEDING AND CURRENT ROW ) AS leads_per_impact_rate_valid_ma,
                           AVG(vips_per_impact_rate_valid)
                               OVER ( PARTITION BY mpid, brand ORDER BY date ROWS BETWEEN $ma_window PRECEDING AND CURRENT ROW ) AS vips_per_impact_rate_valid_ma
                    FROM _weights),
      _filled   AS (SELECT *,
                           --forward fill averages for the simple moving average
                           LAST_VALUE(leads_per_impact_rate_valid_ma)
                                      IGNORE NULLS OVER (PARTITION BY mpid, brand ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS leads_per_impact_rate_valid_ma_filled,
                           LAST_VALUE(vips_per_impact_rate_valid_ma)
                                      IGNORE NULLS OVER (PARTITION BY mpid, brand ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS vips_per_impact_rate_valid_ma_filled
                    FROM _averaged)
 SELECT *
 FROM _filled
 ORDER BY date DESC);



CREATE OR REPLACE TEMP TABLE _finalized_with_predictions AS
(SELECT *,
        --calculate estimated for times when not in hdyh using exponential moving average (not simple moving average - that one is just there as a comparison point)
        IFF(final_in_hdyh = FALSE,
            COALESCE(leads_per_impact_rate_valid_exp_ma, median_vips_per_impact_rate_valid) * impact_score,
            NULL)                                                                                                AS estimated_hdyh_leads,
        IFF(final_in_hdyh = FALSE,
            COALESCE(vips_per_impact_rate_valid_exp_ma, median_vips_per_impact_rate_valid) * impact_score,
            NULL)                                                                                                AS estimated_hdyh_vips
 FROM _averaged_valid_calibrations);


-- SAVE
-- USE ROLE tfg_media_admin;

CREATE OR REPLACE TABLE reporting_media_base_prod.influencers.daily_performance_by_influencer_with_post_data AS
(SELECT *, $update_datetime AS last_updated FROM _finalized_with_predictions);

