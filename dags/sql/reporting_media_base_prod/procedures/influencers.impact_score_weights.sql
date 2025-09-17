--- get the impact score weights for tomorrow

CREATE OR REPLACE TEMPORARY TABLE _periods AS
(SELECT DISTINCT brand,
                 mpid,
                 period_start_filled,
                 period_end_filled,
                 --metric
                 period_hdyh_vips / NULLIF(period_impressions, 0)    AS vips_per_impressions,
                 period_hdyh_vips / NULLIF(period_engagements, 0)    AS vips_per_engagements,
                 period_hdyh_vips / NULLIF(period_views, 0)          AS vips_per_views,
                 period_hdyh_vips / NULLIF(period_earnedmedia, 0)    AS vips_per_earnedmedia,
                 period_hdyh_vips / NULLIF(period_likes, 0)          AS vips_per_likes,
                 period_hdyh_vips / NULLIF(period_shares, 0)         AS vips_per_shares,
                 period_hdyh_vips / NULLIF(period_saves, 0)          AS vips_per_saves,
                 period_hdyh_vips / NULLIF(period_comments, 0)       AS vips_per_comments,
                 period_hdyh_vips / NULLIF(period_tapsback, 0)       AS vips_per_tapsback,
                 period_hdyh_vips / NULLIF(period_replies, 0)        AS vips_per_replies,
                 period_hdyh_vips / NULLIF(period_reach, 0)          AS vips_per_reach,
                 --socialnetwork
                 period_hdyh_vips / NULLIF(period_youtube_bool, 0)   AS vips_per_youtube,
                 period_hdyh_vips / NULLIF(period_pinterest_bool, 0) AS vips_per_pinterest,
                 period_hdyh_vips / NULLIF(period_instagram_bool, 0) AS vips_per_instagram,
                 period_hdyh_vips / NULLIF(period_twitter_bool, 0)   AS vips_per_twitter,
                 period_hdyh_vips / NULLIF(period_tiktok_bool, 0)    AS vips_per_tiktok,
                 period_hdyh_vips / NULLIF(period_twitch_bool, 0)    AS vips_per_twitch,
                 period_hdyh_vips / NULLIF(period_facebook_bool, 0)  AS vips_per_facebook
 FROM reporting_media_base_prod.influencers.daily_performance_by_influencer_with_post_data
 WHERE period_authenticated = 1
--    AND discard_period_on_too_few_impressions_for_leads = FALSE
   AND period_start_filled IS NOT NULL
   AND period_end_filled IS NOT NULL
   AND date >= '2022-01-01');

CREATE OR REPLACE TEMPORARY TABLE _avgs AS
(SELECT brand,
        --metrics
        MEDIAN(vips_per_impressions)                                 AS vips_per_impressions_median,
        AVG(vips_per_impressions) + 3 * STDDEV(vips_per_impressions) AS vips_per_impressions_upper,
        MEDIAN(vips_per_engagements)                                 AS vips_per_engagements_median,
        AVG(vips_per_engagements) + 3 * STDDEV(vips_per_engagements) AS vips_per_engagements_upper,
        MEDIAN(vips_per_views)                                       AS vips_per_views_median,
        AVG(vips_per_views) + 3 * STDDEV(vips_per_views)             AS vips_per_views_upper,
        MEDIAN(vips_per_earnedmedia)                                 AS vips_per_earnedmedia_median,
        AVG(vips_per_earnedmedia) + 3 * STDDEV(vips_per_earnedmedia) AS vips_per_earnedmedia_upper,
        MEDIAN(vips_per_likes)                                       AS vips_per_likes_median,
        AVG(vips_per_likes) + 3 * STDDEV(vips_per_likes)             AS vips_per_likes_upper,
        MEDIAN(vips_per_shares)                                      AS vips_per_shares_median,
        AVG(vips_per_shares) + 3 * STDDEV(vips_per_shares)           AS vips_per_shares_upper,
        MEDIAN(vips_per_saves)                                       AS vips_per_saves_median,
        AVG(vips_per_saves) + 3 * STDDEV(vips_per_saves)             AS vips_per_saves_upper,
        MEDIAN(vips_per_comments)                                    AS vips_per_comments_median,
        AVG(vips_per_comments) + 3 * STDDEV(vips_per_comments)       AS vips_per_comments_upper,
        MEDIAN(vips_per_tapsback)                                    AS vips_per_tapsback_median,
        AVG(vips_per_tapsback) + 3 * STDDEV(vips_per_tapsback)       AS vips_per_tapsback_upper,
        MEDIAN(vips_per_replies)                                     AS vips_per_replies_median,
        AVG(vips_per_replies) + 3 * STDDEV(vips_per_replies)         AS vips_per_replies_upper,
        MEDIAN(vips_per_reach)                                       AS vips_per_reach_median,
        AVG(vips_per_reach) + 3 * STDDEV(vips_per_reach)             AS vips_per_reach_upper,
        --socialnetwork
        MEDIAN(vips_per_youtube)                                     AS vips_per_youtube_median,
        AVG(vips_per_youtube) + 3 * STDDEV(vips_per_youtube)         AS vips_per_youtube_upper,
        MEDIAN(vips_per_pinterest)                                   AS vips_per_pinterest_median,
        AVG(vips_per_pinterest) + 3 * STDDEV(vips_per_pinterest)     AS vips_per_pinterest_upper,
        MEDIAN(vips_per_instagram)                                   AS vips_per_instagram_median,
        AVG(vips_per_instagram) + 3 * STDDEV(vips_per_instagram)     AS vips_per_instagram_upper,
        MEDIAN(vips_per_twitter)                                     AS vips_per_twitter_median,
        AVG(vips_per_twitter) + 3 * STDDEV(vips_per_twitter)         AS vips_per_twitter_upper,
        MEDIAN(vips_per_tiktok)                                      AS vips_per_tiktok_median,
        AVG(vips_per_tiktok) + 3 * STDDEV(vips_per_tiktok)           AS vips_per_tiktok_upper,
        MEDIAN(vips_per_twitch)                                      AS vips_per_twitch_median,
        AVG(vips_per_twitch) + 3 * STDDEV(vips_per_twitch)           AS vips_per_twitch_upper,
        MEDIAN(vips_per_facebook)                                    AS vips_per_facebook_median,
        AVG(vips_per_facebook) + 3 * STDDEV(vips_per_facebook)       AS vips_per_facebook_upper
 FROM _periods
 GROUP BY brand);


CREATE OR REPLACE TEMPORARY TABLE _cleaned AS
(SELECT p.brand,
        mpid,
        period_start_filled,
        period_end_filled,
        --metrics
        IFF(vips_per_impressions > vips_per_impressions_upper, vips_per_impressions_median,
            vips_per_impressions)                                                                                 AS vips_per_impressions_cleaned,
        IFF(vips_per_engagements > vips_per_engagements_upper, vips_per_engagements_median,
            vips_per_engagements)                                                                                 AS vips_per_engagements_cleaned,
        IFF(vips_per_views > vips_per_views_upper, vips_per_views_median,
            vips_per_views)                                                                                       AS vips_per_views_cleaned,
        IFF(vips_per_earnedmedia > vips_per_earnedmedia_upper, vips_per_earnedmedia_median,
            vips_per_earnedmedia)                                                                                 AS vips_per_earnedmedia_cleaned,
        IFF(vips_per_likes > vips_per_likes_upper, vips_per_likes_median,
            vips_per_likes)                                                                                       AS vips_per_likes_cleaned,
        IFF(vips_per_shares > vips_per_shares_upper, vips_per_shares_median,
            vips_per_shares)                                                                                      AS vips_per_shares_cleaned,
        IFF(vips_per_saves > vips_per_saves_upper, vips_per_saves_median,
            vips_per_saves)                                                                                       AS vips_per_saves_cleaned,
        IFF(vips_per_comments > vips_per_comments_upper, vips_per_comments_median,
            vips_per_comments)                                                                                    AS vips_per_comments_cleaned,
        IFF(vips_per_tapsback > vips_per_tapsback_upper, vips_per_tapsback_median,
            vips_per_tapsback)                                                                                    AS vips_per_tapsback_cleaned,
        IFF(vips_per_replies > vips_per_replies_upper, vips_per_replies_median,
            vips_per_replies)                                                                                     AS vips_per_replies_cleaned,
        IFF(vips_per_reach > vips_per_reach_upper, vips_per_reach_median,
            vips_per_reach)                                                                                       AS vips_per_reach_cleaned,
        --socialnetwork
        IFF(vips_per_youtube > vips_per_youtube_upper, vips_per_youtube_median,
            vips_per_youtube)                                                                                     AS vips_per_youtube_cleaned,
        IFF(vips_per_pinterest > vips_per_pinterest_upper, vips_per_pinterest_median,
            vips_per_pinterest)                                                                                   AS vips_per_pinterest_cleaned,
        IFF(vips_per_instagram > vips_per_instagram_upper, vips_per_instagram_median,
            vips_per_instagram)                                                                                   AS vips_per_instagram_cleaned,
        IFF(vips_per_twitter > vips_per_twitter_upper, vips_per_twitter_median,
            vips_per_twitter)                                                                                     AS vips_per_twitter_cleaned,
        IFF(vips_per_tiktok > vips_per_tiktok_upper, vips_per_tiktok_median,
            vips_per_tiktok)                                                                                      AS vips_per_tiktok_cleaned,
        IFF(vips_per_twitch > vips_per_twitch_upper, vips_per_twitch,
            vips_per_twitch)                                                                                      AS vips_per_twitch_cleaned,
        IFF(vips_per_facebook > vips_per_facebook_upper, vips_per_facebook_median,
            vips_per_facebook)                                                                                    AS vips_per_facebook_cleaned
 FROM _periods p
      LEFT JOIN _avgs ON p.brand = _avgs.brand);

CREATE OR REPLACE TEMPORARY TABLE _relative_weights AS
(SELECT brand,
        --metrics: Scale to likes
        AVG(vips_per_impressions_cleaned) AS avg_vips_per_impressions_cleaned,
        AVG(vips_per_likes_cleaned)       AS avg_vips_per_likes_cleaned,
        AVG(vips_per_shares_cleaned)      AS avg_vips_per_shares_cleaned,
        AVG(vips_per_saves_cleaned)       AS avg_vips_per_saves_cleaned,
        AVG(vips_per_comments_cleaned)    AS avg_vips_per_comments_cleaned,
        AVG(vips_per_tapsback_cleaned)    AS avg_vips_per_tapsback_cleaned,
        AVG(vips_per_replies_cleaned)     AS avg_vips_per_replies_cleaned,
        AVG(vips_per_reach_cleaned)       AS avg_vips_per_reach_cleaned,
        AVG(vips_per_engagements_cleaned) AS avg_vips_per_engagements_cleaned,
        AVG(vips_per_views_cleaned)       AS avg_vips_per_views_cleaned,
        AVG(vips_per_earnedmedia_cleaned) AS avg_vips_per_earnedmedia_cleaned,
        AVG(vips_per_likes_cleaned)       AS metrics_scaler,
        --socialnetwork: Scale to instagram
        AVG(vips_per_youtube_cleaned)     AS avg_vips_per_youtube_cleaned,
        AVG(vips_per_pinterest_cleaned)   AS avg_vips_per_pinterest_cleaned,
        AVG(vips_per_instagram_cleaned)   AS avg_vips_per_instagram_cleaned,
        AVG(vips_per_twitter_cleaned)     AS avg_vips_per_twitter_cleaned,
        AVG(vips_per_tiktok_cleaned)      AS avg_vips_per_tiktok_cleaned,
        AVG(vips_per_twitch_cleaned)      AS avg_vips_per_twitch_cleaned,
        AVG(vips_per_facebook_cleaned)    AS avg_vips_per_facebook_cleaned,
        AVG(vips_per_instagram_cleaned)   AS socialnetwork_scaler
 FROM _cleaned
 WHERE period_start_filled >= DATEADD(DAYS, -3 * 365, CURRENT_DATE)
 GROUP BY brand);


-- CREATE OR REPLACE TABLE reporting_media_base_prod.influencers.impact_score_metric_weights AS
INSERT INTO reporting_media_base_prod.influencers.impact_score_metric_weights
    WITH _scaled_metric_weights AS (SELECT brand,
                                           avg_vips_per_impressions_cleaned / metrics_scaler AS impressions_weight,
                                           avg_vips_per_views_cleaned / metrics_scaler       AS views_weight,
                                           avg_vips_per_reach_cleaned / metrics_scaler       AS reach_weight,
                                           avg_vips_per_engagements_cleaned / metrics_scaler AS engagements_weight,
                                           avg_vips_per_tapsback_cleaned / metrics_scaler    AS tapsback_weight,
                                           avg_vips_per_likes_cleaned / metrics_scaler       AS likes_weight,
                                           avg_vips_per_saves_cleaned / metrics_scaler       AS saves_weight,
                                           avg_vips_per_comments_cleaned / metrics_scaler    AS comments_weight,
                                           avg_vips_per_shares_cleaned / metrics_scaler      AS shares_weight,
                                           avg_vips_per_replies_cleaned / metrics_scaler     AS replies_weight,
                                           avg_vips_per_earnedmedia_cleaned / metrics_scaler AS earnedmedia_weight
                                    FROM _relative_weights)
    SELECT *, CURRENT_TIMESTAMP() AS last_updated
    FROM _scaled_metric_weights;

-- CREATE OR REPLACE TABLE reporting_media_base_prod.influencers.impact_score_socialnetwork_weights AS
INSERT INTO reporting_media_base_prod.influencers.impact_score_socialnetwork_weights
    WITH social_networks AS (SELECT *
                             FROM (VALUES ('youtube'),
                                          ('pinterest'),
                                          ('instagram'),
                                          ('twitter'),
                                          ('tiktok'),
                                          ('twitch'),
                                          ('facebook')) AS sn(socialnetwork)
                                  JOIN (SELECT DISTINCT brand FROM _periods))
    SELECT sn.brand,
           sn.socialnetwork,
           CASE WHEN sn.socialnetwork = 'instagram' THEN avg_vips_per_instagram_cleaned
                WHEN sn.socialnetwork = 'pinterest' THEN avg_vips_per_pinterest_cleaned
                WHEN sn.socialnetwork = 'youtube' THEN avg_vips_per_youtube_cleaned
                WHEN sn.socialnetwork = 'twitter' THEN avg_vips_per_twitter_cleaned
                WHEN sn.socialnetwork = 'tiktok' THEN avg_vips_per_tiktok_cleaned
                WHEN sn.socialnetwork = 'twitch' THEN avg_vips_per_twitch_cleaned
                WHEN sn.socialnetwork = 'facebook' THEN avg_vips_per_facebook_cleaned END AS vips_per_socialnetwork,
           MAX(avg_vips_per_instagram_cleaned) OVER (PARTITION BY sn.brand )              AS scaler,
           vips_per_socialnetwork / scaler                                                AS vips_per_socialnetwork_scaled,
           CURRENT_TIMESTAMP()                                                            AS last_updated
    FROM social_networks sn
         LEFT JOIN _relative_weights rw ON sn.brand = rw.brand
    ORDER BY sn.brand, sn.socialnetwork;

