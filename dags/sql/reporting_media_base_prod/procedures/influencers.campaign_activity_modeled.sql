USE SCHEMA reporting_media_base_prod.influencers;

-- daily engagements data
CREATE OR REPLACE TEMPORARY TABLE _ciq AS
        SELECT CASE WHEN lower(campaignname) LIKE ANY ('%savage%', '%sx%') THEN 'sx'
                WHEN lower(campaignname) LIKE ANY ('%fabletics% men%', '%flm%') THEN 'flm'
                WHEN lower(campaignname) LIKE ANY ('%fabletics%', '%fl%') THEN 'fl'
                WHEN lower(campaignname) LIKE ANY ('%scrubs%', '%scb%') THEN 'scb'
                WHEN lower(campaignname) LIKE ANY ('%yitty%', '%yty%') THEN 'yty'
                WHEN lower(campaignname) LIKE ANY ('%justfab%', 'jf %') THEN 'jfb' END                            AS brand,
           CASE WHEN campaignname LIKE ANY ('%\\_US\\_%') ESCAPE '\\' THEN 'US'
                WHEN campaignname LIKE ANY ('% FR %', '% FR', '% France %') THEN 'FR'
                WHEN campaignname LIKE ANY ('% NL %', '% NL') THEN 'NL'
                WHEN campaignname LIKE ANY ('% ES %', '% ES') THEN 'ES'
                WHEN campaignname LIKE ANY ('% DE %', '% DE') THEN 'DE'
                WHEN campaignname LIKE ANY ('% UK %', '% UK') THEN 'UK' END                                       AS campaign_country,
           CASE WHEN campaignname ILIKE ANY ('Men %', '% Men %', '% Men', '%(M)%', '%FLM%') THEN 'M' ELSE 'W' END AS campaign_gender,
           CASE WHEN contains(campaignname, 'HDYH') THEN TRUE ELSE FALSE END                                      AS campaign_hdyh,
           campaignid,
           campaignname,
           publisherid,
           first_value(publishername)
                       OVER (PARTITION BY brand, publisherid, postid ORDER BY meta_is_current DESC )                  AS publishername,
           lower(c.posttype)                                                                                          AS posttype,
           iff(posttype = 'story', 1, iff(linked IS NULL, 0, linked))                                                 AS authenticated,
           iff(c.socialnetwork = 'Instagram' AND posttype = 'story',
               dense_rank() OVER (PARTITION BY campaignid, publisherid, posttype, cast(datesubmitted AS date) ORDER BY datesubmitted ASC),
               NULL)                                                                                                  AS ig_story_frame_sequence,
           lower(socialnetwork)                                                                                       AS socialnetwork,
           first_value(socialnetworkid)
                       OVER (PARTITION BY brand, publisherid, postid ORDER BY meta_is_current DESC )                  AS socialnetworkid,
           postid,
           first_value(datesubmitted)
                       OVER (PARTITION BY brand, publisherid, postid ORDER BY meta_is_current DESC )                  AS datesubmitted,
           CASE WHEN meta_from_datetime = min(meta_from_datetime) OVER (PARTITION BY postid) THEN 1
                ELSE 0 END                                                                                            AS first_record,
           CASE WHEN meta_is_current = TRUE THEN meta_create_datetime
                ELSE meta_to_datetime END                                                                             AS ciq_datetime,
           to_date(ciq_datetime)                                                                                      AS ciq_date,
           max(ciq_date) OVER (PARTITION BY brand, publisherid, postid )                                              AS last_ciq_record,
           row_number() OVER (PARTITION BY brand, publisherid, postid, ciq_date ORDER BY ciq_datetime DESC)           AS last_record_of_day,
           datediff('days', datesubmitted, current_date())                                                            AS days_since_post,
           datediff('hours', datesubmitted, current_timestamp())                                                      AS hours_since_post,
           coalesce(earnedmedia, 0)                                                                                   AS earnedmedia,
           coalesce(networkimpressions, 0)                                                                            AS impressions,
           coalesce(engagement, networkengagements)                                                                   AS engagements,
           coalesce(likes, 0)                                                                                         AS likes,
           coalesce(comments, 0)                                                                                      AS comments,
           coalesce(totalshares, 0)                                                                                   AS shares,
           coalesce(saves, 0)                                                                                         AS saves,
           coalesce(views, 0)                                                                                         AS views,
           coalesce(networkreach, 0)                                                                                  AS reach,
           coalesce(replies, 0)                                                                                       AS replies,
           coalesce(tapsback, 0)                                                                                      AS tapsback
    FROM reporting_media_base_prod.creatoriq.campaign_activity_cleansed c
    WHERE datesubmitted >= '2019-06-01'
      AND campaign_country = 'US'
      AND campaign_hdyh = True
    ORDER BY publishername ASC, postid ASC, meta_to_datetime DESC, meta_from_datetime ASC;


CREATE OR REPLACE TEMPORARY TABLE _indexed AS
    WITH RECURSIVE
        daterange AS (SELECT date('2019-01-01') AS date -- Start date
                      UNION ALL
                      SELECT dateadd(DAY, 1, date)
                      FROM daterange
                      WHERE date < current_date()),
        _dates    AS (SELECT date FROM daterange),
        _combos   AS (SELECT date,
                             brand,
                             publisherid,
                             publishername,
                             postid,
                             posttype,
                             ig_story_frame_sequence,
                             authenticated,
                             socialnetwork,
                             socialnetworkid,
                             campaignname,
                             datesubmitted,
                             last_ciq_record,
                             days_since_post,
                             hours_since_post
                      FROM (SELECT DISTINCT date FROM _dates) d
                           JOIN (SELECT DISTINCT brand,
                                                 publisherid,
                                                 publishername,
                                                 postid,
                                                 posttype,
                                                 ig_story_frame_sequence,
                                                 authenticated,
                                                 socialnetwork,
                                                 socialnetworkid,
                                                 campaignname,
                                                 datesubmitted,
                                                 last_ciq_record,
                                                 days_since_post,
                                                 hours_since_post
                                 FROM _ciq)
                      WHERE date >= datesubmitted
                        AND date <= last_ciq_record
                      ORDER BY brand, datesubmitted, postid, date)
    SELECT d.date,
           d.brand,
           d.publisherid,
           d.publishername,
           d.postid,
           d.posttype,
           d.ig_story_frame_sequence,
           d.authenticated,
           d.socialnetwork,
           d.socialnetworkid,
           d.campaignname,
           d.datesubmitted,
           datediff('days', to_date(d.datesubmitted), d.date)                                                                  AS days_after_post,
           d.days_since_post,
           d.hours_since_post,
           c.ciq_date,
           c.impressions                                                                                                       AS raw_cum_impressions,
           -- Get the last non-zero impressions for each postid and date order
           -- this should fix times when a post goes all of a sudden to zero for a few days and then back to what it was at before
           -- ex. 100, 150, 0, 0, 150, 152 --> the raw incremental data will look like 100, 50, -150, 0, 150, 2 which obviously isnt what happened
           -- this fills the post forward if there are those random zeros so that the data now looks like 100, 150, 150, 150, 150, 152
           -- and the incremental data will look like 100, 50, 0, 0, 0, 2 which is more correct!!
           last_value(nullif(c.impressions, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_impressions,
           iff(c.impressions = 0 AND last_non_zero_impressions > 0, last_non_zero_impressions,
               c.impressions)                                                                                                  AS cum_impressions,

           c.engagements                                                                                                       AS raw_cum_engagements,
           last_value(nullif(c.engagements, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_engagements,
           iff(c.engagements = 0 AND last_non_zero_engagements > 0, last_non_zero_engagements,
               c.engagements)                                                                                                  AS cum_engagements,

           c.likes                                                                                                             AS raw_cum_likes,
           last_value(nullif(c.likes, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_likes,
           iff(c.likes = 0 AND last_non_zero_likes > 0, last_non_zero_likes,
               c.likes)                                                                                                        AS cum_likes,

           c.comments                                                                                                          AS raw_cum_comments,
           last_value(nullif(c.comments, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_comments,
           iff(c.comments = 0 AND last_non_zero_comments > 0, last_non_zero_comments,
               c.comments)                                                                                                     AS cum_comments,

           c.shares                                                                                                            AS raw_cum_shares,
           last_value(nullif(c.shares, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_shares,
           iff(c.shares = 0 AND last_non_zero_shares > 0, last_non_zero_shares,
               c.shares)                                                                                                       AS cum_shares,

           c.saves                                                                                                             AS raw_cum_saves,
           last_value(nullif(c.saves, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_saves,
           iff(c.saves = 0 AND last_non_zero_saves > 0, last_non_zero_saves,
               c.saves)                                                                                                        AS cum_saves,

           c.views                                                                                                             AS raw_cum_views,
           last_value(nullif(c.views, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_views,
           iff(c.views = 0 AND last_non_zero_views > 0, last_non_zero_views,
               c.views)                                                                                                        AS cum_views,

           c.reach                                                                                                             AS raw_cum_reach,
           last_value(nullif(c.reach, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_reach,
           iff(c.reach = 0 AND last_non_zero_reach > 0, last_non_zero_reach,
               c.reach)                                                                                                        AS cum_reach,

           c.replies                                                                                                           AS raw_cum_replies,
           last_value(nullif(c.replies, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_replies,
           iff(c.replies = 0 AND last_non_zero_replies > 0, last_non_zero_replies,
               c.replies)                                                                                                      AS cum_replies,

           c.tapsback                                                                                                          AS raw_cum_tapsback,
           last_value(nullif(c.tapsback, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_tapsback,
           iff(c.tapsback = 0 AND last_non_zero_tapsback > 0, last_non_zero_tapsback,
               c.tapsback)                                                                                                     AS cum_tapsback,

           c.earnedmedia                                                                                                       AS raw_cum_earnedmedia,
           last_value(nullif(c.earnedmedia, 0))
                      IGNORE NULLS OVER (PARTITION BY d.postid ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_zero_earnedmedia,
           iff(c.earnedmedia = 0 AND last_non_zero_earnedmedia > 0, last_non_zero_earnedmedia,
               c.earnedmedia)                                                                                                  AS cum_earnedmedia

    FROM _combos d
         LEFT JOIN _ciq c ON d.brand = c.brand AND d.publisherid = c.publisherid AND d.postid = c.postid AND
                             d.date = c.ciq_date AND last_record_of_day = 1
    ORDER BY d.date;

CREATE OR REPLACE TEMPORARY TABLE _incremental AS
    --Literally just gets the previous day's value for each engagement metric
    SELECT *,
           cum_impressions - lag(cum_impressions, 1, 0)
                                 IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)              AS raw_inc_impressions,
           cum_engagements - lag(cum_engagements, 1, 0)
                                 IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)              AS raw_inc_engagements,
           cum_likes - lag(cum_likes, 1, 0)
                           IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)                    AS raw_inc_likes,
           cum_comments - lag(cum_comments, 1, 0)
                              IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)                 AS raw_inc_comments,
           cum_shares - lag(cum_shares, 1, 0)
                            IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)                   AS raw_inc_shares,
           cum_saves - lag(cum_saves, 1, 0)
                           IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)                    AS raw_inc_saves,
           cum_views - lag(cum_views, 1, 0)
                           IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)                    AS raw_inc_views,
           cum_reach - lag(cum_reach, 1, 0)
                           IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)                    AS raw_inc_reach,
           cum_replies - lag(cum_replies, 1, 0)
                             IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)                  AS raw_inc_replies,
           cum_tapsback - lag(cum_tapsback, 1, 0)
                              IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)                 AS raw_inc_tapsback,
           cum_earnedmedia - lag(cum_earnedmedia, 1, 0)
                                 IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date ASC)              AS raw_inc_earnedmedia,
           max(cum_impressions) OVER (PARTITION BY publisherid, postid)                                              AS total_impressions,
           max(cum_engagements) OVER (PARTITION BY publisherid, postid)                                              AS total_engagements,
           max(cum_likes) OVER (PARTITION BY publisherid, postid)                                                    AS total_likes,
           max(cum_comments) OVER (PARTITION BY publisherid, postid)                                                 AS total_comments,
           max(cum_shares) OVER (PARTITION BY publisherid, postid)                                                   AS total_shares,
           max(cum_saves) OVER (PARTITION BY publisherid, postid)                                                    AS total_saves,
           max(cum_views) OVER (PARTITION BY publisherid, postid)                                                    AS total_views,
           max(cum_reach) OVER (PARTITION BY publisherid, postid)                                                    AS total_reach,
           max(cum_replies) OVER (PARTITION BY publisherid, postid)                                                  AS total_replies,
           max(cum_tapsback) OVER (PARTITION BY publisherid, postid)                                                 AS total_tapsback,
           max(cum_earnedmedia) OVER (PARTITION BY publisherid, postid)                                              AS total_earnedmedia
    FROM _indexed
    ORDER BY date;


CREATE OR REPLACE TEMPORARY TABLE _general_decay_curves AS
    WITH t1 AS (SELECT brand,
                       publisherid,
                       postid,
                       socialnetwork,
                       posttype,
                       days_after_post,
                       iff(total_impressions > 0 AND raw_inc_impressions > 0, raw_inc_impressions / total_impressions,
                           NULL)                                                                                 AS pct_impressions,
                       iff(total_engagements > 0 AND raw_inc_engagements > 0, raw_inc_engagements / total_engagements,
                           NULL)                                                                                 AS pct_engagements,
                       iff(total_likes > 0 AND raw_inc_likes > 0, raw_inc_likes / total_likes,
                           NULL)                                                                                 AS pct_likes,
                       iff(total_comments > 0 AND raw_inc_comments > 0, raw_inc_comments / total_comments,
                           NULL)                                                                                 AS pct_comments,
                       iff(total_shares > 0 AND raw_inc_shares > 0, raw_inc_shares / total_shares,
                           NULL)                                                                                 AS pct_shares,
                       iff(total_saves > 0 AND raw_inc_saves > 0, raw_inc_saves / total_saves,
                           NULL)                                                                                 AS pct_saves,
                       iff(total_views > 0 AND raw_inc_views > 0, raw_inc_views / total_views,
                           NULL)                                                                                 AS pct_views,
                       iff(total_reach > 0 AND raw_inc_reach > 0, raw_inc_reach / total_reach,
                           NULL)                                                                                 AS pct_reach,
                       iff(total_replies > 0 AND raw_inc_replies > 0, raw_inc_replies / total_replies,
                           NULL)                                                                                 AS pct_replies,
                       iff(total_tapsback > 0 AND raw_inc_tapsback > 0, raw_inc_tapsback / total_tapsback,
                           NULL)                                                                                 AS pct_tapsback,
                       iff(total_earnedmedia > 0 AND raw_inc_earnedmedia > 0, raw_inc_earnedmedia / total_earnedmedia,
                           NULL)                                                                                 AS pct_earnedmedia
                FROM _incremental
                WHERE days_after_post <= 60
                  AND days_after_post >= 0
                ORDER BY publisherid, postid, days_after_post)
--    , t2 AS (
    SELECT socialnetwork,
           posttype,
           days_after_post,
           count(postid)        AS count_posts,
           avg(pct_impressions) AS avg_pct_impressions,
           avg(pct_engagements) AS avg_pct_engagements,
           avg(pct_likes)       AS avg_pct_likes,
           avg(pct_comments)    AS avg_pct_comments,
           avg(pct_shares)      AS avg_pct_shares,
           avg(pct_saves)       AS avg_pct_saves,
           avg(pct_views)       AS avg_pct_views,
           avg(pct_reach)       AS avg_pct_reach,
           avg(pct_replies)     AS avg_pct_replies,
           avg(pct_tapsback)    AS avg_pct_tapsback,
           avg(pct_earnedmedia) AS avg_pct_earnedmedia
    FROM t1
    GROUP BY socialnetwork, posttype, days_after_post
    HAVING count_posts > 0
    ORDER BY socialnetwork, posttype, days_after_post;


CREATE OR REPLACE TEMPORARY TABLE _modeled AS
    WITH t1 AS (SELECT i.*,
                       coalesce(ciq_date, lead(ciq_date)
                                               IGNORE NULLS OVER (PARTITION BY publisherid, postid ORDER BY date)) AS next_ciq_record,
                       g.avg_pct_impressions,
                       g.avg_pct_engagements,
                       g.avg_pct_likes,
                       g.avg_pct_comments,
                       g.avg_pct_shares,
                       g.avg_pct_saves,
                       g.avg_pct_views,
                       g.avg_pct_reach,
                       g.avg_pct_replies,
                       g.avg_pct_tapsback,
                       g.avg_pct_earnedmedia
                FROM _incremental i
                     LEFT JOIN _general_decay_curves g
                               ON i.days_after_post = g.days_after_post AND i.socialnetwork = g.socialnetwork AND
                                  i.posttype = g.posttype
                ORDER BY i.date)
       --spreads out engagements on days where following days have no record by the average pct engagement curves
       --ex. if incremental data looks like (431, NaN, 100, 17, 5, 2) and the general decay curve over the first 5 days is (.75, .20, .3, .1, ...)
       -- then to fill in day 3, we say 0.20 + 0.03 = 0.23 happened over the 2nd and 3rd days. (0.2/.23) * 100 = 87 happened on the 2nd Day while only
       -- (0.03/0.23)*100 = 13 happened on the 3rd day
       , t2 AS (SELECT *,
                       sum(avg_pct_impressions)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_impressions_within_group,
                       max(raw_inc_impressions)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS impressions_within_group,
                       impressions_within_group * avg_pct_impressions /
                       pct_impressions_within_group                                                      AS m_inc_impressions,

                       sum(avg_pct_engagements)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_engagements_within_group,
                       max(raw_inc_engagements)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS engagements_within_group,
                       engagements_within_group * avg_pct_engagements /
                       pct_engagements_within_group                                                      AS m_inc_engagements,

                       sum(avg_pct_likes)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_likes_within_group,
                       max(raw_inc_likes)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS likes_within_group,
                       likes_within_group * avg_pct_likes / pct_likes_within_group                       AS m_inc_likes,

                       sum(avg_pct_comments)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_comments_within_group,
                       max(raw_inc_comments)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS comments_within_group,
                       comments_within_group * avg_pct_comments / pct_comments_within_group              AS m_inc_comments,

                       sum(avg_pct_shares)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_shares_within_group,
                       max(raw_inc_shares)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS shares_within_group,
                       shares_within_group * avg_pct_shares / pct_shares_within_group                    AS m_inc_shares,

                       sum(avg_pct_saves)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_saves_within_group,
                       max(raw_inc_saves)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS saves_within_group,
                       saves_within_group * avg_pct_saves / pct_saves_within_group                       AS m_inc_saves,

                       sum(avg_pct_views)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_views_within_group,
                       max(raw_inc_views)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS views_within_group,
                       views_within_group * avg_pct_views / pct_views_within_group                       AS m_inc_views,

                       sum(avg_pct_replies)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_replies_within_group,
                       max(raw_inc_replies)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS replies_within_group,
                       replies_within_group * avg_pct_replies / pct_replies_within_group                 AS m_inc_replies,

                       sum(avg_pct_reach)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_reach_within_group,
                       max(raw_inc_reach)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS reach_within_group,
                       reach_within_group * avg_pct_reach / pct_reach_within_group                       AS m_inc_reach,

                       sum(avg_pct_tapsback)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_tapsback_within_group,
                       max(raw_inc_tapsback)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS tapsback_within_group,
                       tapsback_within_group * avg_pct_tapsback / pct_tapsback_within_group              AS m_inc_tapsback,

                       sum(avg_pct_earnedmedia)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS pct_earnedmedia_within_group,
                       max(raw_inc_earnedmedia)
                           OVER (PARTITION BY publisherid, postid, next_ciq_record)                      AS earnedmedia_within_group,
                       earnedmedia_within_group * avg_pct_earnedmedia /
                       pct_earnedmedia_within_group                                                      AS m_inc_earnedmedia

                FROM t1
                ORDER BY date)

    --This moves impressions on stories after day 1 to day 1

-- Example for a story
    -- Impressions  | after_day_0 | sum_after_day_0  | non_story_or_before_day_2 | final_modeled_impressions
    -- 100          |      0      |      100         |            1              |          1*100 = 100
    -- 50           |      1      |      60          |            1              |          1*60 = 60
    -- 10           |      1      |      60          |            0              |          0*60 = 0

-- Example for a non-story
    -- Impressions  | after_day_0 | sum_after_day_0  | non_story_or_before_day_2 | final_modeled_impressions
    -- 100          |      0      |      100         |            1              |          1*100 = 100
    -- 50           |      1      |      50          |            1              |          1*50 = 50
    -- 10           |      2      |      10          |            1              |          1*10 = 10

    SELECT *,
           CASE WHEN posttype = 'story' AND days_after_post > 0 THEN 1 ELSE days_after_post END AS after_day_0,
           CASE WHEN days_after_post > 1 AND posttype = 'story' THEN 0 ELSE 1 END               AS non_story_or_before_day_2,

           sum(m_inc_impressions)
               OVER (PARTITION BY publisherid, postid, after_day_0)                             AS sum_modeled_inc_impressions_after_day_0,
           sum_modeled_inc_impressions_after_day_0 * non_story_or_before_day_2                  AS modeled_inc_impressions,

           sum(m_inc_engagements)
               OVER (PARTITION BY publisherid, postid, after_day_0)                             AS sum_modeled_inc_engagements_after_day_0,
           sum_modeled_inc_engagements_after_day_0 * non_story_or_before_day_2                  AS modeled_inc_engagements,

           sum(m_inc_likes) OVER (PARTITION BY publisherid, postid, after_day_0)                AS sum_modeled_inc_likes_after_day_0,
           sum_modeled_inc_likes_after_day_0 * non_story_or_before_day_2                        AS modeled_inc_likes,

           sum(m_inc_comments)
               OVER (PARTITION BY publisherid, postid, after_day_0)                             AS sum_modeled_inc_comments_after_day_0,
           sum_modeled_inc_comments_after_day_0 * non_story_or_before_day_2                     AS modeled_inc_comments,

           sum(m_inc_saves) OVER (PARTITION BY publisherid, postid, after_day_0)                AS sum_modeled_inc_saves_after_day_0,
           sum_modeled_inc_saves_after_day_0 * non_story_or_before_day_2                        AS modeled_inc_saves,

           sum(m_inc_shares) OVER (PARTITION BY publisherid, postid, after_day_0)               AS sum_modeled_inc_shares_after_day_0,
           sum_modeled_inc_shares_after_day_0 * non_story_or_before_day_2                       AS modeled_inc_shares,

           sum(m_inc_views) OVER (PARTITION BY publisherid, postid, after_day_0)                AS sum_modeled_inc_views_after_day_0,
           sum_modeled_inc_views_after_day_0 * non_story_or_before_day_2                        AS modeled_inc_views,

           sum(m_inc_replies) OVER (PARTITION BY publisherid, postid, after_day_0)              AS sum_modeled_inc_replies_after_day_0,
           sum_modeled_inc_replies_after_day_0 * non_story_or_before_day_2                      AS modeled_inc_replies,

           sum(m_inc_reach) OVER (PARTITION BY publisherid, postid, after_day_0)                AS sum_modeled_inc_reach_after_day_0,
           sum_modeled_inc_reach_after_day_0 * non_story_or_before_day_2                        AS modeled_inc_reach,

           sum(m_inc_tapsback)
               OVER (PARTITION BY publisherid, postid, after_day_0)                             AS sum_modeled_inc_tapsback_after_day_0,
           sum_modeled_inc_tapsback_after_day_0 * non_story_or_before_day_2                     AS modeled_inc_tapsback,

           sum(m_inc_earnedmedia)
               OVER (PARTITION BY publisherid, postid, after_day_0)                             AS sum_modeled_inc_earnedmedia_after_day_0,
           sum_modeled_inc_earnedmedia_after_day_0 * non_story_or_before_day_2                  AS modeled_inc_earnedmedia
    FROM t2
    ORDER BY date;


-- create avg audience metrics table
CREATE OR REPLACE TEMPORARY TABLE _audience_metrics AS
    WITH RECURSIVE _daterange AS (SELECT date('2019-01-01') AS date -- Start date
                                  UNION ALL
                                  SELECT dateadd(DAY, 1, date)
                                  FROM _daterange
                                  WHERE date < current_date())
       , _dates               AS (SELECT date FROM _daterange)
       , _audience            AS (SELECT DISTINCT brand,
                                                  a.publisherid,
                                                  socialnetworkid,
                                                  lower(network)                                                                                                       AS network,
                                                  CASE WHEN meta_is_current = TRUE THEN meta_create_datetime
                                                       ELSE meta_to_datetime END                                                                                       AS ciq_datetime,
                                                  to_date(ciq_datetime)                                                                                                AS ciq_date,
                                                  row_number() OVER (PARTITION BY brand, a.publisherid, network, socialnetworkid, ciq_date ORDER BY ciq_datetime DESC) AS last_record_of_day,
                                                  female,
                                                  CASE WHEN audiencecountry_top1_name = 'United States'
                                                           THEN audienceshare_top1
                                                       WHEN audiencecountry_top2_name = 'United States'
                                                           THEN audienceshare_top2
                                                       WHEN audiencecountry_top3_name = 'United States'
                                                           THEN audienceshare_top3
                                                       WHEN audiencecountry_top4_name = 'United States'
                                                           THEN audienceshare_top4
                                                       ELSE 100 - (audienceshare_top1 + audienceshare_top2 +
                                                                   audienceshare_top3 +
                                                                   audienceshare_top4) END                                                                             AS percent_us
                                  FROM lake.creatoriq.audience_first_party a
                                       JOIN (SELECT DISTINCT brand, publisherid FROM _ciq) c
                                            ON c.publisherid = a.publisherid
                                  ORDER BY a.publisherid, network, ciq_date)
       , _publisher_networks  AS (SELECT DISTINCT brand, publisherid, socialnetworkid, socialnetwork
                                  FROM _ciq
                                  ORDER BY brand, publisherid, socialnetwork)
       , _combos              AS (SELECT brand, publisherid, socialnetwork, socialnetworkid, date
                                  FROM (SELECT DISTINCT date FROM _dates) d
                                       JOIN _publisher_networks
                                  ORDER BY brand, publisherid, socialnetwork, socialnetworkid, date)
       , _avgs                AS (SELECT brand,
                                         network,
                                         avg(female)     AS network_pct_female,
                                         avg(percent_us) AS network_pct_us
                                  FROM _audience
                                  GROUP BY brand, network)
    SELECT c.*,
           t.ciq_date,
           coalesce(t.female, lag(t.female)
                                  IGNORE NULLS OVER (PARTITION BY c.brand, c.publisherid, c.socialnetwork, c.socialnetworkid ORDER BY date),
                    lead(t.female)
                         IGNORE NULLS OVER (PARTITION BY c.brand, c.publisherid, c.socialnetwork, c.socialnetworkid ORDER BY date)) AS publisher_pct_female,
           network_pct_female,
           coalesce(t.percent_us, lag(t.percent_us)
                                      IGNORE NULLS OVER (PARTITION BY c.brand, c.publisherid, c.socialnetwork, c.socialnetworkid ORDER BY date),
                    lead(t.percent_us)
                         IGNORE NULLS OVER (PARTITION BY c.brand, c.publisherid, c.socialnetwork, c.socialnetworkid ORDER BY date)) AS publisher_pct_us,
           network_pct_us
    FROM _combos c
         LEFT JOIN _audience t ON c.date = t.ciq_date AND c.brand = t.brand AND c.publisherid = t.publisherid AND
                                  c.socialnetwork = t.network AND c.socialnetworkid = t.socialnetworkid AND
                                  t.last_record_of_day = 1
         LEFT JOIN _avgs a ON c.brand = a.brand AND c.socialnetwork = a.network
    ORDER BY c.publisherid, c.socialnetwork, socialnetworkid, date;



CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.influencers.campaign_activity_modeled AS
    SELECT m.date,
           m.brand,
           m.publisherid,
           m.publishername,
           postid,
           posttype,
           ig_story_frame_sequence,
           authenticated,
           m.socialnetwork,
           m.socialnetworkid,
           campaignname,
           datesubmitted,
           days_after_post,
           days_since_post,
           hours_since_post,
           m.ciq_date,
           cum_impressions,
           cum_engagements,
           cum_likes,
           cum_comments,
           cum_shares,
           cum_saves,
           cum_views,
           cum_replies,
           cum_reach,
           cum_tapsback,
           cum_earnedmedia,
           raw_inc_impressions,
           raw_inc_engagements,
           raw_inc_likes,
           raw_inc_comments,
           raw_inc_shares,
           raw_inc_saves,
           raw_inc_views,
           raw_inc_replies,
           raw_inc_reach,
           raw_inc_tapsback,
           raw_inc_earnedmedia,

--        total_impressions, total_engagements, total_likes, total_comments, total_shares, total_saves,
--        next_ciq_record,
--        avg_pct_impressions, avg_pct_engagements, avg_pct_likes, avg_pct_comments, avg_pct_shares, avg_pct_saves,
--        pct_impressions_within_group, impressions_within_group,
           modeled_inc_impressions,
           modeled_inc_engagements,
           modeled_inc_likes,
           modeled_inc_comments,
           modeled_inc_shares,
           modeled_inc_saves,
           modeled_inc_views,
           modeled_inc_replies,
           modeled_inc_reach,
           modeled_inc_tapsback,
           modeled_inc_earnedmedia,
           a.publisher_pct_female,
           a.network_pct_female,
           a.publisher_pct_us,
           a.network_pct_us,
           current_timestamp() AS last_updated
    FROM _modeled m
         LEFT JOIN _audience_metrics a ON m.date = a.date AND m.brand = a.brand AND m.publisherid = a.publisherid AND
                                          m.socialnetwork = a.socialnetwork AND m.socialnetworkid = a.socialnetworkid
    ORDER BY postid, date;



