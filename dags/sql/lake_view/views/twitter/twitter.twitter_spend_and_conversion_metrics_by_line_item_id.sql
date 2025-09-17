CREATE VIEW if NOT EXISTS LAKE_VIEW.TWITTER.TWITTER_SPEND_AND_CONVERSION_METRICS_BY_LINE_ITEM_ID AS
SELECT account_id,
       line_item_id,
       CONVERT_TIMEZONE('America/Los_Angeles', DATE)::DATE DATE,
       SUM(billed_charge_local_micro/1000000) AS spend,
       SUM(impressions) AS impressions,
       SUM(engagements) AS engagements,
       SUM(carousel_swipes) AS carousel_swipes,
       SUM(tweets_send) AS tweet_send,
       SUM(video_total_views) AS total_views,
       SUM(media_views) AS media_views,
       SUM(video_cta_clicks) AS video_cta_clicks,
       SUM(likes) AS likes,
       SUM(retweets) AS retweets,
       sum(clicks) as clicks,
       sum(url_clicks) as link_clicks,
       SUM(conversion_site_visits_metric) AS site_visits,
       SUM(conversion_sign_ups_metric) AS leads,
       SUM(conversion_purchases_metric) AS vips
FROM lake_fivetran.med_twitter_ads_v1.line_item_report
GROUP BY 1, 2, 3;
