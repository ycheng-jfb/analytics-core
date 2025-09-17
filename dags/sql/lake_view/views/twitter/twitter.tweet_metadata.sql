CREATE VIEW if NOT EXISTS LAKE_VIEW.TWITTER.TWEET_METADATA AS
SELECT id AS tweet_id,
       name,
       account_id,
       to_date(created_at) AS created_date,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_twitter_ads_v1.tweet;
