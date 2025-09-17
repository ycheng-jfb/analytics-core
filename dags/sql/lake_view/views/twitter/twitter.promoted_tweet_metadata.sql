CREATE VIEW if NOT EXISTS LAKE_VIEW.TWITTER.PROMOTED_TWEET_METADATA AS
SELECT id AS promoted_tweet_id,
       line_item_id,
       tweet_id,
       created_at AS created_datetime,
       updated_at AS updated_datetime,
       deleted,
       approval_status,
       entity_status,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_twitter_ads_v1.promoted_tweet_history qualify row_number() OVER (
                PARTITION BY id ORDER BY UPDATED_AT DESC
                ) = 1;
