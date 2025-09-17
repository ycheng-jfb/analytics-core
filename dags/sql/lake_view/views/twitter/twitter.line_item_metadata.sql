CREATE VIEW if NOT EXISTS LAKE_VIEW.TWITTER.LINE_ITEM_METADATA AS
SELECT id AS line_item_id,
       name AS line_item_name,
       campaign_id,
       advertiser_user_id,
       product_type,
       objective,
       entity_status,
       deleted,
       start_time AS start_datetime,
       created_at AS created_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_twitter_ads_v1.line_item_history qualify row_number() OVER (
                PARTITION BY id ORDER BY UPDATED_AT DESC
                ) = 1;
