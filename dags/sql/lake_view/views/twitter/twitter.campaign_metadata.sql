CREATE VIEW if NOT EXISTS LAKE_VIEW.TWITTER.CAMPAIGN_METADATA AS
SELECT id AS campaign_id,
       name AS campaign_name,
       account_id,
       budget_optimization,
       entity_status,
       deleted,
       updated_at AS updated_datetime,
       created_at AS created_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_twitter_ads_v1.campaign_history qualify row_number() OVER (
                PARTITION BY id ORDER BY UPDATED_AT DESC
                ) = 1;
