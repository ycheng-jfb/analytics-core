CREATE VIEW IF NOT EXISTS LAKE_VIEW.TWITTER.ACCOUNT_METADATA AS
SELECT id AS account_id,
       name AS account_name,
       approval_status,
       deleted,
       updated_at AS updated_datetime,
       created_at AS created_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_twitter_ads_v1.account_history qualify row_number() OVER (
                PARTITION BY id ORDER BY UPDATED_AT DESC
                ) = 1;
