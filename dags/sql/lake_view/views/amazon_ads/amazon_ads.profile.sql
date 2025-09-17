CREATE OR REPLACE VIEW lake_view.amazon_ads.profile
AS
SELECT id,
       country_code,
       currency_code,
       daily_budget,
       timezone,
       account_marketplace_string_id,
       account_id,
       account_type,
       account_name,
       account_sub_type,
       account_valid_payment_method,
       _fivetran_deleted,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_ads_v1.profile;
