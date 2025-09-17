CREATE OR REPLACE VIEW lake_view.amazon_ads.portfolio_history
AS
SELECT id,
       last_updated_date,
       profile_id,
       name,
       in_budget,
       state,
       creation_date,
       serving_status,
       budget_amount,
       budget_currency_code,
       budget_policy,
       budget_start_date,
       budget_end_date,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_ads_v1.portfolio_history;
