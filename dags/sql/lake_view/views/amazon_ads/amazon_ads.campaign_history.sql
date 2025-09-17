CREATE OR REPLACE VIEW lake_view.amazon_ads.campaign_history
AS
SELECT id,
       last_updated_date,
       serving_status,
       creation_date,
       portfolio_id,
       profile_id,
       name,
       targeting_type,
       state,
       bidding_strategy,
       start_date,
       end_date,
       budget_type,
       budget,
       effective_budget,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_ads_v1.campaign_history;
