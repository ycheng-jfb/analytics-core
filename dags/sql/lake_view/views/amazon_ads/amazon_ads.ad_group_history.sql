CREATE OR REPLACE VIEW lake_view.amazon_ads.ad_group_history
AS
SELECT id,
       last_updated_date,
       serving_status,
       creation_date,
       campaign_id,
       name,
       default_bid,
       state,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_ads_v1.ad_group_history;
