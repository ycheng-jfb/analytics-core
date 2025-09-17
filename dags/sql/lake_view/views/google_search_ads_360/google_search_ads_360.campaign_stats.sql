CREATE OR REPLACE VIEW LAKE_VIEW.GOOGLE_SEARCH_ADS_360.CAMPAIGN_STATS AS
SELECT segments_date AS date,
       customer_id,
       campaign_id,
       campaign_name,
       customer_descriptive_name,
       metrics_cost_micros AS cost_micros,
       metrics_clicks AS clicks,
       metrics_impressions AS impressions,
       metrics_ctr AS ctr,
       metrics_visits AS visits,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_google_sa_360_v1.campaign_stats;
