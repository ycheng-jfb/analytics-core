CREATE OR REPLACE VIEW LAKE_VIEW.GOOGLE_SEARCH_ADS_360.CAMPAIGN_CONVERSIONS AS
SELECT segments_date AS date,
       customer_id,
       campaign_id,
       segments_conversion_action_name AS conversion_action_name,
       metrics_all_conversions AS all_conversions,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_google_sa_360_v1.campaign_conversions;
