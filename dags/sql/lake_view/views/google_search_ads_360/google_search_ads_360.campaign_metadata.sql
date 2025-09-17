CREATE OR REPLACE VIEW LAKE_VIEW.GOOGLE_SEARCH_ADS_360.CAMPAIGN_METADATA AS
SELECT customer_descriptive_name,
       campaign_id,
       campaign_name,
       campaign_labels,
       customer_id,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_google_sa_360_v1.campaign_metadata;
