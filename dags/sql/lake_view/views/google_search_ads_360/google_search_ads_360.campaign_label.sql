CREATE OR REPLACE VIEW LAKE_VIEW.GOOGLE_SEARCH_ADS_360.CAMPAIGN_LABEL AS
SELECT customer_id,
       campaign_id,
       label_name,
       label_id,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_google_sa_360_v1.campaign_label;
