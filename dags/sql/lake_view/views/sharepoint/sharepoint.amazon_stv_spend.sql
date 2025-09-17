CREATE OR REPLACE VIEW lake_view.sharepoint.amazon_stv_spend AS
SELECT business_unit,
       country,
       store_id,
       mens_flag,
       scrubs_flag,
       channel,
       subchannel,
       vendor,
       date,
       spend,
       impressions,
       clicks,
       CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _fivetran_synced::TIMESTAMP_NTZ) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_spend_v1.amazon_spend_flm_stv
UNION ALL
SELECT business_unit,
       country,
       store_id,
       mens_flag,
       scrubs_flag,
       channel,
       subchannel,
       vendor,
       date,
       spend,
       impressions,
       clicks,
       CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _fivetran_synced::TIMESTAMP_NTZ) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_spend_v1.amazon_spend_flw_stv;
