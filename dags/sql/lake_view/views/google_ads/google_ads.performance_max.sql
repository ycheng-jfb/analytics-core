CREATE OR REPLACE VIEW lake_view.google_ads.performance_max_spend_by_campaign AS
SELECT _fivetran_id,
       customer_id,
       date,
       start_date,
       end_date,
       advertising_channel_type,
       id as campaign_id,
       cost_micros,
       clicks,
       impressions,
	   convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM LAKE_FIVETRAN.med_google_ads_v1.performance_max;
