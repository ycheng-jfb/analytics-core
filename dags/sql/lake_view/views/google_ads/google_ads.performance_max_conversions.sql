CREATE OR REPLACE VIEW lake_view.google_ads.performance_max_conversion_by_campaign AS
SELECT _fivetran_id,
       customer_id,
       date,
       start_date,
  	   end_date,
       id as campaign_id,
  	   advertising_channel_type,
  	   conversion_action_name,
  	   conversions_value,
  	   conversion_action_category,
   	   conversion_action,
  	   view_through_conversions,
       conversions,
  	   all_conversions,
  	   all_conversions_value,
  	   convert_timezone('America/Los_Angeles', _fivetran_synced) as meta_update_datetime
FROM LAKE_FIVETRAN.med_google_ads_v1.performance_max_conversions;
