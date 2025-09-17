CREATE OR REPLACE VIEW IF NOT EXISTS LAKE_VIEW.FACEBOOK.ACTIONS_BY_HOUR_7_D (
	account_id,
	account_name,
	ad_id,
	date,
	actions_value,
	actions_action_type,
	actions_7d_view,
	actions_7d_click,
	conversions_value,
	conversions_action_type,
	conversions_7d_view,
	conversions_7d_click,
	conversion_values_value,
	conversion_values_action_type,
	conversion_values_7d_view,
	conversion_values_7d_click,
	actions_values_value,
	actions_values_action_type,
	actions_values_7d_view,
	actions_values_7d_click,
	hourly_stats_aggregated_by_advertiser_time_zone,
	meta_create_datetime,
	meta_update_datetime
) AS

SELECT account_id,
    account_name,
    a.ad_id,
    a.date,
    b.value AS actions_value,
    b.action_type AS actions_action_type,
    b._7_d_view AS actions_7d_view,
    b._7_d_click AS actions_7d_click,
    c.value AS conversions_value,
    c.action_type AS conversions_action_type,
    c._7_d_view AS conversions_7d_view,
    c._7_d_click AS conversions_7d_click,
    d.value AS conversion_values_value,
    d.action_type AS conversion_values_action_type,
    d._7_d_view AS conversion_values_7d_view,
    d._7_d_click AS conversion_values_7d_click,
    e.value AS actions_values_value,
    e.action_type AS actions_values_action_type,
    e._7_d_view AS actions_values_7d_view,
    e._7_d_click AS actions_values_7d_click,
    a.hourly_stats_aggregated_by_advertiser_time_zone,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_create_datetime,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_8hr_v1.actions_by_hour_7_d_actions b
FULL JOIN lake_fivetran.med_facebook_ads_8hr_v1.actions_by_hour_7_d_conversions c
    ON b.ad_id=c.ad_id AND b.date=c.date AND b._fivetran_id=c._fivetran_id AND b.action_type=c.action_type
FULL JOIN lake_fivetran.med_facebook_ads_8hr_v1.actions_by_hour_7_d_conversion_values d
    ON b.ad_id=d.ad_id AND b.date=d.date AND b._fivetran_id=d._fivetran_id AND b.action_type=d.action_type
FULL JOIN lake_fivetran.med_facebook_ads_8hr_v1.actions_by_hour_7_d_action_values e
    ON b.ad_id=e.ad_id AND b.date=e.date AND b._fivetran_id=e._fivetran_id AND b.action_type=e.action_type
FULL JOIN lake_fivetran.med_facebook_ads_8hr_v1.actions_by_hour_7_d a
    ON a.ad_id=coalesce(b.ad_id,c.ad_id,d.ad_id,e.ad_id) AND a.date=coalesce(b.date,c.date,d.date,e.date) AND
        a._fivetran_id=coalesce(b._fivetran_id,c._fivetran_id,d._fivetran_id,e._fivetran_id);
