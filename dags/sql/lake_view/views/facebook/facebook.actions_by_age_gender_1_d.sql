CREATE OR REPLACE VIEW IF NOT EXISTS LAKE_VIEW.FACEBOOK.ACTIONS_BY_AGE_GENDER_1_D (
   account_id,
	account_name,
	age,
	gender,
	ad_id,
	date,
	actions_value,
	actions_action_type,
	actions_1d_view,
	actions_1d_click,
	conversions_value,
	conversions_action_type,
	conversions_1d_view,
	conversions_1d_click,
	conversion_values_value,
	conversion_values_action_type,
	conversion_values_1d_view,
	conversion_values_1d_click,
	meta_create_datetime,
	meta_update_datetime
) AS
SELECT account_id,
    account_name,
    a.age,
    a.gender,
    a.ad_id ad_id,
    a.date AS date,
    b.value AS actions_value,
    b.action_type AS actions_action_type,
    b._1_d_view AS actions_1d_view,
    b._1_d_click AS actions_1d_click,
    c.value AS conversions_value,
    c.action_type AS conversions_action_type,
    c._1_d_view AS conversions_1d_view,
    c._1_d_click AS conversions_1d_click,
    d.value AS conversion_values_value,
    d.action_type AS conversion_values_action_type,
    d._1_d_view AS conversion_values_1d_view,
    d._1_d_click AS conversion_values_1d_click,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_create_datetime,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_8hr_v1.actions_by_age_gender_1_d_actions b
FULL JOIN lake_fivetran.med_facebook_ads_8hr_v1.actions_by_age_gender_1_d_conversions c
    ON b.ad_id=c.ad_id AND b.date=c.date AND b._fivetran_id=c._fivetran_id AND b.action_type=c.action_type
FULL JOIN lake_fivetran.med_facebook_ads_8hr_v1.actions_by_age_gender_1_d_conversion_values d
    ON b.ad_id=d.ad_id AND b.date=d.date AND b._fivetran_id=d._fivetran_id AND b.action_type=d.action_type
FULL JOIN lake_fivetran.med_facebook_ads_8hr_v1.actions_by_age_gender_1_d a
    ON a.ad_id=coalesce(b.ad_id,c.ad_id,d.ad_id) AND a.date=coalesce(b.date,c.date,d.date)
        AND a._fivetran_id=coalesce(b._fivetran_id,c._fivetran_id,d._fivetran_id);
