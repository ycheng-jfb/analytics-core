CREATE OR REPLACE VIEW IF NOT EXISTS LAKE_VIEW.FACEBOOK.AD_CREATIVE (
	AD_ID,
	AD_NAME,
	ADSET_ID,
	CAMPAIGN_ID,
	ACCOUNT_ID,
	EFFECTIVE_STATUS,
	CREATIVE_ID,
	UPDATED_TIME
) as
SELECT id AS ad_id,
	name AS ad_name,
	ad_set_id AS adset_id,
	campaign_id,
	account_id,
	effective_status,
    creative_id,
    convert_timezone('America/Los_Angeles',UPDATED_TIME) AS UPDATED_TIME
FROM lake_fivetran.med_facebook_ads_8hr_v1.ad_history qualify row_number() OVER (
		PARTITION BY id ORDER BY UPDATED_TIME DESC
		) = 1;
