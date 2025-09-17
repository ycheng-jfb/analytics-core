CREATE OR REPLACE VIEW IF NOT EXISTS LAKE_VIEW.FACEBOOK.CAMPAIGN_METADATA (
	campaign_id,
	campaign_name,
	account_id,
	effective_status,
	meta_create_datetime,
	meta_update_datetime
) AS
SELECT id AS campaign_id,
	name AS campaign_name,
	account_id,
	effective_status,
	convert_timezone('America/Los_Angeles', _FIVETRAN_SYNCED) AS meta_create_datetime,
	convert_timezone('America/Los_Angeles', UPDATED_TIME) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_8hr_v1.campaign_history qualify row_number() OVER (
		PARTITION BY id ORDER BY UPDATED_TIME DESC
		) = 1;
