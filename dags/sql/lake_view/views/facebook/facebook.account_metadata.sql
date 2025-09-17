CREATE OR REPLACE VIEW IF NOT EXISTS LAKE_VIEW.FACEBOOK.ACCOUNT_METADATA (
    account_id,
	account_name,
	business_id,
	meta_create_datetime,
	meta_update_datetime
) as
SELECT id AS account_id,
    name AS account_name,
    business_manager_manager_id AS business_id,
    convert_timezone('America/Los_Angeles', _FIVETRAN_SYNCED) AS meta_create_datetime,
    convert_timezone('America/Los_Angeles', _FIVETRAN_SYNCED) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_8hr_v1.account_history;
