CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_FACEBOOK_CONVERSION_MAPPING AS
SELECT ACCOUNT_ID
	,CUSTOM_CONVERSION_ID::varchar as CUSTOM_CONVERSION_ID
	,MAP_TO_COLUMN
	,FIRST_DAY_USED::DATE AS FIRST_DAY_USED
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_admin_v1.media_spend_management_fb_conversion;
