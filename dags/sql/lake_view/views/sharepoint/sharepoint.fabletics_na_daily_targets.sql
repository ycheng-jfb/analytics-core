CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.FABLETICS_NA_DAILY_TARGETS as
SELECT date::date AS date
	,vips
	,is_fl_mens_flag
	,cac
	,spend
	,store_brand
	,is_fl_scrubs_flag
	,store_region
	,meta_create_datetime
	,meta_update_datetime
FROM (
	SELECT DATE
		,vips
		,is_fl_mens_flag
		,cac
		,spend
		,store_brand
		,is_fl_scrubs_flag
		,store_region
		,convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
		,convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
	FROM lake_fivetran.med_sharepoint_acquisition_v1.fabletics_na_daily_targets_flna_m

	UNION

	SELECT DATE
		,vips
		,is_fl_mens_flag
		,cac
		,spend
		,store_brand
		,is_fl_scrubs_flag
		,store_region
		,convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
		,convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
	FROM lake_fivetran.med_sharepoint_acquisition_v1.fabletics_na_daily_targets_flna_w

	UNION

	SELECT DATE
		,vips
		,is_fl_mens_flag
		,cac
		,spend
		,store_brand
		,is_fl_scrubs_flag
		,store_region
		,convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
		,convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
	FROM lake_fivetran.med_sharepoint_acquisition_v1.fabletics_na_daily_targets_flna_scb

	UNION

	SELECT DATE
		,vips
		,is_fl_mens_flag
		,cac
		,spend
		,store_brand
		,is_fl_scrubs_flag
		,store_region
		,convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
		,convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
	FROM lake_fivetran.med_sharepoint_acquisition_v1.fabletics_na_daily_targets_ytyna
	);
