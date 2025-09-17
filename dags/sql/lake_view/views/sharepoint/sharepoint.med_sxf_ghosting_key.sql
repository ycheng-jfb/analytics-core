CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SXF_GHOSTING_KEY AS
SELECT COLOR_SKU
	,GHOST_START_DATE::DATE AS GHOST_START_DATE
	,GHOST_END_DATE::DATE AS GHOST_END_DATE
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_acquisition_v1.style_master_merch_inputs_ghosted_skus;
