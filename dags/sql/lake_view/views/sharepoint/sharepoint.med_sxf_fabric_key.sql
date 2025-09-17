CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SXF_FABRIC_KEY AS
SELECT FABRIC_PLM
	,FABRIC_MERCH_NAME
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_acquisition_v1.style_master_merch_inputs_fabric_key;
