CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SXF_FABRICATION_KEY AS
SELECT COLLECTION
	,FABRICATION
    ,STYLE_NUMBER
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_acquisition_v1.style_master_merch_inputs_fabrication_key_new_;
