CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SXF_CORE_FASHION AS
SELECT COLOR_SKU_PO
	,SUB_DEPARTMENT
    ,COLLECTION
    ,CATEGORY
    ,STYLE_NUMBER_PO
    ,SIZE_RANGE
    ,STYLE_NAME
    ,PO_COLOR
    ,SITE_COLOR
    ,CORE_FASHION
    ,NEW_CORE_FASHION
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_acquisition_v1.style_master_merch_inputs_core_fashion_new_;
