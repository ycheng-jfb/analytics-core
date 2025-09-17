CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_ACCOUNT_MAPPING_MEDIA AS
SELECT ID AS SOURCE_ID
    ,REFERENCE_COLUMN
    ,STORE_ID
    ,SPECIALTY_STORE
    ,CURRENCY
    ,INCLUDE_IN_CPA_REPORT
    ,MENS_ACCOUNT_FLAG
    ,RETAIL_ACCOUNT_FLAG
    ,IS_SCRUBS_FLAG
    ,SOURCE
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.MED_SHAREPOINT_admin_V1.media_account_mapping_master_mapping_table;
