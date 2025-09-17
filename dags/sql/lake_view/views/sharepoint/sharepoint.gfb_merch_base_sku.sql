CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_MERCH_BASE_SKU as (
    select
        BUSINESS_UNIT,
        REGION,
        PRODUCT_SKU,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_MERCH_BASE_SKU_SHEET_1
);
