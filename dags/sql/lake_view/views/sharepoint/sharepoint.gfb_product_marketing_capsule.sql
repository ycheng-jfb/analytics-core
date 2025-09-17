CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_PRODUCT_MARKETING_CAPSULE as (
    select
        BRAND,
        REGION,
        PRODUCT_SKU,
        MARKETING_CAPSULE_NAME,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_PRODUCT_MARKETING_CAPSULE_SHEET_1
);
