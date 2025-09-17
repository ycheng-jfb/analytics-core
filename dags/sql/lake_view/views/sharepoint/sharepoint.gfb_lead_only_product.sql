CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_LEAD_ONLY_PRODUCT as (
    select
        STORE as BRAND,
        REGION,
        PRODUCT_SKU,
        CATEGORY,
        STYLE_NAME,
        COLOR,
        DATE_ADDED::DATE as DATE_ADDED,
        DATE_REMOVED::DATE as DATE_REMOVED,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_LEAD_ONLY_PRODUCT_LEAD_ONLY
);
