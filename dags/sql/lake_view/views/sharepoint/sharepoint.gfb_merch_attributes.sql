CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_MERCH_ATTRIBUTES as (
    select
        BRAND as BUSINESS_UNIT,
        DEPARTMENT,
        PRODUCT_SKU,
        LAUNCH_DATE::DATE as LAUNCH_DATE,
        SHARED_BRAND,
        COLLECTION,
        STYLE_TYPE,
        SEASON_CODE,
        REORDER_STATUS,
        NA_OG_VIP_PRICE,
        EU_OG_VIP_PRICE,
        EU_EXCLUSIVE,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_MERCH_ATTRIBUTES_JF_APPAREL

    UNION

    select
        BRAND as BUSINESS_UNIT,
        DEPARTMENT,
        PRODUCT_SKU,
        LAUNCH_DATE::DATE as LAUNCH_DATE,
        SHARED_BRAND,
        COLLECTION,
        STYLE_TYPE,
        SEASON_CODE,
        REORDER_STATUS,
        NA_OG_VIP_PRICE,
        EU_OG_VIP_PRICE,
        EU_EXCLUSIVE,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_MERCH_ATTRIBUTES_SD_APPAREL
);
