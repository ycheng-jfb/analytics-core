CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_COLLECTION_APPAREL_TRACKER as (
    select
        BUSINESS_UNIT,
        PRODUCT_SKU,
        COLLECTION,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_COLLECTION_APPAREL_TRACKER_SHEET_1
);
