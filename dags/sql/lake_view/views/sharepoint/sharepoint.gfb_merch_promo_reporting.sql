CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_MERCH_PROMO_REPORTING as (
    select
        CAMPAIGN_NAME,
        PRODUCT_SKU,
        BUSINESS_UNIT,
        START_DATE::DATE as START_DATE,
        END_DATE::DATE as END_DATE,
        PROMO_CODE,
        LEVEL,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_MERCH_PROMO_REPORTING_SHEET_1
);
