CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_PROMO_MAPPING as (
    select
        BRAND as BUSINESS_UNIT,
        COUNTRY,
        PROMO_CODE,
        PROMO_MAPPING,
        START_DATE::DATE as START_DATE,
        END_DATE::DATE as END_DATE,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_PROMO_MAPPING_SHEET_1
);
