CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_CLEARANCE_PROMO as (
    select
        BUSINESS_UNIT,
        REGION,
        COUNTRY,
        PROMO_CODE,
        START_DATE::DATE as START_DATE,
        END_DATE::DATE as END_DATE,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_CLEARANCE_PROMO_SHEET_1
);
