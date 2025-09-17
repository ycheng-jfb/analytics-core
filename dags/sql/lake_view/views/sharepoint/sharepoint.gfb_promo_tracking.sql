CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_PROMO_TRACKING as (
    select
        BRAND,
        REGION,
        DATE::DATE as DATE,
        PROSPECTING_PROMO,
        AGED_LEAD_PROMO,
        REPEAT_PROMO,
        MISC,
        HOLIDAY,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_PROMO_TRACKING_SHEET_1
);
