CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_PEAK_FORECAST_DAILY as (
    select
        DATE::DATE as DATE,
        DAY_OF_WEEK,
        BUSINESS_UNIT,
        REGION,
        ACTIVATING_REPEAT_TOTAL as ORDER_TYPE,
        TY_PROMO_DESCRIPTION,
        LY_PROMO_DESCRIPTION,
        ORDERS::FLOAT as ORDERS,
        UPT,
        UNITS::FLOAT as UNITS,
        AUR,
        AUC,
        GM_ as GM_PERCENT,
        AIR,
        AOV,
        AOV_W_SHIPPING as AOV_INCLUDE_SHIPPING,
        GAAP_REVENUE,
        GAAP_GM,
        GAAP_GM_ as GAAP_GM_PERCENT,
        DISCOUNT as DISCOUNT_RATE,
        CASH_COLLECTED,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_CONFIDENTIAL_SHAREPOINT_V1.GFB_PEAK_FORECAST_DAILY_DATA_FRIENDLY_FORECAST
);
