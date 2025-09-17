CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.SXF_PLANNING_FORECAST_DAILY AS
SELECT DATE,
       FCT_ACT_PRODUCT_GROSS_REV,
       FCT_NONACT_PRODUCT_GROSS_REV,
       FCT_TTL_PRODUCT_GROSS_REV,
       TY_PROMOS,
       LY_DATE,
       LY_PROMOS,
       FCT_SESSIONS,
       FCT_ORDERS,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.savagex_inbound_sharepoint_v1.planning_forecast_daily
where _line > 0;
