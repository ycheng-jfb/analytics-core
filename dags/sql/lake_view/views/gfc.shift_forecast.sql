CREATE OR REPLACE VIEW lake_view.gfc.shift_forecast AS
SELECT
    forecast_date,
    fc_code,
    shift_label,
    units,
    sla_day,
    meta_create_datetime,
    meta_update_datetime
FROM lake_view.sharepoint.shift_forecast;
