CREATE OR REPLACE VIEW lake_view.sharepoint.shift_forecast (
    forecast_date,
    fc_code,
    shift_label,
    units,
    sla_day,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    forecast_date,
    fc_code,
    shift_label,
    units,
    sla_day,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM (
    SELECT
        forecast_date::date AS forecast_date,
        fc_code,
        shift_label,
        REPLACE(units, ',', '')::number(38, 0) AS units,
        sla_day,
        _fivetran_synced::timestamp AS meta_create_datetime,
        _fivetran_synced::timestamp AS meta_update_datetime,
        HASH(
            forecast_date::date,
            fc_code,
            shift_label,
            REPLACE(units, ',', '')::number(38, 0),
            sla_day
        ) AS meta_row_hash,
        ROW_NUMBER() OVER(
            PARTITION BY forecast_date::date, fc_code, shift_label
            ORDER BY null DESC
        ) AS rn
    FROM lake_fivetran.global_apps_inbound_sharepoint_v1.fc_shift_forecast_sheet_1
) WHERE rn = 1;
