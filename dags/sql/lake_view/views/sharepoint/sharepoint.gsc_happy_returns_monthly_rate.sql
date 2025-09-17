CREATE OR REPLACE VIEW lake_view.sharepoint.gsc_happy_returns_monthly_rate as
SELECT month,
       year,
       to_date(date_use_yyyy_01_mm_format_) AS year_month,
       brand_bu as BU,
       rate as monthly_rate,
       to_timestamp_ltz(_fivetran_synced) as meta_update_Datetime
FROM LAKE_FIVETRAN.GLOBAL_APPS_GSC_SECONDARY_INGESTIONS.HAPPY_RETURNS_INVOICES_MONTHLY_RATES
WHERE brand_bu <> 'TSOS'
ORDER BY bu, year_month;

