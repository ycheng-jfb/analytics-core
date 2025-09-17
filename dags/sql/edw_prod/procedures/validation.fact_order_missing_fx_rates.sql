TRUNCATE TABLE validation.fact_order_missing_fx_rates;

CREATE OR REPLACE TEMP TABLE _fx_rates_count AS
SELECT SUM(CASE WHEN order_date_usd_conversion_rate IS NULL THEN 1 ELSE 0 END) AS null_order_date_usd_conversion_rate_count,
       SUM(CASE WHEN order_date_eur_conversion_rate IS NULL THEN 1 ELSE 0 END) AS null_order_date_eur_conversion_rate_count,
       SUM(CASE WHEN reporting_usd_conversion_rate IS NULL THEN 1 ELSE 0 END)  AS null_reporting_usd_conversion_rate_count,
       SUM(CASE WHEN reporting_eur_conversion_rate IS NULL THEN 1 ELSE 0 END)  AS null_reporting_eur_conversion_rate_count
FROM stg.fact_order
WHERE order_placed_local_datetime::DATE >= '2012-01-01';

INSERT INTO validation.fact_order_missing_fx_rates
SELECT *
FROM _fx_rates_count
WHERE null_order_date_usd_conversion_rate_count > 0
   OR null_order_date_eur_conversion_rate_count > 0
   OR null_reporting_usd_conversion_rate_count > 0
   OR null_reporting_eur_conversion_rate_count > 0;
