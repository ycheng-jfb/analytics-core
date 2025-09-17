CREATE OR REPLACE TEMPORARY TABLE _calc1 AS
SELECT
        billing_month,
        storefront,
        ifnull(m1_new_vips, 0) as m1_new_vips,
        ifnull(first_successful_new_vips,0) as first_successful_new_vips,
        ifnull(m1_grace_vips,0) as m1_grace_vips,
        ifnull(first_successful_grace_vips,0) as first_successful_grace_vips,
        ifnull(first_successful_billed_credit_rate,0) as first_successful_billed_credit_rate,
        ifnull(first_successful_billed_credit_rate_trailing,0) as first_successful_billed_credit_rate_trailing,
        ifnull(retry_rate,0) as retry_rate,
        ifnull(retry_rate_trailing_6_months,0) as retry_rate_trailing_6_months,
        ifnull(m2_tenure_vips,0) as m2_tenure_vips,
        ifnull(m2_plus_vips,0) as m2_plus_vips,
        ifnull(fpa_m2_credit_billings,0) as fpa_m2_credit_billings,
        ifnull(fpa_m2_plus_credit_billings,0) as fpa_m2_plus_credit_billings,
        ifnull(m2_billing_credit_rate_fpa,0) as m2_billings_credit_rate_fpa,
        ifnull(m2_plus_billing_rate,0) as m2_plus_billing_rate,
        ifnull(total_billing_fpa_forecast,0) as total_billing_fpa_forecast
FROM reporting_prod.gms.bos_boss_forecast;

CREATE OR REPLACE TEMPORARY TABLE _calc2 AS
SELECT CASE
        WHEN storefront IN ('FL US', 'FL CA') THEN (total_billing_fpa_forecast-(total_billing_fpa_forecast*retry_rate))-(first_successful_new_vips+first_successful_grace_vips)
        WHEN storefront NOT IN ('FL US', 'FL CA') THEN fpa_m2_plus_credit_billings-(fpa_m2_plus_credit_billings*retry_rate) ELSE 0
        END AS first_successful_default,
        total_billing_fpa_forecast * retry_rate as fpa_forecast_retries,
        total_billing_fpa_forecast - (total_billing_fpa_forecast*retry_rate) as fpa_forecast_first_time,
        *
FROM _calc1;

BEGIN;

DELETE FROM reporting_prod.gms.bos_raw_data;

MERGE INTO reporting_prod.gms.bos_raw_data t
    USING (SELECT *
                FROM (select *,
                        HASH(*) as meta_row_hash,
                        row_number() OVER (PARTITION BY billing_month,storefront order by NULL) AS rn
                        FROM _calc2)
                where rn = 1) s
        ON equal_null(t.billing_month, s.billing_month)
        AND equal_null(t.storefront, s.storefront)
    WHEN NOT MATCHED THEN INSERT
        (first_successful_default,fpa_forecast_retries,fpa_forecast_first_time,billing_month,storefront,m1_new_vips,
        first_successful_new_vips,m1_grace_vips,first_successful_grace_vips,first_successful_billed_credit_rate,first_successful_billed_credit_rate_trailing,
        retry_rate,retry_rate_trailing_6_months,m2_tenure_vips,m2_plus_vips,fpa_m2_credit_billings,fpa_m2_plus_credit_billings,m2_billings_credit_rate_fpa,
        m2_plus_billing_rate,total_billing_fpa_forecast, meta_row_hash)
        VALUES  (first_successful_default,fpa_forecast_retries,fpa_forecast_first_time,billing_month,storefront,m1_new_vips,
        first_successful_new_vips,m1_grace_vips,first_successful_grace_vips,first_successful_billed_credit_rate,first_successful_billed_credit_rate_trailing,
        retry_rate,retry_rate_trailing_6_months,m2_tenure_vips,m2_plus_vips,fpa_m2_credit_billings,fpa_m2_plus_credit_billings,m2_billings_credit_rate_fpa,
        m2_plus_billing_rate,total_billing_fpa_forecast, meta_row_hash)
    WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        --AND coalesce(s.segment_end, '1900-01-01') > coalesce(t.segment_end, '1900-01-01')
        THEN
        UPDATE
            SET t.first_successful_default =s.first_successful_default
                ,t.fpa_forecast_retries = s.fpa_forecast_retries
                ,t.fpa_forecast_first_time = s.fpa_forecast_first_time
                ,t.m1_new_vips = s.m1_new_vips
                ,t.first_successful_new_vips = s.first_successful_new_vips
                ,t.m1_grace_vips = s.m1_grace_vips
                ,t.first_successful_grace_vips = s.first_successful_grace_vips
                ,t.first_successful_billed_credit_rate = s.first_successful_billed_credit_rate
                ,t.first_successful_billed_credit_rate_trailing = s.first_successful_billed_credit_rate_trailing
                ,t.retry_rate = s.retry_rate
                ,t.retry_rate_trailing_6_months = s.retry_rate_trailing_6_months
                ,t.m2_tenure_vips = s.m2_tenure_vips
                ,t.m2_plus_vips = s.m2_plus_vips
                ,t.fpa_m2_credit_billings = s.fpa_m2_credit_billings
                ,t.fpa_m2_plus_credit_billings = s.fpa_m2_plus_credit_billings
                ,t.m2_billings_credit_rate_fpa = s.m2_billings_credit_rate_fpa
                ,t.m2_plus_billing_rate = s.m2_plus_billing_rate
                ,t.total_billing_fpa_forecast = s.total_billing_fpa_forecast
                ,t.meta_row_hash = s.meta_row_hash
                ,t.meta_update_datetime = CURRENT_TIMESTAMP;

COMMIT;
