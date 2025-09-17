CREATE OR REPLACE TEMPORARY TABLE _transform1 AS
SELECT
        storefront,
        billing_month,
        dateadd('month', -1, billing_month) as last_month,
        dateadd('month', -2, billing_month) as two_months_ago,
        dateadd('month', -3, billing_month) as three_months_ago
from reporting_prod.gms.bos_joining_planned_actuals;

CREATE OR REPLACE TEMPORARY TABLE _transform2 AS
select
        t.storefront,
        t.billing_month,
        ifnull(b1.day_1_actuals,0) + ifnull(b2.day_1_actuals,0) + ifnull(b3.day_1_actuals,0) as prev_3_month_day_1_actuals,
        ifnull(b1.day_1_to_be_billed,0) + ifnull(b2.day_1_to_be_billed,0) + ifnull(b3.day_1_to_be_billed,0) as prev_3_month_day_1_to_be_billed,
        ifnull(b1.day_2_actuals,0) + ifnull(b2.day_2_actuals,0) + ifnull(b3.day_2_actuals,0) as prev_3_month_day_2_actuals,
        ifnull(b1.day_2_to_be_billed,0) + ifnull(b2.day_2_to_be_billed,0) + ifnull(b3.day_2_to_be_billed,0) as prev_3_month_day_2_to_be_billed,
        ifnull(b1.day_3_actuals,0) + ifnull(b2.day_3_actuals,0) + ifnull(b3.day_3_actuals,0) as prev_3_month_day_3_actuals,
        ifnull(b1.day_3_to_be_billed,0) + ifnull(b2.day_3_to_be_billed,0) + ifnull(b3.day_3_to_be_billed,0) as prev_3_month_day_3_to_be_billed,
        ifnull(b1.day_4_actuals,0) + ifnull(b2.day_4_actuals,0) + ifnull(b3.day_4_actuals,0) as prev_3_month_day_4_actuals,
        ifnull(b1.day_4_to_be_billed,0) + ifnull(b2.day_4_to_be_billed,0) + ifnull(b3.day_4_to_be_billed,0) as prev_3_month_day_4_to_be_billed,
        ifnull(b1.day_5_actuals,0) + ifnull(b2.day_5_actuals,0) + ifnull(b3.day_5_actuals,0) as prev_3_month_day_5_actuals,
        ifnull(b1.day_5_to_be_billed,0) + ifnull(b2.day_5_to_be_billed,0) + ifnull(b3.day_5_to_be_billed,0) as prev_3_month_day_5_to_be_billed,
        ifnull(b1.day_6_actuals,0) + ifnull(b2.day_6_actuals,0) + ifnull(b3.day_6_actuals,0) as prev_3_month_day_6_actuals,
        ifnull(b1.day_6_to_be_billed,0) + ifnull(b2.day_6_to_be_billed,0) + ifnull(b3.day_6_to_be_billed,0) as prev_3_month_day_6_to_be_billed,
        ifnull(b1.day_7_actuals,0) + ifnull(b2.day_7_actuals,0) + ifnull(b3.day_7_actuals,0) as prev_3_month_day_7_actuals,
        ifnull(b1.day_7_to_be_billed,0) + ifnull(b2.day_7_to_be_billed,0) + ifnull(b3.day_7_to_be_billed,0) as prev_3_month_day_7_to_be_billed,
        ifnull(b1.day_8_actuals,0) + ifnull(b2.day_8_actuals,0) + ifnull(b3.day_8_actuals,0) as prev_3_month_day_8_actuals,
        ifnull(b1.day_8_to_be_billed,0) + ifnull(b2.day_8_to_be_billed,0) + ifnull(b3.day_8_to_be_billed,0) as prev_3_month_day_8_to_be_billed,
        ifnull(b1.day_9_actuals,0) + ifnull(b2.day_9_actuals,0) + ifnull(b3.day_9_actuals,0) as prev_3_month_day_9_actuals,
        ifnull(b1.day_9_to_be_billed,0) + ifnull(b2.day_9_to_be_billed,0) + ifnull(b3.day_9_to_be_billed,0) as prev_3_month_day_9_to_be_billed,
        b1.total_actuals as prev_month_1st_actuals,
        b1.total_retries as prev_month_retries
FROM _transform1 t
JOIN reporting_prod.gms.bos_joining_planned_actuals b1
    ON b1.storefront=t.storefront AND b1.billing_month=t.last_month
JOIN reporting_prod.gms.bos_joining_planned_actuals b2
    ON b2.storefront=t.storefront AND b2.billing_month=t.two_months_ago
JOIN reporting_prod.gms.bos_joining_planned_actuals b3
    ON b3.storefront=t.storefront AND b3.billing_month=t.three_months_ago;

CREATE OR REPLACE TEMPORARY TABLE _transform3 AS
SELECT
        b.curr_date,
        b.billing_month,
        b.storefront,
        b.day_9_planned,
        b.day_9_actuals,
        b.day_9_to_be_billed,
        b.day_9_pa_pct,
        b.day_8_planned,
        b.day_8_actuals,
        b.day_8_to_be_billed,
        b.day_8_pa_pct,
        b.day_7_planned,
        b.day_7_actuals,
        b.day_7_to_be_billed,
        b.day_7_pa_pct ,
        b.day_6_planned ,
        b.day_6_actuals,
        b.day_6_to_be_billed,
        b.day_6_pa_pct ,
        b.day_5_planned,
        b.day_5_actuals,
        b.day_5_to_be_billed,
        b.day_5_pa_pct,
        b.day_4_planned,
        b.day_4_actuals,
        b.day_4_to_be_billed,
        b.day_4_pa_pct,
        b.day_3_planned,
        b.day_3_actuals,
        b.day_3_to_be_billed,
        b.day_3_pa_pct,
        b.day_2_planned,
        b.day_2_actuals,
        b.day_2_to_be_billed,
        b.day_2_pa_pct,
        b.day_1_planned,
        b.day_1_actuals,
        b.day_1_to_be_billed,
        b.day_1_pa_pct,
        b.total_actuals,
        b.total_retries,
        b.total_planned,
        b.total_pa_pct,
        b.total_total_planned,
        b.first_successful_default,
        b.fpa_forecast_retries,
        b.fpa_forecast_first_time,
        b.m1_new_vips,
        b.first_successful_new_vips,
        b.m1_grace_vips,
        b.first_successful_grace_vips,
        b.first_successful_billed_credit_rate,
        b.first_successful_billed_credit_rate_trailing,
        b.retry_rate,
        b.retry_rate_trailing_6_months,
        b.m2_tenure_vips,
        b.m2_plus_vips,
        b.fpa_m2_credit_billings,
        b.fpa_m2_plus_credit_billings,
        b.m2_billings_credit_rate_fpa,
        b.m2_plus_billing_rate,
        b.total_billing_fpa_forecast,
        t.prev_3_month_day_1_actuals,
        prev_3_month_day_1_to_be_billed,
        prev_3_month_day_2_actuals,
        prev_3_month_day_2_to_be_billed,
        prev_3_month_day_3_actuals,
        prev_3_month_day_3_to_be_billed,
        prev_3_month_day_4_actuals,
        prev_3_month_day_4_to_be_billed,
        prev_3_month_day_5_actuals,
        prev_3_month_day_5_to_be_billed,
        prev_3_month_day_6_actuals,
        prev_3_month_day_6_to_be_billed,
        prev_3_month_day_7_actuals,
        prev_3_month_day_7_to_be_billed,
        prev_3_month_day_8_actuals,
        prev_3_month_day_8_to_be_billed,
        prev_3_month_day_9_actuals,
        prev_3_month_day_9_to_be_billed,
        prev_month_1st_actuals,
        prev_month_retries
FROM reporting_prod.gms.bos_joining_planned_actuals b
LEFT JOIN _transform2 t on t.billing_month=b.billing_month and t.storefront=b.storefront;

BEGIN;

DELETE FROM reporting_prod.gms.bos_raw_data_final;

MERGE INTO reporting_prod.gms.bos_raw_data_final t
    USING (SELECT *
                FROM (select *,
                        HASH(*) as meta_row_hash,
                        row_number()
                            OVER (PARTITION BY billing_month,storefront
                                order by NULL
                                 ) AS rn
                        FROM _transform3)
                where rn = 1) s
        ON equal_null(t.billing_month, s.billing_month)
        AND equal_null(t.storefront, s.storefront)
    WHEN NOT MATCHED THEN INSERT
        (curr_date,billing_month,storefront, day_9_planned, day_9_actuals, day_9_to_be_billed, day_9_pa_pct, day_8_planned, day_8_actuals, day_8_to_be_billed, day_8_pa_pct, day_7_planned, day_7_actuals, day_7_to_be_billed, day_7_pa_pct, day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct, day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct, day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct, day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct, day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct, day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct, total_actuals, total_retries, total_planned, total_pa_pct, total_total_planned, first_successful_default, fpa_forecast_retries, fpa_forecast_first_time, m1_new_vips, first_successful_new_vips, m1_grace_vips, first_successful_grace_vips, first_successful_billed_credit_rate, first_successful_billed_credit_rate_trailing, retry_rate, retry_rate_trailing_6_months, m2_tenure_vips, m2_plus_vips, fpa_m2_credit_billings, fpa_m2_plus_credit_billings, m2_billings_credit_rate_fpa, m2_plus_billing_rate, total_billing_fpa_forecast, prev_3_month_day_1_actuals, prev_3_month_day_1_to_be_billed, prev_3_month_day_2_actuals, prev_3_month_day_2_to_be_billed, prev_3_month_day_3_actuals, prev_3_month_day_3_to_be_billed, prev_3_month_day_4_actuals, prev_3_month_day_4_to_be_billed, prev_3_month_day_5_actuals, prev_3_month_day_5_to_be_billed, prev_3_month_day_6_actuals, prev_3_month_day_6_to_be_billed, prev_3_month_day_7_actuals, prev_3_month_day_7_to_be_billed, prev_3_month_day_8_actuals, prev_3_month_day_8_to_be_billed, prev_3_month_day_9_actuals, prev_3_month_day_9_to_be_billed, prev_month_1st_actuals, prev_month_retries, meta_row_hash)
        VALUES  (curr_date, billing_month, storefront, day_9_planned, day_9_actuals, day_9_to_be_billed, day_9_pa_pct, day_8_planned, day_8_actuals, day_8_to_be_billed, day_8_pa_pct, day_7_planned, day_7_actuals, day_7_to_be_billed, day_7_pa_pct, day_6_planned, day_6_actuals, day_6_to_be_billed, day_6_pa_pct, day_5_planned, day_5_actuals, day_5_to_be_billed, day_5_pa_pct, day_4_planned, day_4_actuals, day_4_to_be_billed, day_4_pa_pct, day_3_planned, day_3_actuals, day_3_to_be_billed, day_3_pa_pct, day_2_planned, day_2_actuals, day_2_to_be_billed, day_2_pa_pct, day_1_planned, day_1_actuals, day_1_to_be_billed, day_1_pa_pct, total_actuals, total_retries, total_planned, total_pa_pct, total_total_planned, first_successful_default, fpa_forecast_retries, fpa_forecast_first_time, m1_new_vips, first_successful_new_vips, m1_grace_vips, first_successful_grace_vips, first_successful_billed_credit_rate, first_successful_billed_credit_rate_trailing, retry_rate, retry_rate_trailing_6_months, m2_tenure_vips, m2_plus_vips, fpa_m2_credit_billings, fpa_m2_plus_credit_billings, m2_billings_credit_rate_fpa, m2_plus_billing_rate, total_billing_fpa_forecast, prev_3_month_day_1_actuals, prev_3_month_day_1_to_be_billed, prev_3_month_day_2_actuals, prev_3_month_day_2_to_be_billed, prev_3_month_day_3_actuals, prev_3_month_day_3_to_be_billed, prev_3_month_day_4_actuals, prev_3_month_day_4_to_be_billed, prev_3_month_day_5_actuals, prev_3_month_day_5_to_be_billed, prev_3_month_day_6_actuals, prev_3_month_day_6_to_be_billed, prev_3_month_day_7_actuals, prev_3_month_day_7_to_be_billed, prev_3_month_day_8_actuals, prev_3_month_day_8_to_be_billed, prev_3_month_day_9_actuals, prev_3_month_day_9_to_be_billed, prev_month_1st_actuals, prev_month_retries,meta_row_hash)

        WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        AND coalesce(s.curr_date, '1900-01-01') > coalesce(t.curr_date, '1900-01-01')
        THEN
        UPDATE
            SET t.curr_date = s.curr_date
            ,t.day_9_planned = s.day_9_planned
            ,t.day_9_actuals = s.day_9_actuals
            ,t.day_9_to_be_billed = s.day_9_to_be_billed
            ,t.day_9_pa_pct = s.day_9_pa_pct
            ,t.day_8_planned = s.day_8_planned
            ,t.day_8_actuals = s.day_8_actuals
            ,t.day_8_to_be_billed = s.day_8_to_be_billed
            ,t.day_8_pa_pct = s.day_8_pa_pct
            ,t.day_7_planned = s.day_7_planned
            ,t.day_7_actuals = s.day_7_actuals
            ,t.day_7_to_be_billed = s.day_7_to_be_billed
            ,t.day_7_pa_pct = s.day_7_pa_pct
            ,t.day_6_planned = s.day_6_planned
            ,t.day_6_actuals = s.day_6_actuals
            ,t.day_6_to_be_billed = s.day_6_to_be_billed
            ,t.day_6_pa_pct = s.day_6_pa_pct
            ,t.day_5_planned = s.day_5_planned
            ,t.day_5_actuals = s.day_5_actuals
            ,t.day_5_to_be_billed = s.day_5_to_be_billed
            ,t.day_5_pa_pct = s.day_5_pa_pct
            ,t.day_4_planned = s.day_4_planned
            ,t.day_4_actuals = s.day_4_actuals
            ,t.day_4_to_be_billed = s.day_4_to_be_billed
            ,t.day_4_pa_pct = s.day_4_pa_pct
            ,t.day_3_planned = s.day_3_planned
            ,t.day_3_actuals = s.day_3_planned
            ,t.day_3_to_be_billed = s.day_3_to_be_billed
            ,t.day_3_pa_pct = s.day_3_pa_pct
            ,t.day_2_planned = s.day_2_planned
            ,t.day_2_actuals = s.day_2_actuals
            ,t.day_2_to_be_billed = s.day_2_to_be_billed
            ,t.day_2_pa_pct = s.day_2_pa_pct
            ,t.day_1_planned = s.day_1_planned
            ,t.day_1_actuals = s.day_1_actuals
            ,t.day_1_to_be_billed = s.day_1_to_be_billed
            ,t.day_1_pa_pct = s.day_1_pa_pct
            ,t.total_actuals = s.total_actuals
            ,t.total_retries = s.total_retries
            ,t.total_planned = s.total_planned
            ,t.total_pa_pct = s.total_pa_pct
            ,t.total_total_planned = s.total_total_planned
            ,t.first_successful_default = s.first_successful_default
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
            ,t.prev_3_month_day_1_actuals = s.prev_3_month_day_1_actuals
            ,t.prev_3_month_day_1_to_be_billed = s.prev_3_month_day_1_to_be_billed
            ,t.prev_3_month_day_2_actuals = s.prev_3_month_day_2_actuals
            ,t.prev_3_month_day_2_to_be_billed = s.prev_3_month_day_2_to_be_billed
            ,t.prev_3_month_day_3_actuals = s.prev_3_month_day_3_actuals
            ,t.prev_3_month_day_3_to_be_billed = s.prev_3_month_day_3_to_be_billed
            ,t.prev_3_month_day_4_actuals = s.prev_3_month_day_4_actuals
            ,t.prev_3_month_day_4_to_be_billed = s.prev_3_month_day_4_to_be_billed
            ,t.prev_3_month_day_5_actuals = s.prev_3_month_day_5_actuals
            ,t.prev_3_month_day_5_to_be_billed = s.prev_3_month_day_5_to_be_billed
            ,t.prev_3_month_day_6_actuals = s.prev_3_month_day_6_actuals
            ,t.prev_3_month_day_6_to_be_billed = s.prev_3_month_day_6_to_be_billed
            ,t.prev_3_month_day_7_actuals = s.prev_3_month_day_7_actuals
            ,t.prev_3_month_day_7_to_be_billed = s.prev_3_month_day_7_to_be_billed
            ,t.prev_3_month_day_8_actuals = s.prev_3_month_day_8_actuals
            ,t.prev_3_month_day_8_to_be_billed = s.prev_3_month_day_8_to_be_billed
            ,t.prev_3_month_day_9_actuals = s.prev_3_month_day_9_actuals
            ,t.prev_3_month_day_9_to_be_billed = s.prev_3_month_day_9_to_be_billed
            ,t.prev_month_1st_actuals = s.prev_month_1st_actuals
            ,t.prev_month_retries = s.prev_month_retries
            ,t.meta_row_hash = s.meta_row_hash
            ,t.meta_update_datetime = CURRENT_TIMESTAMP;

COMMIT;
