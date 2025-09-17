SET today_date = CURRENT_DATE();

--Billing Cycles Day
CREATE OR REPLACE TEMP TABLE _billing_cycle_days AS
SELECT DISTINCT month_date,
                full_date
FROM edw_prod.data_model.dim_date
WHERE DATE_TRUNC('MONTH', full_date) <= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
  AND DAY(full_date) <= DAY(CURRENT_TIMESTAMP())
  AND full_date >= '2018-01-01'
  AND DATE_TRUNC('MONTH', full_date) = DATE_TRUNC('MONTH', $today_date)
;

-- Activating Order Details
CREATE OR REPLACE TEMPORARY TABLE _first_order_details AS
SELECT customer_id,
       activation_key,
       order_local_datetime AS first_order_date,
       payment_key          AS first_payment_key
FROM edw_prod.data_model.fact_order
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, activation_key ORDER BY order_local_datetime) = 1;

CREATE OR REPLACE TEMPORARY TABLE _activating_order_data AS
SELECT clvmc.customer_id,
       clvmc.store_id,
       clvmc.month_date,
       clvmc.activation_key,
       clvmc.first_activation_key,
       first_order_date,
       first_payment_key
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvmc
         LEFT JOIN _first_order_details fod ON fod.customer_id = clvmc.customer_id
    AND fod.activation_key = clvmc.activation_key
WHERE clvmc.is_bop_vip;

--Billable VIPs and BOP VIPs with MAX_DAY
CREATE OR REPLACE TEMP TABLE _bop_vips AS
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                             AS store_brand,
       st.store_country,
       ltv.gender                                                              AS customer_gender,
       st.store_region,
       ltv.is_retail_vip,
       ltv.month_date                                                          AS period_month_date,
       p.period_id,
       ltv.customer_id,
       m.membership_id,
       ltv.customer_action_category,
       'M' || (1 + DATEDIFF(MONTH, ltv.vip_cohort_month_date, ltv.month_date)) AS tenure,
       aod.activation_key,
       aod.first_payment_key
FROM edw_prod.analytics_base.customer_lifetime_value_monthly ltv
         JOIN lake_consolidated_view.ultra_merchant.period p
              ON p.date_period_start = ltv.month_date
         JOIN edw_prod.data_model.dim_store st
              ON st.store_id = ltv.store_id
         JOIN lake_consolidated_view.ultra_merchant.membership m
              ON m.customer_id = ltv.customer_id
         JOIN _activating_order_data AS aod
              ON aod.customer_id = ltv.customer_id
                  AND aod.store_id = ltv.store_id
                  AND aod.month_date = ltv.month_date
WHERE is_bop_vip = 1
  AND p.type = 'monthly'
  AND DATE_TRUNC('MONTH', period_month_date) = DATE_TRUNC('MONTH', $today_date)
;

CREATE OR REPLACE TEMP TABLE _bop_vips_final AS
SELECT a.store_brand,
       a.store_country,
       a.customer_gender,
       a.store_region,
       a.is_retail_vip,
       a.period_month_date,
       a.period_id,
       a.customer_id,
       a.membership_id,
       a.customer_action_category,
       a.tenure,
       a.first_payment_key AS payment_key,
       full_date,
       DAY(full_date)      AS max_day
FROM _bop_vips a
         JOIN _billing_cycle_days d
              ON a.period_month_date = d.month_date;

CREATE OR REPLACE TEMP TABLE _skips AS
SELECT DISTINCT period_month_date,
                d.full_date,
                DAY(d.full_date) AS max_day,
                customer_id
FROM _bop_vips b
         JOIN _billing_cycle_days d
              ON d.month_date = b.period_month_date
         JOIN lake_consolidated_view.ultra_merchant.membership_skip ms
              ON ms.membership_id = b.membership_id
                  AND DATE_TRUNC(MONTH, ms.datetime_added::DATE) = b.period_month_date
                  AND DAY(ms.datetime_added) <= DAY(d.full_date);

CREATE OR REPLACE TEMP TABLE _snooze AS
SELECT DISTINCT period_month_date,
                d.full_date,
                DAY(d.full_date) AS max_day,
                customer_id
FROM _bop_vips b
         JOIN _billing_cycle_days d
              ON d.month_date = b.period_month_date
         JOIN lake_consolidated_view.ultra_merchant.membership_skip ms
              ON ms.membership_id = b.membership_id
                  AND DATE_TRUNC(MONTH, ms.datetime_added::DATE) = b.period_month_date
                  AND DAY(ms.datetime_added) <= DAY(d.full_date)
WHERE membership_skip_reason_id = 36;

CREATE OR REPLACE TEMP TABLE _product_orders AS
SELECT DISTINCT b.period_month_date,
                d.full_date,
                DAY(d.full_date) AS max_day,
                b.customer_id
FROM _bop_vips b
         JOIN _billing_cycle_days d
              ON d.month_date = b.period_month_date
         JOIN edw_prod.data_model.fact_order fo
              ON fo.customer_id = b.customer_id
         JOIN edw_prod.data_model.dim_order_sales_channel doc
              ON doc.order_sales_channel_key = fo.order_sales_channel_key
         JOIN edw_prod.data_model.dim_order_status os
              ON os.order_status_key = fo.order_status_key
WHERE doc.order_classification_l1 = 'Product Order'
  AND os.order_status IN ('Success', 'Pending')
  AND DAY(fo.order_local_datetime) <= DAY(d.full_date)
  AND DATE_TRUNC(MONTH, fo.order_local_datetime::DATE) = b.period_month_date;

CREATE OR REPLACE TEMP TABLE _cancels AS
SELECT DISTINCT b.period_month_date,
                d.full_date,
                DAY(d.full_date) AS max_day,
                b.customer_id
FROM _bop_vips b
         JOIN _billing_cycle_days d
              ON d.month_date = b.period_month_date
         JOIN edw_prod.data_model.fact_activation v
              ON v.customer_id = b.customer_id
                  AND DATE_TRUNC(MONTH, v.cancellation_local_datetime::DATE) = period_month_date
                  AND DAY(v.cancellation_local_datetime) <= DAY(d.full_date);

CREATE OR REPLACE TEMP TABLE _billing_cycle_rates_1st_through_5th AS
SELECT b.store_brand,
       b.store_region,
       b.store_country,
       b.is_retail_vip,
       b.period_month_date  AS month,
       b.full_date,
       b.max_day,
       b.tenure,
       b.customer_action_category,
       b.payment_key,
       COUNT(b.customer_id) AS bop_vips,
       CASE
           WHEN (b.store_brand = 'Fabletics' AND b.store_region = 'NA' AND b.period_month_date < '2021-01-01')
               OR (b.store_brand = 'Fabletics' AND b.store_region = 'EU' AND b.period_month_date < '2021-11-01')
               OR (b.store_brand = 'Savage X' AND b.store_region = 'NA' AND b.period_month_date < '2022-01-01')
               OR (b.store_brand = 'JustFab' AND b.store_country = 'US' AND b.period_month_date < '2023-08-01')
               OR (b.store_brand = 'FabKids' AND b.store_country = 'US' AND b.period_month_date < '2023-10-01')
               OR (b.store_brand = 'ShoeDazzle' AND b.store_country = 'US' AND b.period_month_date < '2023-10-01')
               THEN
               COUNT(DISTINCT IFF(s.customer_id IS NULL AND sn.customer_id IS NULL AND po.customer_id IS NULL AND
                                  c.customer_id IS NULL, b.customer_id, NULL))
           ELSE
               COUNT(DISTINCT
                     IFF(s.customer_id IS NULL AND sn.customer_id IS NULL AND c.customer_id IS NULL, b.customer_id,
                         NULL))
           END              AS eligible_to_be_billed
FROM _bop_vips_final b
         LEFT JOIN _skips s
                   ON b.customer_id = s.customer_id
                       AND b.period_month_date = s.period_month_date
                       AND b.max_day = s.max_day
         LEFT JOIN _snooze sn
                   ON b.customer_id = sn.customer_id
                       AND b.period_month_date = sn.period_month_date
                       AND b.max_day = sn.max_day
         LEFT JOIN _product_orders po
                   ON b.customer_id = po.customer_id
                       AND b.period_month_date = po.period_month_date
                       AND b.max_day = po.max_day
         LEFT JOIN _cancels c
                   ON c.customer_id = b.customer_id
                       AND b.period_month_date = c.period_month_date
                       AND b.max_day = c.max_day
WHERE b.max_day <= 5
GROUP BY store_brand,
         store_region,
         store_country,
         is_retail_vip,
         b.period_month_date,
         b.full_date,
         b.max_day,
         b.tenure,
         b.customer_action_category,
         b.payment_key;

CREATE OR REPLACE TEMPORARY TABLE _vw_billing_cycle_rates AS
SELECT month                      AS order_month,
       store_brand                AS store_name,
       store_region               AS region,
       store_country              AS country,
       full_date                  AS full_date,
       max_day                    AS max_day,
       customer_action_category,
       payment_key,
       SUM(eligible_to_be_billed) AS billable_vips,
       SUM(bop_vips)              AS bop_vips
FROM _billing_cycle_rates_1st_through_5th
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

CREATE OR REPLACE TEMP TABLE _overlapping_orders AS
SELECT mp.*
FROM lake_consolidated_view.ultra_merchant.membership_period mp
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q ON q.order_id = mp.credit_order_id
WHERE q.datetime_last_retry IS NOT NULL  AND (q.statuscode != 4209 );

--Successful Billing and Retries
CREATE OR REPLACE TEMP TABLE _billing_estimator_rates AS
SELECT DATE(p.date_period_start)                         AS billing_period,
       CAST(convert_timezone('America/Los_Angeles',fo.order_completion_local_datetime) AS DATE)              AS order_date_added,
       CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                       AS store_name,
       st.store_country                                  AS country,
       st.store_region                                   AS region,
       aod.first_payment_key,
       COUNT(DISTINCT CASE
                          WHEN q.datetime_last_retry IS NOT NULL AND (q.statuscode != 4209 )
                              THEN q.order_id END)       AS retry_success,

       COUNT(DISTINCT CASE
                          WHEN mp.statuscode = 3957 AND (q.billing_retry_schedule_id IS NULL OR q.statuscode = 4209)
                              THEN o.order_id END)       AS credited_first_time,

       COUNT(DISTINCT o.order_id)                        AS total_attempts,
       COUNT(DISTINCT o.order_id)
           - COUNT(DISTINCT CASE
                                WHEN mp.statuscode = 3957 AND q.billing_retry_schedule_id IS NULL
                                    THEN o.order_id END) AS total_attempts_less_credited_first_time
FROM lake_consolidated_view.ultra_merchant.membership_period mp
         LEFT OUTER JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = mp.credit_order_id
         LEFT OUTER JOIN edw_prod.data_model.fact_order fo ON o.order_id = fo.order_id
         LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q ON q.order_id = o.order_id
         JOIN lake_consolidated_view.ultra_merchant.membership m ON mp.membership_id = m.membership_id
         JOIN lake_consolidated_view.ultra_merchant.store s ON s.store_id = m.store_id
         JOIN lake_consolidated_view.ultra_merchant.period p ON p.period_id = mp.period_id
         LEFT JOIN edw_prod.data_model.dim_store st ON s.store_id = st.store_id
         LEFT JOIN edw_prod.data_model.dim_customer dc ON st.store_id = dc.store_id AND o.customer_id = dc.customer_id
         LEFT JOIN _activating_order_data AS aod
                   ON dc.customer_id = aod.customer_id
                       AND dc.store_id = aod.store_id
                       AND DATE_TRUNC('MONTH', o.datetime_added) = DATE_TRUNC('MONTH', aod.month_date)
WHERE DATE(p.date_period_start) >= '2018-01-01'
  AND m.store_id IN (46, 52, 79, 26, 41, 121, 55, 36, 38, 48, 50, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77,
                     125, 127, 129, 131, 133, 135, 137, 139)
  AND order_status_key in (1,2)
  AND DATE_TRUNC('MONTH', p.date_period_start) = DATE_TRUNC('MONTH', $today_date)
GROUP BY 1, 2, 3, 4, 5, 6
UNION
SELECT DATE(p.date_period_start)                         AS billing_period,
       CAST(convert_timezone('America/Los_Angeles',fo.order_completion_local_datetime) AS DATE)              AS order_date_added,
       CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                       AS store_name,
       st.store_country                                  AS country,
       st.store_region                                   AS region,
       aod.first_payment_key,
       COUNT(DISTINCT CASE
                          WHEN q.datetime_last_retry IS NOT NULL AND
                               (q.statuscode != 4209 )
                              THEN q.order_id END)       AS retry_success,
       COUNT(DISTINCT CASE
                          WHEN mp.statuscode IN (5170, 5171) AND
                               (q.billing_retry_schedule_id IS NULL OR q.statuscode = 4209)
                              THEN o.order_id END)       AS credited_first_time,
       COUNT(DISTINCT o.order_id)                        AS total_attempts,
       COUNT(DISTINCT o.order_id)
           - COUNT(DISTINCT CASE
                                WHEN mp.statuscode IN (5170, 5171) AND q.billing_retry_schedule_id IS NULL
                                    THEN o.order_id END) AS total_attempts_less_credited_first_time
FROM lake_consolidated_view.ultra_merchant.membership_billing mp
         LEFT OUTER JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = mp.order_id
         LEFT OUTER JOIN edw_prod.data_model.fact_order fo ON o.order_id = fo.order_id
         LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q ON q.order_id = o.order_id
         JOIN lake_consolidated_view.ultra_merchant.membership m ON mp.membership_id = m.membership_id
         JOIN lake_consolidated_view.ultra_merchant.store s ON s.store_id = m.store_id
         JOIN lake_consolidated_view.ultra_merchant.period p ON p.period_id = mp.period_id
         LEFT JOIN edw_prod.data_model.dim_store st ON s.store_id = st.store_id
         LEFT JOIN edw_prod.data_model.dim_customer dc ON st.store_id = dc.store_id AND o.customer_id = dc.customer_id
         LEFT JOIN _activating_order_data AS aod
                   ON dc.customer_id = aod.customer_id
                       AND DATE_TRUNC('MONTH', o.datetime_added) = DATE_TRUNC('MONTH', aod.month_date)
                       AND dc.store_id = aod.store_id
WHERE DATE(p.date_period_start) >= '2021-01-01'
  AND mp.membership_type_id = 3
  AND order_status_key in (1,2)
  AND mp.order_id not in (select credit_order_id from _overlapping_orders)
  AND DATE_TRUNC('MONTH', p.date_period_start) = DATE_TRUNC('MONTH', $today_date)
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE TEMPORARY TABLE _rates_by_day AS
SELECT billing_period                                                         AS order_month,
       order_date_added                                                       AS full_date,
       store_name,
       country,
       region,
       first_payment_key                                                      AS payment_key,
       SUM(credited_first_time)                                               AS successful_first_billing_attempts,
       SUM(retry_success)                                                     AS successful_retries,
       DIV0(SUM(credited_first_time), SUM(total_attempts))                    AS first_time_success_rate_daily,
       DIV0(SUM(retry_success), SUM(total_attempts_less_credited_first_time)) AS retry_success_rate_daily
FROM _billing_estimator_rates
GROUP BY 1, 2, 3, 4, 5, 6;

DELETE
FROM shared.action_from_billable_vips
WHERE order_month = DATE_TRUNC('MONTH', $today_date);

INSERT INTO shared.action_from_billable_vips
(order_month,
       store_name,
       region,
       country,
       full_date,
       max_day,
       bop_vips,
       billable_vips,
       customer_action_category,
       creditcard_type,
       successful_first_billing_attempts,
       successful_retries,
       first_time_success_rate_daily,
       retry_success_rate_daily)
SELECT order_month,
       store_name,
       region,
       country,
       full_date,
       max_day,
       bop_vips,
       billable_vips,
       customer_action_category,
       dp.creditcard_type,
       successful_first_billing_attempts,
       successful_retries,
       first_time_success_rate_daily,
       retry_success_rate_daily
FROM (SELECT order_month,
             store_name,
             region,
             country,
             full_date,
             max_day,
             bop_vips,
             billable_vips,
             customer_action_category,
             payment_key,
             0 AS successful_first_billing_attempts,
             0 AS successful_retries,
             0 AS first_time_success_rate_daily,
             0 AS retry_success_rate_daily
      FROM _vw_billing_cycle_rates

      UNION

      SELECT order_month,
             store_name,
             region,
             country,
             full_date,
             DAY(full_date) AS max_day,
             0              AS bop_vips,
             0              AS billable_vips,
             NULL           AS customer_action_category,
             payment_key,
             successful_first_billing_attempts,
             successful_retries,
             first_time_success_rate_daily,
             retry_success_rate_daily
      FROM _rates_by_day) AS a
         LEFT JOIN edw_prod.data_model.dim_payment dp
                   ON COALESCE(a.payment_key, -1) = dp.payment_key;
