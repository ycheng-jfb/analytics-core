SET process_from_date= (
        SELECT COALESCE(DATEADD('MONTH', -3, MAX(month_date)), '1900-01-01')
        FROM edw_prod.analytics_base.customer_failed_billings_monthly
    );

SET datetime_added= CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _failed_billing_base AS
SELECT clvmc.month_date,
       clvmc.customer_id,
       edw_prod.stg.udf_unconcat_brand(customer_id)                                                                                      AS meta_original_customer_id,
       clvmc.store_id,
       clvmc.gender,
       vip_cohort_month_date,
       ROW_NUMBER() OVER (PARTITION BY clvmc.month_date, clvmc.customer_id, clvmc.store_id ORDER BY vip_cohort_month_date DESC) AS rnk
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvmc
WHERE is_failed_billing
  AND clvmc.month_date >= $process_from_date;

CREATE OR REPLACE TEMPORARY TABLE _customer_base AS
SELECT month_date,
       customer_id,
       meta_original_customer_id,
       store_id,
       gender,
       vip_cohort_month_date,
       DATEADD('MONTH', -12, month_date) AS twelve_months_back
FROM _failed_billing_base
WHERE rnk = 1;

CREATE OR REPLACE TEMPORARY TABLE _distinct_customers AS
SELECT DISTINCT customer_id, meta_original_customer_id
FROM _customer_base;

CREATE OR REPLACE TEMPORARY TABLE _email_opens AS
SELECT dc.customer_id,
       TO_DATE(DATE_TRUNC('MONTH', email.event_time)) AS opened_month,
       COUNT(1)                                       AS number_of_email_opens
FROM lake_view.emarsys.email_opens email
         JOIN lake_view.emarsys.email_subscribes sub ON sub.emarsys_user_id = email.contact_id
    AND email.store_group = sub.brand
         JOIN _distinct_customers dc ON dc.meta_original_customer_id = sub.customer_id
WHERE email.event_time >= $process_from_date
GROUP BY dc.customer_id,
         TO_DATE(DATE_TRUNC('MONTH', email.event_time));


CREATE OR REPLACE TEMPORARY TABLE _email_clicks AS
SELECT dc.customer_id,
       TO_DATE(DATE_TRUNC('MONTH', email.event_time)) AS clicked_month,
       COUNT(1)                                       AS number_of_email_clicks
FROM lake_view.emarsys.email_clicks email
         JOIN lake_view.emarsys.email_subscribes sub ON sub.emarsys_user_id = email.contact_id
    AND email.store_group = sub.brand
         JOIN _distinct_customers dc ON dc.meta_original_customer_id = sub.customer_id
WHERE clicked_month >= $process_from_date
GROUP BY dc.customer_id,
         TO_DATE(DATE_TRUNC('MONTH', email.event_time));

CREATE OR REPLACE TEMPORARY TABLE _failed_billings AS
SELECT month_date,
       cb.customer_id,
       cb.gender,
       cb.store_id,
       ds.store_region,
       ds.store_brand,
       ds.store_country,
       ds.store_type,
       ds.store_full_name,
       DATEDIFF('MONTH', vip_cohort_month_date, month_date) + 1 AS vip_tenure,
       twelve_months_back,
       IFF(opened_month IS NOT NULL, TRUE, FALSE)               AS opened_an_email,
       COALESCE(number_of_email_opens, 0)                       AS number_of_email_opens,
       IFF(clicked_month IS NOT NULL, TRUE, FALSE)              AS clicked_an_email,
       COALESCE(number_of_email_clicks, 0)                      AS number_of_email_clicks
FROM _customer_base cb
         JOIN edw_prod.data_model_jfb.dim_store ds ON ds.store_id = cb.store_id
         LEFT JOIN _email_clicks ec ON cb.customer_id = ec.customer_id
    AND cb.month_date = ec.clicked_month
         LEFT JOIN _email_opens eo ON cb.customer_id = eo.customer_id
    AND cb.month_date = eo.opened_month
;

CREATE OR REPLACE TEMPORARY TABLE _distinct_failed_billing_customers AS
SELECT DISTINCT customer_id,
                store_id
FROM _failed_billings fb;

CREATE OR REPLACE TEMPORARY TABLE _clvm_base AS
SELECT clvm.month_date,
       clvm.customer_id,
       clvm.store_id,
       MAX(is_successful_billing) AS is_successful_billing,
       MAX(is_skip)               AS is_skip,
       MAX(is_login)              AS is_login,
       MAX(is_failed_billing)     AS is_failed_billing,
       SUM(product_order_count)   AS product_order_count
FROM edw_prod.analytics_base.customer_lifetime_value_monthly clvm
         JOIN _distinct_failed_billing_customers dfbc ON clvm.customer_id = dfbc.customer_id
    AND clvm.store_id = dfbc.store_id
GROUP BY clvm.month_date,
         clvm.customer_id,
         clvm.store_id;

CREATE OR REPLACE TEMPORARY TABLE _customer_actions AS
SELECT fb.month_date,
       fb.customer_id,
       fb.store_id,
       MAX(IFF(cb.is_failed_billing AND cb.month_date = DATEADD('MONTH', -1, fb.month_date), TRUE,
               FALSE))                                                                            AS is_failed_prior_month,
       MAX(IFF(cb.is_successful_billing AND cb.month_date <> fb.month_date, cb.month_date,
               NULL))                                                                             AS last_successful_billing_month,
       DATEDIFF('MONTH', last_successful_billing_month, fb.month_date)                            AS months_since_last_successful_billing,
       MAX(IFF(cb.is_skip AND cb.month_date <> fb.month_date, cb.month_date, NULL))               AS last_skip_month,
       DATEDIFF('MONTH', last_skip_month, fb.month_date)                                          AS months_since_last_skip,
       MAX(IFF(cb.is_login AND cb.month_date <> fb.month_date, cb.month_date, NULL))              AS last_login_month,
       DATEDIFF('MONTH', last_login_month, fb.month_date)                                         AS months_since_last_login,
       MAX(IFF(cb.product_order_count > 0, cb.month_date, NULL))                                  AS last_successful_order_month,
       DATEDIFF('MONTH', last_successful_order_month, fb.month_date)                              AS months_since_last_successful_order,
       COUNT(DISTINCT
             IFF(cb.is_failed_billing AND fb.month_date > cb.month_date AND fb.twelve_months_back <= cb.month_date,
                 cb.month_date,
                 NULL))                                                                           AS number_of_failed_billings_in_last_12_months,
       COUNT(DISTINCT IFF(cb.is_login AND fb.month_date > cb.month_date AND fb.twelve_months_back <= cb.month_date,
                          cb.month_date,
                          NULL))                                                                  AS number_of_visits_in_last_12_months,
       COUNT(DISTINCT
             IFF(cb.is_skip AND fb.month_date > cb.month_date AND fb.twelve_months_back <= cb.month_date, cb.month_date,
                 NULL))                                                                           AS number_of_skips_in_last_12_months
FROM _failed_billings fb
         LEFT JOIN _clvm_base cb ON cb.customer_id = fb.customer_id
    AND cb.store_id = fb.store_id
    AND cb.month_date <= fb.month_date
GROUP BY fb.month_date,
         fb.customer_id,
         fb.store_id;

CREATE OR REPLACE TEMPORARY TABLE _consecutive_failed_billings AS
WITH _groups AS (SELECT month_date,
                        customer_id,
                        store_id,
                        ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY month_date) AS rnk,
                        DATEADD('MONTH', -rnk, month_date)                                         AS grp
                 FROM _clvm_base
                 WHERE is_failed_billing)
SELECT month_date,
       customer_id,
       store_id,
       ROW_NUMBER() OVER (PARTITION BY customer_id, store_id, grp ORDER BY month_date) -
       1 AS consecutive_failed_billings
FROM _groups;

CREATE OR REPLACE TEMPORARY TABLE _months_since_last_email_open AS
SELECT fb.month_date,
       fb.customer_id,
       fb.store_id,
       MAX(IFF(eo.opened_month IS NOT NULL, eo.opened_month, NULL))                         AS last_email_opened_month,
       DATEDIFF('MONTH', last_email_opened_month, fb.month_date)                            AS months_since_last_email_open,
       COUNT(DISTINCT IFF(eo.opened_month IS NOT NULL AND fb.month_date > eo.opened_month AND
                          fb.twelve_months_back <= eo.opened_month, eo.opened_month,
                          NULL))                                                            AS emails_opened_in_last_12_months
FROM _failed_billings fb
         LEFT JOIN _email_opens eo ON eo.customer_id = fb.customer_id
    AND eo.opened_month <= fb.month_date
GROUP BY fb.month_date,
         fb.customer_id,
         fb.store_id;

CREATE OR REPLACE TEMPORARY TABLE _months_since_last_email_click AS
SELECT fb.month_date,
       fb.customer_id,
       fb.store_id,
       MAX(IFF(es.clicked_month IS NOT NULL, es.clicked_month, NULL)) AS last_email_clicked_month,
       DATEDIFF('MONTH', last_email_clicked_month, fb.month_date)     AS months_since_last_email_click
FROM _failed_billings fb
         LEFT JOIN _email_clicks es ON es.customer_id = fb.customer_id
    AND es.clicked_month <= fb.month_date
GROUP BY fb.month_date,
         fb.customer_id,
         fb.store_id;

CREATE OR REPLACE TEMPORARY TABLE _first_failed_billing_month AS
SELECT customer_id,
       store_id,
       MIN(month_date) AS month_date
FROM _clvm_base
WHERE is_failed_billing
GROUP BY customer_id,
         store_id;

CREATE OR REPLACE TEMPORARY TABLE _failed_billings_dataset AS
SELECT fb.month_date,
       fb.customer_id,
       fb.gender,
       fb.store_id,
       fb.store_brand,
       fb.store_region,
       fb.store_country,
       fb.store_full_name,
       fb.vip_tenure,
       cb.is_login,
       IFF(ffbm.month_date IS NOT NULL, TRUE, FALSE) AS is_first_failed_billing,
       ca.is_failed_prior_month,
       fb.opened_an_email,
       fb.clicked_an_email,
       fb.number_of_email_opens,
       fb.number_of_email_clicks,
       cfb.consecutive_failed_billings,
       ca.months_since_last_successful_billing,
       ca.number_of_failed_billings_in_last_12_months,
       ca.months_since_last_skip,
       ca.number_of_skips_in_last_12_months,
       ca.months_since_last_login,
       ca.number_of_visits_in_last_12_months,
       msleo.months_since_last_email_open,
       mslec.months_since_last_email_click,
       ca.months_since_last_successful_order
FROM _failed_billings fb
         LEFT JOIN _clvm_base cb ON cb.month_date = fb.month_date
    AND cb.customer_id = fb.customer_id
    AND cb.store_id = fb.store_id
         LEFT JOIN _customer_actions ca ON ca.month_date = fb.month_date
    AND ca.customer_id = fb.customer_id
    AND ca.store_id = fb.store_id
         LEFT JOIN _first_failed_billing_month ffbm ON ffbm.month_date = fb.month_date
    AND ffbm.customer_id = fb.customer_id
    AND ffbm.store_id = fb.store_id
         LEFT JOIN _months_since_last_email_open msleo ON msleo.month_date = fb.month_date
    AND msleo.customer_id = fb.customer_id
    AND msleo.store_id = fb.store_id
         LEFT JOIN _months_since_last_email_click mslec ON mslec.month_date = fb.month_date
    AND mslec.customer_id = fb.customer_id
    AND mslec.store_id = fb.store_id
         LEFT JOIN _consecutive_failed_billings cfb ON cfb.month_date = fb.month_date
    AND cfb.customer_id = fb.customer_id
    AND cfb.store_id = fb.store_id;

BEGIN;

DELETE
FROM analytics_base.customer_failed_billings_monthly
WHERE month_date >= $process_from_date;

INSERT INTO analytics_base.customer_failed_billings_monthly (month_date,
                                                             customer_id,
                                                             gender,
                                                             store_id,
                                                             store_brand,
                                                             store_region,
                                                             store_full_name,
                                                             store_country,
                                                             vip_tenure,
                                                             is_first_failed_billing,
                                                             is_failed_prior_month,
                                                             consecutive_failed_billings,
                                                             months_since_last_successful_billing,
                                                             number_of_failed_billings_in_last_12_months,
                                                             months_since_last_skip,
                                                             number_of_skips_in_last_12_months,
                                                             months_since_last_login,
                                                             is_login,
                                                             number_of_visits_in_last_12_months,
                                                             opened_an_email,
                                                             clicked_an_email,
                                                             months_since_last_email_open,
                                                             months_since_last_email_click,
                                                             months_since_last_successful_order,
                                                             number_of_email_opens,
                                                             number_of_email_clicks,
                                                             meta_create_datetime,
                                                             meta_update_datetime)
SELECT month_date,
       customer_id,
       gender,
       store_id,
       store_brand,
       store_region,
       store_full_name,
       store_country,
       vip_tenure,
       is_first_failed_billing,
       is_failed_prior_month,
       consecutive_failed_billings,
       months_since_last_successful_billing,
       number_of_failed_billings_in_last_12_months,
       months_since_last_skip,
       number_of_skips_in_last_12_months,
       months_since_last_login,
       is_login,
       number_of_visits_in_last_12_months,
       opened_an_email,
       clicked_an_email,
       months_since_last_email_open,
       months_since_last_email_click,
       months_since_last_successful_order,
       number_of_email_opens,
       number_of_email_clicks,
       $datetime_added AS meta_create_datetime,
       $datetime_added AS meta_update_datetime
FROM _failed_billings_dataset;

COMMIT;
