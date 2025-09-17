SET execution_start_time = CURRENT_TIMESTAMP;

/*
#1
M1/Activations
Metrics Count, Cancellations
 */
CREATE OR REPLACE TEMPORARY TABLE _m1_cancels AS
SELECT fa.sub_store_id                                                                AS event_store_id
     , fa.sub_store_id                                                                AS vip_store_id
     , fa.is_retail_vip                                                               AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)                                         AS customer_gender
     , dc.is_cross_promo                                                              AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(st.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , fa.vip_cohort_month_date                                                       AS vip_cohort
     , fa.vip_cohort_month_date                                                       AS month_date
     , COUNT(DISTINCT fa.customer_id)                                                 AS metric_count
     , SUM(IFF(DATE_TRUNC('MONTH', CAST(activation_local_datetime AS DATE)) =
               DATE_TRUNC('MONTH', CAST(cancellation_local_datetime AS DATE)), 1, 0)) AS cancellations
FROM data_model.fact_activation fa
         JOIN data_model.dim_customer dc ON dc.customer_id = fa.customer_id
         JOIN data_model.dim_store st ON st.store_id = fa.sub_store_id
WHERE 1 = 1
  AND CAST(activation_local_datetime AS DATE) >= '2010-01-01'
GROUP BY fa.sub_store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(st.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , fa.vip_cohort_month_date
       , fa.vip_cohort_month_date
;

/*
#2
M2/BOPs
Metrics Count, Credit Billers, cancellations, skips, failed billings, passive cancels, merch purchasers
 */
CREATE OR REPLACE TEMPORARY TABLE _m2_ltv AS
SELECT fa.sub_store_id                              AS event_store_id
     , fa.sub_store_id                              AS vip_store_id
     , fa.is_retail_vip                             AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)       AS customer_gender
     , dc.is_cross_promo                            AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , ltv.vip_cohort_month_date                    AS vip_cohort
     , ltv.month_date                               AS month_date
     , SUM(IFF(is_bop_vip = TRUE, 1, 0))            AS metric_count
     , SUM(IFF(is_successful_billing = TRUE, 1, 0)) AS credit_billers
     , SUM(IFF(is_cancel = TRUE, 1, 0))             AS cancellations
     , SUM(IFF(is_skip = TRUE, 1, 0))               AS skips
     , SUM(IFF(is_failed_billing = TRUE, 1, 0))     AS failed_billing
     , SUM(IFF(is_passive_cancel = TRUE, 1, 0))     AS passive_cancel
     , SUM(IFF(is_merch_purchaser = TRUE AND is_bop_vip = TRUE, 1, 0))    AS merch_purchasers_ltv
FROM analytics_base.customer_lifetime_value_monthly ltv
         JOIN data_model.fact_activation fa ON ltv.activation_key = fa.activation_key
         JOIN data_model.dim_store ds ON ltv.store_id = ds.store_id
         LEFT JOIN data_model.dim_store ds2 ON fa.sub_store_id = ds2.store_id
         JOIN data_model.dim_customer dc ON ltv.customer_id = dc.customer_id
WHERE ltv.vip_cohort_month_date >= '2010-01-01'
  AND is_bop_vip = 1
GROUP BY fa.sub_store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , ltv.vip_cohort_month_date
       , ltv.month_date
;


/*
#3
ALL
shipped orders
 */
CREATE OR REPLACE TEMPORARY TABLE _shipped_orders AS
SELECT fo.store_id                                                  AS event_store_id
     , fa.sub_store_id                                              AS vip_store_id
     , fa.is_retail_vip                                             AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)                       AS customer_gender
     , dc.is_cross_promo                                            AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , fa.vip_cohort_month_date                                     AS vip_cohort
     , DATE_TRUNC('MONTH', cast(fo.order_completion_local_datetime AS DATE)) AS month_date
     , COUNT(DISTINCT iff(fa.activation_local_datetime <= fo.order_local_datetime,
                           fo.order_id, null))                              AS shipped_orders -- now total
     , COUNT(DISTINCT iff(fa.activation_local_datetime <= fo.order_local_datetime
                            AND fa.cancellation_local_datetime > fo.order_local_datetime,
                           fo.order_id, null)) as shipped_orders_active_vip
     , COUNT(DISTINCT iff(fa.cancellation_local_datetime <= fo.order_local_datetime,
                           fo.order_id, null)) as shipped_orders_inactive_vip
     , SUM(fo.product_gross_revenue_local_amount*fo.reporting_usd_conversion_rate) AS product_gross_revenue
FROM data_model.fact_order fo
         JOIN data_model.fact_activation fa ON fo.activation_key = fa.activation_key
    AND fo.activation_key <> -1
    AND fo.order_local_datetime >= fa.activation_local_datetime
--     AND fo.order_local_datetime < fa.cancellation_local_datetime
         JOIN data_model.dim_store ds ON fa.sub_store_id = ds.store_id
         LEFT JOIN data_model.dim_customer dc ON dc.customer_id = fa.customer_id
         JOIN data_model.dim_order_status dos ON fo.order_status_key = dos.order_status_key
         JOIN data_model.dim_order_sales_channel dosc
              ON fo.order_sales_channel_key = dosc.order_sales_channel_key AND dosc.is_ps_order <> 1
         LEFT JOIN data_model.dim_order_membership_classification domc
                   ON fo.order_membership_classification_key = domc.order_membership_classification_key
WHERE dos.order_status = 'Success'
  AND dosc.order_classification_l1 = 'Product Order'
  AND fo.order_completion_local_datetime IS NOT NULL
  AND fa.vip_cohort_month_date >= '2010-01-01'
  AND domc.membership_order_type_l1 = 'NonActivating'
GROUP BY fo.store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , fa.vip_cohort_month_date
       , DATE_TRUNC('MONTH', cast(fo.order_completion_local_datetime AS DATE))
;

/*
#4
M2+, BOP VIPS
Credits Cancelled
 */
CREATE OR REPLACE TEMPORARY TABLE _m2_canc_credits AS
SELECT fa.sub_store_id                                             AS event_store_id
     , fa.sub_store_id                                             AS vip_store_id
     , fa.is_retail_vip                                            AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)                      AS customer_gender
     , dc.is_cross_promo                                           AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , fa.vip_cohort_month_date                                    AS vip_cohort
     , DATE_TRUNC(MONTH, fce.credit_activity_local_datetime::DATE) AS month_date
     , COUNT(DISTINCT iff(fa.activation_local_datetime <= fce.credit_activity_local_datetime,
                          fce.credit_key, NULL))                   AS cancelled_credits -- now total
     , COUNT(DISTINCT iff(fa.activation_local_datetime <= fce.credit_activity_local_datetime
                              AND fa.cancellation_local_datetime > fce.credit_activity_local_datetime,
                          fce.credit_key, NULL))                   AS cancelled_credits_active_vip
     , COUNT(DISTINCT iff(fa.cancellation_local_datetime <= fce.credit_activity_local_datetime,
                          fce.credit_key, NULL))                   AS cancelled_credits_inactive_vip
FROM reporting_base_prod.shared.fact_credit_event fce
         JOIN reporting_base_prod.shared.dim_credit dcred ON fce.credit_key = dcred.credit_key
         JOIN data_model.dim_store ds ON dcred.store_id = ds.store_id
         JOIN data_model.dim_customer dc ON dcred.customer_id = dc.customer_id
         LEFT JOIN data_model.fact_activation fa ON fce.activation_key = fa.activation_key
    AND fa.activation_local_datetime <= fce.credit_activity_local_datetime
--    AND fa.cancellation_local_datetime > fce.credit_activity_local_datetime
         LEFT JOIN data_model.dim_store ds2 ON fa.sub_store_id = ds2.store_id
WHERE 1 = 1
  AND LOWER(fce.credit_activity_type) = 'cancelled'
  AND LOWER(dcred.credit_report_mapping) = 'billed credit'
  AND fa.vip_cohort_month_date >= '2010-01-01'
GROUP BY fa.sub_store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , fa.vip_cohort_month_date
       , DATE_TRUNC(MONTH, fce.credit_activity_local_datetime::DATE)
;


/*
#5
M2/BOPs
Credits Redeemed
 */
CREATE OR REPLACE TEMPORARY TABLE _m2_credits_redeemed AS
SELECT fo.store_id                                                           AS event_store_id
     , fa.sub_store_id                                                       AS vip_store_id
     , fa.is_retail_vip                                                      AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)                                AS customer_gender
     , dc.is_cross_promo                                                     AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , fa.vip_cohort_month_date                                              AS vip_cohort
     , DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE)) AS month_date
     , SUM(foc.billed_cash_credit_redeemed_equivalent_count)                 AS credits_redeemed
     , SUM(foc.billed_cash_credit_redeemed_local_amount*fo.reporting_usd_conversion_rate) AS credits_redeemed_amount
FROM data_model.fact_order fo
         JOIN data_model.fact_activation fa ON fo.activation_key = fa.activation_key
    AND fo.activation_key <> -1
    AND fo.order_local_datetime >= fa.activation_local_datetime
    AND fo.order_local_datetime < fa.cancellation_local_datetime
         JOIN data_model.dim_store ds ON fa.sub_store_id = ds.store_id
         JOIN data_model.dim_store ds2 ON fo.store_id = ds2.store_id
         LEFT JOIN data_model.dim_customer dc ON dc.customer_id = fa.customer_id
         JOIN data_model.dim_order_status dos ON fo.order_status_key = dos.order_status_key
         JOIN data_model.dim_order_sales_channel dosc ON fo.order_sales_channel_key = dosc.order_sales_channel_key
         LEFT JOIN data_model.dim_order_membership_classification domc
                   ON fo.order_membership_classification_key = domc.order_membership_classification_key
         LEFT JOIN reporting_base_prod.shared.fact_order_credit foc ON fo.order_id = foc.order_id
WHERE 1 = 1
  AND dos.order_status = 'Success'
  AND dosc.order_classification_l1 = 'Product Order'
  AND fa.vip_cohort_month_date >= '2010-01-01'
  AND domc.membership_order_type_l1 = 'NonActivating'
GROUP BY fo.store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , fa.vip_cohort_month_date
       , DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE))
;


/*
#6
*/
CREATE OR REPLACE TEMPORARY TABLE _m1_credits_billers AS
SELECT fa.sub_store_id                                                       AS event_store_id
     , fa.sub_store_id                                                       AS vip_store_id
     , fa.is_retail_vip                                                      AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)                                AS customer_gender
     , dc.is_cross_promo                                                     AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , fa.vip_cohort_month_date                                              AS vip_cohort
     , DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE)) AS month_date
     , SUM(IFF(dos.order_status = 'Success', 1, 0))                          AS credit_billers
FROM data_model.fact_order fo
         JOIN data_model.fact_activation fa ON fo.activation_key = fa.activation_key
    AND fo.activation_key <> -1
    AND fo.order_local_datetime >= fa.activation_local_datetime
    AND fo.order_local_datetime < fa.cancellation_local_datetime
         JOIN data_model.dim_store ds ON fa.sub_store_id = ds.store_id
         JOIN data_model.dim_store ds2 ON fo.store_id = ds2.store_id
         LEFT JOIN data_model.dim_customer dc ON dc.customer_id = fa.customer_id
         JOIN data_model.dim_order_status dos ON fo.order_status_key = dos.order_status_key
         JOIN data_model.dim_order_sales_channel dosc ON fo.order_sales_channel_key = dosc.order_sales_channel_key
         LEFT JOIN data_model.dim_order_membership_classification domc
                   ON fo.order_membership_classification_key = domc.order_membership_classification_key
WHERE 1 = 1
  AND fa.vip_cohort_month_date >= '2010-01-01'
  AND order_classification_l2 IN ('Credit Billing', 'Token Billing')
  AND domc.membership_order_type_l1 = 'NonActivating'
  AND DATEDIFF(MONTH, fa.vip_cohort_month_date, DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE))) =
      0
GROUP BY fa.sub_store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , fa.vip_cohort_month_date
       , DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE))
;

/*
#7
*/
CREATE OR REPLACE TEMP TABLE _merch_purch AS
SELECT fo.store_id                                                           AS event_store_id
     , fa.sub_store_id                                                       AS vip_store_id
     , fa.is_retail_vip                                                      AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)                                AS customer_gender
     , dc.is_cross_promo                                                     AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , fa.vip_cohort_month_date                                              AS vip_cohort
     , DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE)) AS month_date
     , COUNT(DISTINCT iff(fa.activation_local_datetime <= fo.order_local_datetime,
                           fo.customer_id, null))                            AS merch_purchasers_fo_shipped
     , COUNT(DISTINCT iff(fa.activation_local_datetime <= fo.order_local_datetime
                            AND fa.cancellation_local_datetime > fo.order_local_datetime,
                           fo.customer_id, null)) as merch_purchasers_fo_shipped_active_vip
     , COUNT(DISTINCT iff(fa.cancellation_local_datetime <= fo.order_local_datetime,
                           fo.customer_id, null)) as merch_purchasers_fo_shipped_inactive_vip
FROM data_model.fact_order fo
         JOIN data_model.fact_activation fa ON fo.activation_key = fa.activation_key
    AND fo.activation_key <> -1
    AND fo.order_local_datetime >= fa.activation_local_datetime
--    AND fo.order_local_datetime < fa.cancellation_local_datetime
         JOIN data_model.dim_store ds ON fa.sub_store_id = ds.store_id
         LEFT JOIN data_model.dim_customer dc ON dc.customer_id = fa.customer_id
         JOIN data_model.dim_order_status dos ON fo.order_status_key = dos.order_status_key
         JOIN data_model.dim_order_sales_channel dosc
              ON fo.order_sales_channel_key = dosc.order_sales_channel_key AND dosc.is_ps_order <> 1
         LEFT JOIN data_model.dim_order_membership_classification domc
                   ON fo.order_membership_classification_key = domc.order_membership_classification_key
WHERE 1 = 1
  AND dos.order_status = 'Success'
  AND dosc.order_classification_l1 = 'Product Order'
  AND fa.vip_cohort_month_date >= '2010-01-01'
  AND domc.membership_order_type_l1 = 'NonActivating'
  AND fa.vip_cohort_month_date <> DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE))
GROUP BY fo.store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , fa.vip_cohort_month_date
       , DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE))
;

CREATE OR REPLACE TEMP TABLE _merch_purch_placed AS
SELECT fo.store_id                                                AS event_store_id
     , fa.sub_store_id                                            AS vip_store_id
     , fa.is_retail_vip                                           AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)                     AS customer_gender
     , dc.is_cross_promo                                          AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , fa.vip_cohort_month_date                                   AS vip_cohort
     , DATE_TRUNC('MONTH', CAST(fo.order_local_datetime AS DATE)) AS month_date
     , COUNT(DISTINCT fo.customer_id)                             AS merch_purchasers_fo_placed
FROM data_model.fact_order fo
         JOIN data_model.fact_activation fa ON fo.activation_key = fa.activation_key
    AND fo.activation_key <> -1
    AND fo.order_local_datetime >= fa.activation_local_datetime
    AND fo.order_local_datetime < fa.cancellation_local_datetime
         JOIN data_model.dim_store ds ON fa.sub_store_id = ds.store_id
         LEFT JOIN data_model.dim_customer dc ON dc.customer_id = fa.customer_id
         JOIN data_model.dim_order_status dos ON fo.order_status_key = dos.order_status_key
         JOIN data_model.dim_order_sales_channel dosc
              ON fo.order_sales_channel_key = dosc.order_sales_channel_key AND dosc.is_ps_order <> 1
         LEFT JOIN data_model.dim_order_membership_classification domc
                   ON fo.order_membership_classification_key = domc.order_membership_classification_key
WHERE 1 = 1
  AND dos.order_status = 'Success'
  AND dosc.order_classification_l1 = 'Product Order'
  AND fa.vip_cohort_month_date >= '2010-01-01'
  AND domc.membership_order_type_l1 = 'NonActivating'
  AND fa.vip_cohort_month_date <> DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE))
GROUP BY fo.store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , fa.vip_cohort_month_date
       , DATE_TRUNC('MONTH', CAST(fo.order_local_datetime AS DATE))
;


/*
#8
Activating Billed Credit Redeemed equivalent count
*/

CREATE OR REPLACE TEMPORARY TABLE _act_bcr_eqcnt AS
SELECT b.store_id                                        AS event_store_id
     , COALESCE(b.vip_store_id, -1)                      AS vip_store_id
     , b.is_retail_vip
     , b.gender                                          AS customer_gender
     , b.is_cross_promo
     , b.finance_specialty_store
     , b.is_scrubs_customer
     , b.vip_cohort_month_date                           AS vip_cohort
     , DATE_TRUNC('MONTH', b.date)                       AS month_date
     , SUM(billed_cash_credit_redeemed_equivalent_count) AS activating_billed_credit_redeemed_equivalent_counts
FROM analytics_base.finance_sales_ops b
         LEFT JOIN data_model.dim_order_membership_classification mc
                   ON mc.order_membership_classification_key = b.order_membership_classification_key
WHERE b.vip_cohort_month_date >= '2010-01-01'
  AND date_object = 'shipped'
  AND mc.membership_order_type_l1 = 'Activating VIP'
GROUP BY b.store_id
       , COALESCE(b.vip_store_id, -1)
       , b.is_retail_vip
       , b.gender
       , b.is_cross_promo
       , b.finance_specialty_store
       , b.is_scrubs_customer
       , b.vip_cohort_month_date
       , DATE_TRUNC('MONTH', b.date)
;


/*
#9
Cancelled VIP Billed Credit Redeemed equivalent count
*/
CREATE OR REPLACE TEMPORARY TABLE _canvip_bcr_eqcnt AS
SELECT fo.store_id                                                                        AS event_store_id
     , fa.sub_store_id                                                                    AS vip_store_id
     , fa.is_retail_vip                                                                   AS is_retail_vip
     , IFF(dc.gender='Unknown','F',dc.gender)                                             AS customer_gender
     , dc.is_cross_promo                                                                  AS is_cross_promo
     , dc.finance_specialty_store
     , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE) AS is_scrubs_customer
     , fa.vip_cohort_month_date                                                           AS vip_cohort
     , DATE_TRUNC('MONTH', cast(fo.order_completion_local_datetime AS DATE))                       AS month_date
     , SUM(CASE WHEN foc.order_id IS NOT NULL then foc.billed_cash_credit_redeemed_equivalent_count
           ELSE 0 END)                                                                          AS cancelled_vip_billed_credit_redeemed_equivalent_counts
FROM data_model.fact_order fo
         JOIN data_model.fact_activation fa ON fo.activation_key = fa.activation_key
    AND fo.activation_key <> -1
    AND fo.order_local_datetime >= fa.activation_local_datetime
    AND fo.order_local_datetime > fa.cancellation_local_datetime /*after cancellation*/
         JOIN data_model.dim_store ds ON fa.sub_store_id = ds.store_id
         LEFT JOIN data_model.dim_customer dc ON dc.customer_id = fa.customer_id
         JOIN data_model.dim_order_status dos ON fo.order_status_key = dos.order_status_key
         JOIN data_model.dim_order_sales_channel dosc
              ON fo.order_sales_channel_key = dosc.order_sales_channel_key AND dosc.is_ps_order <> 1
         LEFT JOIN data_model.dim_order_membership_classification domc
                   ON fo.order_membership_classification_key = domc.order_membership_classification_key
         LEFT JOIN stg.fact_order_credit AS foc ON foc.order_id = fo.order_id
WHERE 1 = 1
  AND dos.order_status = 'Success'
  AND dosc.order_classification_l1 = 'Product Order'
  AND fo.order_completion_local_datetime IS NOT NULL
  AND fa.vip_cohort_month_date >= '2010-01-01'
GROUP BY fo.store_id
       , fa.sub_store_id
       , fa.is_retail_vip
       , IFF(dc.gender='Unknown','F',dc.gender)
       , dc.is_cross_promo
       , dc.finance_specialty_store
       , IFF(ds.store_brand = 'Fabletics' AND dc.is_scrubs_customer = TRUE, TRUE, FALSE)
       , fa.vip_cohort_month_date
       , DATE_TRUNC('MONTH', CAST(fo.order_completion_local_datetime AS DATE))
;



CREATE OR REPLACE TEMPORARY TABLE _scaffold AS
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _m1_cancels
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _m2_ltv
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _shipped_orders
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _m2_canc_credits
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _m2_credits_redeemed
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _m1_credits_billers
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _merch_purch
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _merch_purch_placed
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _act_bcr_eqcnt
UNION
SELECT event_store_id,
       vip_store_id,
       is_retail_vip,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       is_scrubs_customer,
       vip_cohort,
       month_date
FROM _canvip_bcr_eqcnt
;


CREATE OR REPLACE TEMPORARY TABLE _final_output AS
SELECT s.event_store_id,
       s.vip_store_id,
       s.is_retail_vip,
       s.customer_gender,
       s.is_cross_promo,
       s.finance_specialty_store,
       s.is_scrubs_customer,
       s.vip_cohort,
       s.month_date,
       'M' || (1 + DATEDIFF(MONTH, s.vip_cohort, s.month_date))       AS tenure,
       CASE WHEN tenure = 'M1' THEN 'Activations' ELSE 'BOP VIPs' END AS metric_type,
       COALESCE(c1.metric_count, ltv.metric_count, 0)                 AS metric_count,
       COALESCE(cb1.credit_billers, ltv.credit_billers, 0)            AS credit_billers,
       COALESCE(c1.cancellations, ltv.cancellations, 0)               AS cancellations,
       COALESCE(ltv.passive_cancel, 0)                                AS passive_cancellations,
       COALESCE(ltv.skips, 0)                                         AS skips,
       COALESCE(ltv.merch_purchasers_ltv, 0)                          AS merch_purchasers_ltv,
       COALESCE(mp.merch_purchasers_fo_shipped, 0)                    AS merch_purchasers_fo_shipped,
       COALESCE(mp.merch_purchasers_fo_shipped_active_vip, 0)         AS merch_purchasers_fo_shipped_active_vip,
       COALESCE(mp.merch_purchasers_fo_shipped_inactive_vip, 0)       AS merch_purchasers_fo_shipped_inactive_vip,
       COALESCE(mpp.merch_purchasers_fo_placed, 0)                    AS merch_purchasers_fo_placed,
       COALESCE(ltv.failed_billing, 0)                                AS failed_billing,
       COALESCE(so.shipped_orders, 0)                                 AS shipped_order_count,
       COALESCE(so.shipped_orders_active_vip,0)                       AS shipped_orders_active_vip,
       COALESCE(so.shipped_orders_inactive_vip,0)                     AS shipped_orders_inactive_vip,
       COALESCE(so.product_gross_revenue, 0)                          AS shipped_order_product_gross_revenue,
       COALESCE(cr2.credits_redeemed, 0)                              AS credits_redeemed,
       COALESCE(cr2.credits_redeemed_amount, 0)                       AS credits_redeemed_amount,
       COALESCE(cc2.cancelled_credits, 0)                             AS credits_cancelled,
       COALESCE(cc2.cancelled_credits_active_vip,0)                   AS cancelled_credits_active_vip,
       COALESCE(cc2.cancelled_credits_inactive_vip,0)                 AS cancelled_credits_inactive_vip,
       COALESCE(aec.activating_billed_credit_redeemed_equivalent_counts, 0) as activating_billed_credit_redeemed_equivalent_counts,
       COALESCE(cec.cancelled_vip_billed_credit_redeemed_equivalent_counts,0) as cancelled_vip_billed_credit_redeemed_equivalent_counts
FROM _scaffold s
         LEFT JOIN _m1_cancels c1 ON s.event_store_id = c1.event_store_id
    AND s.vip_store_id = c1.vip_store_id
    AND s.is_retail_vip = c1.is_retail_vip
    AND s.customer_gender = c1.customer_gender
    AND s.is_cross_promo = c1.is_cross_promo
    AND s.finance_specialty_store = c1.finance_specialty_store
    AND s.is_scrubs_customer = c1.is_scrubs_customer
    AND s.vip_cohort = c1.vip_cohort
    AND s.month_date = c1.month_date
         LEFT JOIN _m2_ltv ltv ON s.event_store_id = ltv.event_store_id
    AND s.vip_store_id = ltv.vip_store_id
    AND s.is_retail_vip = ltv.is_retail_vip
    AND s.customer_gender = ltv.customer_gender
    AND s.is_cross_promo = ltv.is_cross_promo
    AND s.finance_specialty_store = ltv.finance_specialty_store
    AND s.is_scrubs_customer = ltv.is_scrubs_customer
    AND s.vip_cohort = ltv.vip_cohort
    AND s.month_date = ltv.month_date
         LEFT JOIN _shipped_orders so ON s.event_store_id = so.event_store_id
    AND s.vip_store_id = so.vip_store_id
    AND s.is_retail_vip = so.is_retail_vip
    AND s.customer_gender = so.customer_gender
    AND s.is_cross_promo = so.is_cross_promo
    AND s.finance_specialty_store = so.finance_specialty_store
    AND s.is_scrubs_customer = so.is_scrubs_customer
    AND s.vip_cohort = so.vip_cohort
    AND s.month_date = so.month_date
         LEFT JOIN _m2_canc_credits cc2 ON s.event_store_id = cc2.event_store_id
    AND s.vip_store_id = cc2.vip_store_id
    AND s.is_retail_vip = cc2.is_retail_vip
    AND s.customer_gender = cc2.customer_gender
    AND s.is_cross_promo = cc2.is_cross_promo
    AND s.finance_specialty_store = cc2.finance_specialty_store
    AND s.is_scrubs_customer = cc2.is_scrubs_customer
    AND s.vip_cohort = cc2.vip_cohort
    AND s.month_date = cc2.month_date
         LEFT JOIN _m2_credits_redeemed cr2 ON s.event_store_id = cr2.event_store_id
    AND s.vip_store_id = cr2.vip_store_id
    AND s.is_retail_vip = cr2.is_retail_vip
    AND s.customer_gender = cr2.customer_gender
    AND s.is_cross_promo = cr2.is_cross_promo
    AND s.finance_specialty_store = cr2.finance_specialty_store
    AND s.is_scrubs_customer = cr2.is_scrubs_customer
    AND s.vip_cohort = cr2.vip_cohort
    AND s.month_date = cr2.month_date
         LEFT JOIN _m1_credits_billers cb1 ON s.event_store_id = cb1.event_store_id
    AND s.vip_store_id = cb1.vip_store_id
    AND s.is_retail_vip = cb1.is_retail_vip
    AND s.customer_gender = cb1.customer_gender
    AND s.is_cross_promo = cb1.is_cross_promo
    AND s.finance_specialty_store = cb1.finance_specialty_store
    AND s.is_scrubs_customer = cb1.is_scrubs_customer
    AND s.vip_cohort = cb1.vip_cohort
    AND s.month_date = cb1.month_date
         LEFT JOIN _merch_purch mp ON s.event_store_id = mp.event_store_id
    AND s.vip_store_id = mp.vip_store_id
    AND s.is_retail_vip = mp.is_retail_vip
    AND s.customer_gender = mp.customer_gender
    AND s.is_cross_promo = mp.is_cross_promo
    AND s.finance_specialty_store = mp.finance_specialty_store
    AND s.is_scrubs_customer = mp.is_scrubs_customer
    AND s.vip_cohort = mp.vip_cohort
    AND s.month_date = mp.month_date
         LEFT JOIN _merch_purch_placed mpp ON s.event_store_id = mpp.event_store_id
    AND s.vip_store_id = mpp.vip_store_id
    AND s.is_retail_vip = mpp.is_retail_vip
    AND s.customer_gender = mpp.customer_gender
    AND s.is_cross_promo = mpp.is_cross_promo
    AND s.finance_specialty_store = mpp.finance_specialty_store
    AND s.is_scrubs_customer = mpp.is_scrubs_customer
    AND s.vip_cohort = mpp.vip_cohort
    AND s.month_date = mpp.month_date
           LEFT JOIN _act_bcr_eqcnt aec ON s.event_store_id = aec.event_store_id
    AND s.vip_store_id = aec.vip_store_id
    AND s.is_retail_vip = aec.is_retail_vip
    AND s.customer_gender = aec.customer_gender
    AND s.is_cross_promo = aec.is_cross_promo
    AND s.finance_specialty_store = aec.finance_specialty_store
    AND s.is_scrubs_customer = aec.is_scrubs_customer
    AND s.vip_cohort = aec.vip_cohort
    AND s.month_date = aec.month_date
         LEFT JOIN _canvip_bcr_eqcnt cec ON s.event_store_id = cec.event_store_id
    AND s.vip_store_id = cec.vip_store_id
    AND s.is_retail_vip = cec.is_retail_vip
    AND s.customer_gender = cec.customer_gender
    AND s.is_cross_promo = cec.is_cross_promo
    AND s.finance_specialty_store = cec.finance_specialty_store
    AND s.is_scrubs_customer = cec.is_scrubs_customer
    AND s.vip_cohort = cec.vip_cohort
    AND s.month_date = cec.month_date
;



CREATE OR REPLACE TEMP TABLE _final_output_mapping AS
SELECT vip_cohort
     , month_date
     , tenure
     , fo.metric_type
     , fsm.store_brand
     , report_mapping
     , SUM(metric_count)          AS metric_count
     , SUM(credit_billers)        AS credit_billers
     , SUM(cancellations)         AS cancellations
     , SUM(passive_cancellations) AS passive_cancellations
     , SUM(skips)                 AS skips
     , 0                          AS merch_purchasers_ltv
     , 0                          AS merch_purchasers_fo_shipped
     , 0                          AS merch_purchasers_fo_shipped_active_vip
     , 0                          AS merch_purchasers_fo_shipped_inactive_vip
     , 0                          AS merch_purchasers_fo_placed
     , SUM(failed_billing)        AS failed_billing
     , 0                          AS shipped_order_count
     , 0                          AS shipped_orders_active_vip
     , 0                          AS shipped_orders_inactive_vip
     , 0                          AS shipped_order_product_gross_revenue
     , 0                          AS credits_redeemed
     , 0                          AS credits_redeemed_amount
     , SUM(credits_cancelled)     AS credits_cancelled
     , SUM(cancelled_credits_active_vip) AS cancelled_credits_active_vip
     , SUM(cancelled_credits_inactive_vip) AS cancelled_credits_inactive_vip
     , 0                          AS activating_billed_credit_redeemed_equivalent_counts
     , 0                          AS cancelled_vip_billed_credit_redeemed_equivalent_counts
FROM _final_output fo
         LEFT JOIN reference.finance_segment_mapping fsm ON fo.vip_store_id = fsm.vip_store_id
    AND fo.event_store_id = fsm.event_store_id
    AND fo.is_retail_vip = fsm.is_retail_vip
    AND fo.is_cross_promo = fsm.is_cross_promo
    AND fo.customer_gender = fsm.customer_gender
    AND fo.finance_specialty_store = fsm.finance_specialty_store
    AND fo.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.metric_type = 'Acquisition'
    AND fsm.is_vip_tenure = 1
GROUP BY vip_cohort
       , month_date
       , tenure
       , fo.metric_type
       , fsm.store_brand
       , report_mapping

UNION ALL

SELECT vip_cohort
     , month_date
     , tenure
     , fo.metric_type
     , fsm.store_brand
     , report_mapping
     , 0                                                           AS metric_count
     , 0                                                           AS credit_billers
     , 0                                                           AS cancellations
     , 0                                                           AS passive_cancellations
     , 0                                                           AS skips
     , SUM(merch_purchasers_ltv)                                   AS merch_purchasers_ltv
     , SUM(merch_purchasers_fo_shipped)                            AS merch_purchasers_fo_shipped
     , SUM(merch_purchasers_fo_shipped_active_vip)                 AS merch_purchasers_fo_shipped_active_vip
     , SUM(merch_purchasers_fo_shipped_inactive_vip)               AS merch_purchasers_fo_shipped_inactive_vip
     , SUM(merch_purchasers_fo_placed)                             AS merch_purchasers_fo_placed
     , 0                                                           AS failed_billing
     , SUM(shipped_order_count)                                    AS shipped_order_count
     , SUM(shipped_orders_active_vip)                              AS shipped_orders_active_vip
     , SUM(shipped_orders_inactive_vip)                            AS shipped_orders_inactive_vip
     , SUM(shipped_order_product_gross_revenue)                    AS shipped_order_product_gross_revenue
     , SUM(credits_redeemed)                                       AS credits_redeemed
     , SUM(credits_redeemed_amount)                                AS credits_redeemed_amount
     , 0                                                           AS credits_cancelled
     , 0                                                           AS cancelled_credits_active_vip
     , 0                                                           AS cancelled_credits_inactive_vip
     , SUM(activating_billed_credit_redeemed_equivalent_counts)    AS activating_billed_credit_redeemed_equivalent_counts
     , SUM(cancelled_vip_billed_credit_redeemed_equivalent_counts) AS cancelled_vip_billed_credit_redeemed_equivalent_counts
FROM _final_output fo
         LEFT JOIN reference.finance_segment_mapping fsm ON fo.vip_store_id = fsm.vip_store_id
    AND fo.event_store_id = fsm.event_store_id
    AND fo.is_retail_vip = fsm.is_retail_vip
    AND fo.is_cross_promo = fsm.is_cross_promo
    AND fo.customer_gender = fsm.customer_gender
    AND fo.finance_specialty_store = fsm.finance_specialty_store
    AND fo.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.metric_type = 'Orders'
    AND fsm.is_vip_tenure = 1
GROUP BY vip_cohort
       , month_date
       , tenure
       , fo.metric_type
       , fsm.store_brand
       , report_mapping
;

CREATE OR REPLACE TABLE reporting.vip_tenure_final_output AS
SELECT vip_cohort
     , month_date
     , tenure
     , metric_type
     , store_brand
     , report_mapping
     , SUM(metric_count)                                           AS metric_count
     , SUM(credit_billers)                                         AS credit_billers
     , SUM(cancellations)                                          AS cancellations
     , SUM(passive_cancellations)                                  AS passive_cancellations
     , SUM(skips)                                                  AS skips
     , SUM(merch_purchasers_ltv)                                   AS merch_purchasers_ltv
     , SUM(merch_purchasers_fo_shipped)                            AS merch_purchasers_fo_shipped
     , SUM(merch_purchasers_fo_shipped_active_vip)                 AS merch_purchasers_fo_shipped_active_vip
     , SUM(merch_purchasers_fo_shipped_inactive_vip)               AS merch_purchasers_fo_shipped_inactive_vip
     , SUM(merch_purchasers_fo_placed)                             AS merch_purchasers_fo_placed
     , SUM(failed_billing)                                         AS failed_billing
     , SUM(shipped_order_count)                                    AS shipped_order_count
     , SUM(shipped_orders_active_vip)                              AS shipped_order_count_active_vip
     , SUM(shipped_orders_inactive_vip)                            AS shipped_order_count_inactive_vip
     , SUM(shipped_order_product_gross_revenue)                    AS shipped_order_product_gross_revenue
     , SUM(credits_redeemed)                                       AS credits_redeemed
     , SUM(credits_redeemed_amount)                                AS credits_redeemed_amount
     , SUM(credits_cancelled)                                      AS credits_cancelled
     , SUM(cancelled_credits_active_vip)                           AS credits_cancelled_active_vip
     , SUM(cancelled_credits_inactive_vip)                         AS credits_cancelled_inactive_vip
     , SUM(activating_billed_credit_redeemed_equivalent_counts)    AS activating_billed_credit_redeemed_equivalent_counts
     , SUM(cancelled_vip_billed_credit_redeemed_equivalent_counts) AS cancelled_vip_billed_credit_redeemed_equivalent_counts
FROM _final_output_mapping
GROUP BY vip_cohort
       , month_date
       , tenure
       , metric_type
       , store_brand
       , report_mapping
;


INSERT INTO snapshot.vip_tenure_final_output (vip_cohort,
                                              month_date,
                                              tenure,
                                              metric_type,
                                              store_brand,
                                              report_mapping,
                                              metric_count,
                                              credit_billers,
                                              cancellations,
                                              passive_cancellations,
                                              skips,
                                              merch_purchasers_ltv,
                                              merch_purchasers_fo_shipped,
                                              merch_purchasers_fo_placed,
                                              failed_billing,
                                              shipped_order_count,
                                              shipped_order_product_gross_revenue,
                                              credits_redeemed,
                                              credits_redeemed_amount,
                                              credits_cancelled,
                                              activating_billed_credit_redeemed_equivalent_counts,
                                              cancelled_vip_billed_credit_redeemed_equivalent_counts,
                                              merch_purchasers_fo_shipped_active_vip,
                                              merch_purchasers_fo_shipped_inactive_vip,
                                              shipped_order_count_active_vip,
                                              shipped_order_count_inactive_vip,
                                              credits_cancelled_active_vip,
                                              credits_cancelled_inactive_vip,
                                              snapshot_datetime)
SELECT vip_cohort,
       month_date,
       tenure,
       metric_type,
       store_brand,
       report_mapping,
       metric_count,
       credit_billers,
       cancellations,
       passive_cancellations,
       skips,
       merch_purchasers_ltv,
       merch_purchasers_fo_shipped,
       merch_purchasers_fo_placed,
       failed_billing,
       shipped_order_count,
       shipped_order_product_gross_revenue,
       credits_redeemed,
       credits_redeemed_amount,
       credits_cancelled,
       activating_billed_credit_redeemed_equivalent_counts,
       cancelled_vip_billed_credit_redeemed_equivalent_counts,
       merch_purchasers_fo_shipped_active_vip,
       merch_purchasers_fo_shipped_inactive_vip,
       shipped_order_count_active_vip,
       shipped_order_count_inactive_vip,
       credits_cancelled_active_vip,
       credits_cancelled_inactive_vip,
       $execution_start_time AS snapshot_datetime
FROM reporting.vip_tenure_final_output;

DELETE
FROM snapshot.vip_tenure_final_output
WHERE snapshot_datetime < dateadd(MONTH, -18, getdate());
