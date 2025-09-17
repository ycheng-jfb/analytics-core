SET start_date = CAST('2019-01-01' AS DATE);
SET end_date = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _gfb_dim_vip AS
SELECT customer_id,
       (CASE
            WHEN upgrade_vip_flag = 1 THEN '39.95 to 49.95'
            ELSE REPLACE(CAST(membership_price AS VARCHAR(20)), 0, '')
           END) AS membership_price_group
FROM gfb.gfb_dim_vip;

CREATE OR REPLACE TEMPORARY TABLE _vip_activity AS
SELECT st.store_brand_abbr || st.store_region            AS business_unit,
       cltv.month_date                                   AS period_month_date,
       dv.membership_price_group,
       COUNT(DISTINCT cltv.customer_id)                  AS bop_vip,
       COUNT(DISTINCT CASE
                          WHEN cltv.customer_action_category LIKE '%Skip%'
                              THEN cltv.customer_id END) AS skips
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust cltv
         JOIN gfb.vw_store st
              ON st.store_id = cltv.store_id
         JOIN _gfb_dim_vip dv
              ON dv.customer_id = cltv.meta_original_customer_id
WHERE cltv.month_date >= $start_date
  AND cltv.month_date < $end_date
  AND cltv.is_bop_vip = 1
GROUP BY st.store_brand_abbr || st.store_region,
         cltv.month_date,
         dv.membership_price_group;

CREATE OR REPLACE TEMPORARY TABLE _bop_vip_customers AS
SELECT DISTINCT st.store_id,
                cltv.month_date AS period_month_date,
                dv.customer_id
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust cltv
         JOIN gfb.vw_store st
              ON st.store_id = cltv.store_id
         JOIN _gfb_dim_vip dv
              ON dv.customer_id = cltv.meta_original_customer_id
WHERE cltv.month_date >= $start_date
  AND cltv.month_date < $end_date;

CREATE OR REPLACE TEMPORARY TABLE _vip_repeat_purchase_first_5_days AS
SELECT st.store_brand_abbr || st.store_region AS business_unit,
       dd.month_date,
       dv.membership_price_group,
       COUNT(DISTINCT fo.customer_id)         AS vips_purchased_first_5_days
FROM edw_prod.data_model_jfb.fact_order fo
         JOIN gfb.vw_store st
              ON st.store_id = fo.store_id
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.full_date = CAST(fo.order_local_datetime AS DATE)
         JOIN edw_prod.data_model_jfb.dim_order_membership_classification domc
              ON domc.order_membership_classification_key = fo.order_membership_classification_key
                  AND domc.membership_order_type_l2 = 'Repeat VIP'
         JOIN edw_prod.data_model_jfb.dim_order_status dos
              ON dos.order_status_key = fo.order_status_key
         JOIN edw_prod.data_model_jfb.dim_order_processing_status dops
              ON dops.order_processing_status_key = fo.order_processing_status_key
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fo.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
         JOIN _gfb_dim_vip dv
              ON dv.customer_id = fo.customer_id
WHERE (
            dos.order_status = 'Success'
        OR
            (dos.order_status = 'Pending' AND
             dops.order_processing_status IN ('FulFillment (Batching)', 'FulFillment (In Progress)', 'Placed'))
    )
  AND dosc.order_classification_l1 IN ('Product Order')
  AND dd.full_date >= $start_date
  AND dd.full_date < $end_date
  AND dd.day_of_month < 6
GROUP BY st.store_brand_abbr || st.store_region,
         dd.month_date,
         dv.membership_price_group;

CREATE OR REPLACE TEMPORARY TABLE _vip_cancelation AS
SELECT st.store_brand_abbr || st.store_region                                AS business_unit,
       dd.month_date,
       dv.membership_price_group,
       COUNT(DISTINCT fme.customer_id)                                       AS bop_vip_cancellations,
       COUNT(DISTINCT CASE
                          WHEN dd.day_of_month < 6 THEN fme.customer_id END) AS bop_vip_cancellations_in_first_5_days
FROM edw_prod.data_model_jfb.fact_membership_event fme
         JOIN gfb.vw_store st
              ON st.store_id = fme.store_id
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.full_date = CAST(fme.event_start_local_datetime AS DATE)
                  AND dd.full_date >= $start_date
                  AND dd.full_date < $end_date
         JOIN _bop_vip_customers bvc
              ON bvc.customer_id = fme.customer_id
                  AND bvc.period_month_date = dd.month_date
         JOIN _gfb_dim_vip dv
              ON dv.customer_id = fme.customer_id
WHERE fme.membership_event_type = 'Cancellation'
GROUP BY st.store_brand_abbr || st.store_region,
         dd.month_date,
         dv.membership_price_group;

CREATE OR REPLACE TEMPORARY TABLE _credit_billing AS
SELECT st.store_brand_abbr || st.store_region                                          AS business_unit,
       dd.month_date,
       dv.membership_price_group,
       COUNT(DISTINCT CASE WHEN dos.order_status = 'Success' THEN bvc.customer_id END) AS successful_credit_billings,
       COUNT(DISTINCT CASE
                          WHEN dos.order_status = 'Failure' THEN bvc.customer_id END)  AS failed_credit_billings
FROM edw_prod.data_model_jfb.fact_order fo
         JOIN gfb.vw_store st
              ON st.store_id = fo.store_id
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.full_date = CAST(fo.order_local_datetime AS DATE)
         JOIN _bop_vip_customers bvc
              ON bvc.customer_id = fo.customer_id
                  AND bvc.period_month_date = dd.month_date
         JOIN edw_prod.data_model_jfb.dim_order_status dos
              ON dos.order_status_key = fo.order_status_key
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fo.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
         JOIN _gfb_dim_vip dv
              ON dv.customer_id = fo.customer_id
WHERE dosc.order_classification_l1 IN ('Billing Order')
  AND dd.full_date >= $start_date
  AND dd.full_date < $end_date
GROUP BY st.store_brand_abbr || st.store_region,
         dd.month_date,
         dv.membership_price_group;

CREATE OR REPLACE TEMPORARY TABLE _cancelled_credit_billing AS
SELECT st.store_brand_abbr || st.store_region AS business_unit,
       dd.month_date                          AS month_date,
       dv.membership_price_group,
       SUM(credit_activity_equivalent_count)  AS cancelled_credit_billing_count
FROM edw_prod.data_model_jfb.fact_credit_event fce
         JOIN edw_prod.data_model_jfb.dim_credit dc ON fce.credit_key = dc.credit_key
         JOIN gfb.vw_store st ON st.store_id = dc.store_id
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.full_date = CAST(fce.credit_activity_local_datetime AS DATE)
         JOIN _gfb_dim_vip dv
              ON dv.customer_id = dc.customer_id
WHERE fce.credit_activity_type = 'Cancelled'
  AND is_core_store = TRUE
  AND fce.original_credit_activity_type_action = 'Include'
  AND LOWER(dc.credit_report_mapping) = 'billed credit'
  AND dd.full_date >= $start_date
  AND dd.full_date < $end_date
GROUP BY st.store_brand_abbr || st.store_region,
         dd.month_date,
         dv.membership_price_group;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb007_billing_circle_outcome_stats AS
SELECT va.business_unit,
       va.period_month_date,
       va.membership_price_group,
       va.bop_vip,
       va.skips,
       vrp.vips_purchased_first_5_days,
       va.bop_vip - va.skips - vrp.vips_purchased_first_5_days AS vips_eligible_for_billing,
       cb.successful_credit_billings,
       vc.bop_vip_cancellations,
       vc.bop_vip_cancellations_in_first_5_days,
       cb.failed_credit_billings,
       ccb.cancelled_credit_billing_count                      AS cancelled_credit_billings
FROM _vip_activity va
         LEFT JOIN _vip_repeat_purchase_first_5_days vrp
                   ON vrp.business_unit = va.business_unit
                       AND vrp.month_date = va.period_month_date
                       AND vrp.membership_price_group = va.membership_price_group
         LEFT JOIN _vip_cancelation vc
                   ON vc.business_unit = va.business_unit
                       AND vc.month_date = va.period_month_date
                       AND vc.membership_price_group = va.membership_price_group
         LEFT JOIN _credit_billing cb
                   ON cb.business_unit = va.business_unit
                       AND cb.month_date = va.period_month_date
                       AND cb.membership_price_group = va.membership_price_group
         LEFT JOIN _cancelled_credit_billing ccb
                   ON ccb.business_unit = va.business_unit
                       AND ccb.month_date = va.period_month_date
                       AND ccb.membership_price_group = va.membership_price_group;

CREATE OR REPLACE TEMPORARY TABLE _dim_payment AS
SELECT payment_key,
       (CASE WHEN is_prepaid_creditcard = 1 THEN 'PPCC' ELSE 'Non-PPCC' END) AS is_prepaid_creditcard,
       (CASE
            WHEN creditcard_type ILIKE 'Ap_%' THEN 'Apple Pay'
            WHEN LOWER(creditcard_type) LIKE '%visa%' THEN 'VISA'
            WHEN LOWER(creditcard_type) LIKE '%master card%' THEN 'MASTER CARD'
            WHEN LOWER(creditcard_type) LIKE '%mastercard%' THEN 'MASTER CARD'
            WHEN LOWER(creditcard_type) LIKE '%paypal%' THEN 'PAYPAL'
            WHEN LOWER(creditcard_type) LIKE '%amex%' THEN 'AMEX'
            WHEN LOWER(creditcard_type) LIKE '%discover%' THEN 'DISCOVER'
            ELSE creditcard_type END)                                        AS credit_card_type
FROM edw_prod.data_model_jfb.dim_payment;

CREATE OR REPLACE TEMPORARY TABLE _success_failed AS
SELECT st.store_brand_abbr || st.store_region                                     AS business_unit,
       dd.month_date,
       dpm.is_prepaid_creditcard,
       dpm.credit_card_type,
       dv.membership_price_group,
       COUNT(DISTINCT CASE
                          WHEN fo.order_status_key = 1
                              THEN bvc.customer_id END)                           AS successful_credit_billings,
       COUNT(DISTINCT CASE WHEN fo.order_status_key = 4 THEN bvc.customer_id END) AS failed_credit_billings,
       COUNT(DISTINCT bvc.customer_id)                                            AS total_credit_billing
FROM edw_prod.data_model_jfb.fact_order fo
         JOIN gfb.vw_store st
              ON st.store_id = fo.store_id
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.full_date = CAST(fo.order_local_datetime AS DATE)
         JOIN _bop_vip_customers bvc
              ON bvc.customer_id = fo.customer_id
                  AND bvc.period_month_date = dd.month_date
         JOIN edw_prod.data_model_jfb.dim_order_status dos
              ON dos.order_status_key = fo.order_status_key
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fo.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
         JOIN _dim_payment dpm
              ON dpm.payment_key = fo.payment_key
         JOIN _gfb_dim_vip dv
              ON dv.customer_id = fo.customer_id
WHERE fo.order_status_key IN (1, 2, 3, 4)
  AND dosc.order_classification_l1 IN ('Billing Order')
  AND dd.full_date >= $start_date
  AND dd.full_date < $end_date
GROUP BY st.store_brand_abbr || st.store_region, dd.month_date,
         dpm.is_prepaid_creditcard,
         dpm.credit_card_type,
         dv.membership_price_group;

CREATE OR REPLACE TEMPORARY TABLE _cancel AS
SELECT st.store_brand_abbr || st.store_region AS business_unit,
       dd.month_date                          AS month_date,
       dpm.is_prepaid_creditcard,
       dpm.credit_card_type,
       dv.membership_price_group,
       SUM(credit_activity_equivalent_count)  AS cancelled_credit_billings
FROM edw_prod.data_model_jfb.fact_credit_event fce
         JOIN edw_prod.data_model_jfb.dim_credit dc ON fce.credit_key = dc.credit_key
         JOIN edw_prod.data_model_jfb.fact_order fo ON dc.credit_order_id = fo.order_id
         JOIN gfb.vw_store st ON st.store_id = fo.store_id
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.full_date = CAST(fce.credit_activity_local_datetime AS DATE)
         JOIN _dim_payment dpm
              ON dpm.payment_key = fo.payment_key
         JOIN _gfb_dim_vip dv
              ON dv.customer_id = dc.customer_id
WHERE fce.credit_activity_type = 'Cancelled'
  AND is_core_store = TRUE
  AND fce.original_credit_activity_type_action = 'Include'
  AND LOWER(dc.credit_report_mapping) = 'billed credit'
  AND dd.full_date >= $start_date
  AND dd.full_date < $end_date
GROUP BY st.store_brand_abbr || st.store_region, dd.month_date,
         dpm.is_prepaid_creditcard,
         dpm.credit_card_type,
         dv.membership_price_group;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb007_billing_circle_outcome_credit_card_type AS
SELECT sf.business_unit,
       sf.month_date,
       sf.is_prepaid_creditcard,
       sf.credit_card_type,
       sf.membership_price_group,
       sf.successful_credit_billings,
       sf.failed_credit_billings,
       sf.total_credit_billing,
       c.cancelled_credit_billings
FROM _success_failed sf
         LEFT JOIN _cancel c ON sf.business_unit = c.business_unit
    AND sf.month_date = c.month_date
    AND sf.is_prepaid_creditcard = c.is_prepaid_creditcard
    AND sf.credit_card_type = c.credit_card_type
    AND sf.membership_price_group = c.membership_price_group;
