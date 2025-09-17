SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
SET end_date = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _vip_first_activation AS
SELECT dv.business_unit             AS store_brand_name,
       dv.country                   AS store_country_abbr,
       dv.region                    AS store_region_abbr,
       st.store_id,
       dv.customer_id,
       dv.first_activating_cohort   AS vip_cohort,
       DATEDIFF(MONTH, dv.first_activating_date, COALESCE(dv.recent_vip_cancellation_date, CURRENT_DATE())) +
       1                            AS current_vip_tenure,
       DATEDIFF(MONTH, dv.registration_date, COALESCE(dv.first_activating_date, CURRENT_DATE())) +
       1                            AS lead_tenure,
       dv.current_membership_status AS is_active,
       dv.registration_date,
       dv.first_activating_date
FROM reporting_prod.gfb.gfb_dim_vip dv
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = dv.store_id
WHERE dv.first_activating_cohort >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _order_info AS
SELECT vfa.vip_cohort                                                                    AS vip_cohort,
       DATE_TRUNC(MONTH, olds.order_date)                                                AS order_month_date,
       olds.business_unit                                                                AS store_brand_name,
       olds.country                                                                      AS store_country_abbr,
       olds.region                                                                       AS store_region_abbr,
       vfa.customer_id,
       dvc.vip_cohort                                                                    AS new_vip_cohort,
       COUNT(DISTINCT CASE
                          WHEN olds.order_type = 'vip activating'
                              THEN olds.order_id END)                                    AS count_activating_orders,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_discount
               ELSE 0 END)                                                               AS sum_activating_order_discount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_shipping_revenue
               ELSE 0 END)                                                               AS sum_activating_shipping_revenue_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.order_line_subtotal
               ELSE 0 END)                                                               AS sum_activating_subtotal_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_non_cash_credit_amount
               ELSE 0 END)                                                               AS sum_activating_non_cash_credit_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_cash_credit_amount
               ELSE 0 END)                                                               AS sum_activating_cash_credit_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_qty_sold
               ELSE 0 END)                                                               AS sum_activating_order_num_items,
       SUM(CASE WHEN olds.order_type = 'vip activating' THEN olds.total_cogs ELSE 0 END) AS sum_activating_order_cogs,
       COUNT(DISTINCT CASE
                          WHEN olds.order_type = 'vip repeat'
                              THEN olds.order_id END)                                    AS count_repeat_orders,
       SUM(CASE WHEN olds.order_type = 'vip repeat' THEN olds.total_discount ELSE 0 END) AS sum_repeat_discount_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_shipping_revenue
               ELSE 0 END)                                                               AS sum_repeat_shipping_revenue_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.order_line_subtotal
               ELSE 0 END)                                                               AS sum_repeat_subtotal_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_non_cash_credit_amount
               ELSE 0 END)                                                               AS sum_repeat_non_cash_credit_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_cash_credit_amount
               ELSE 0 END)                                                               AS sum_repeat_cash_credit_amount,
       SUM(CASE WHEN olds.order_type = 'vip repeat' THEN olds.total_qty_sold ELSE 0 END) AS sum_repeat_num_items,
       COUNT(DISTINCT CASE
                          WHEN olds.order_type = 'vip repeat' AND DAYOFMONTH(olds.order_date) < 6
                              THEN olds.order_id END)                                    AS count_repeat_orders_first_5_days,
       SUM(CASE WHEN olds.order_type = 'vip repeat' THEN olds.total_cogs ELSE 0 END)     AS sum_repeat_cogs,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.cash_collected_amount - olds.total_shipping_revenue
               ELSE 0 END)                                                               AS sum_repeat_cash_collected_excl_shipping
FROM gfb.gfb_order_line_data_set_place_date olds
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = olds.customer_id
         JOIN gfb.gfb_dim_vip_cohort_monthly dvc
              ON dvc.customer_id = olds.customer_id
                  AND dvc.month_date = DATE_TRUNC(MONTH, olds.order_date)
WHERE olds.order_classification = 'product order'
  AND olds.order_type IN ('vip activating', 'vip repeat')
  AND olds.order_date >= $start_date
  AND olds.order_date < $end_date
GROUP BY vfa.vip_cohort,
         DATE_TRUNC(MONTH, olds.order_date),
         olds.business_unit,
         olds.country,
         olds.region,
         vfa.customer_id,
         dvc.vip_cohort;

CREATE OR REPLACE TEMPORARY TABLE _customer_activity AS
SELECT vfa.vip_cohort                                        AS vip_cohort,
       cltv.month_date                                       AS period_month_date,
       st.store_brand                                        AS store_brand_name,
       vfa.store_country_abbr,
       st.store_region                                       AS store_region_abbr,
       vfa.customer_id,
       dvc.vip_cohort                                        AS new_vip_cohort,
       COUNT(DISTINCT CASE
                          WHEN cltv.customer_action_category LIKE '%Merch Purchase%'
                              THEN cltv.customer_id END)     AS count_merch_purchases,
       COUNT(DISTINCT CASE
                          WHEN cltv.customer_action_category LIKE '%Skip%'
                              THEN cltv.customer_id END)     AS count_skips,
       COUNT(DISTINCT CASE
                          WHEN cltv.customer_action_category LIKE '%Successful Billing%'
                              THEN cltv.customer_id END)     AS count_credit_billings,
       SUM(CASE
               WHEN cltv.customer_action_category LIKE '%Successful Billing%'
                   THEN cltv.billing_cash_gross_revenue END) AS credit_billings,
       SUM(CASE WHEN cltv.customer_action_category LIKE '%Successful Billing%' THEN cltv.billing_cash_gross_revenue END)
           - SUM(cltv.billed_cash_credit_redeemed_amount)
           - SUM(cltv.billed_cash_credit_cancelled_amount)   AS net_credit_billings,
       SUM(cltv.billed_cash_credit_redeemed_amount)          AS net_credit_redemptions,
       SUM(cltv.billed_cash_credit_cancelled_amount)         AS credit_cancellation,
       COUNT(DISTINCT CASE
                          WHEN cltv.customer_action_category LIKE '%Failed Billing%'
                              THEN cltv.customer_id END)     AS count_failed_credit_billings,
       COUNT(DISTINCT CASE
                          WHEN cltv.is_bop_vip = 1
                              THEN cltv.customer_id END)     AS count_bop_vips
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_jfb cltv
         JOIN gfb.vw_store st
              ON st.store_id = cltv.store_id
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = cltv.meta_original_customer_id
         JOIN gfb.gfb_dim_vip_cohort_monthly dvc
              ON dvc.customer_id = cltv.meta_original_customer_id
                  AND dvc.month_date = cltv.month_date
WHERE cltv.month_date >= $start_date
  AND cltv.month_date < $end_date
  AND vfa.vip_cohort != cltv.month_date
GROUP BY vfa.vip_cohort,
         cltv.month_date,
         st.store_brand,
         vfa.store_country_abbr,
         st.store_region,
         vfa.customer_id,
         dvc.vip_cohort;

CREATE OR REPLACE TEMPORARY TABLE _vip_activations AS
SELECT vfa.vip_cohort                                                                AS vip_cohort,
       DATE_TRUNC(MONTH, CAST(fma.event_start_local_datetime AS DATE))               AS month_date,
       st.store_brand                                                                AS store_brand_name,
       vfa.store_country_abbr,
       st.store_region                                                               AS store_region_abbr,
       vfa.customer_id,
       dvc.vip_cohort                                                                AS new_vip_cohort,
       SUM(CASE WHEN dmet.membership_event_type IN ('Activation') THEN 1 ELSE 0 END) AS count_activations,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Activation') AND fa.is_reactivated_vip = 1 THEN 1
               ELSE 0 END)                                                           AS count_reactivations,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Cancellation') THEN 1
               ELSE 0 END)                                                           AS count_vip_cancellations,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Cancellation') AND fma.membership_type_detail = 'Passive'
                   THEN 1
               ELSE 0 END)                                                           AS count_vip_passive_cancellations,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Failed Activation') THEN 1
               ELSE 0 END)                                                           AS count_failed_activation,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Activation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 10 THEN 1
               ELSE 0 END)                                                           AS count_activations_first_9_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Cancellation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 10 THEN 1
               ELSE 0 END)                                                           AS count_vip_cancellations_first_9_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Failed Activation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 10 THEN 1
               ELSE 0 END)                                                           AS count_failed_activation_first_9_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Activation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 6 THEN 1
               ELSE 0 END)                                                           AS count_activations_first_5_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Cancellation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 6 THEN 1
               ELSE 0 END)                                                           AS count_vip_cancellations_first_5_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Failed Activation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 6 THEN 1
               ELSE 0 END)                                                           AS count_failed_activation_first_5_days
FROM edw_prod.data_model_jfb.fact_membership_event fma
         JOIN gfb.vw_store st
              ON st.store_id = fma.store_id
         JOIN edw_prod.data_model_jfb.dim_membership_event_type dmet
              ON dmet.membership_event_type_key = fma.membership_event_type_key
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = fma.customer_id
         LEFT JOIN edw_prod.data_model_jfb.fact_activation fa
                   ON fa.membership_event_key = fma.membership_event_key
         JOIN gfb.gfb_dim_vip_cohort_monthly dvc
              ON dvc.customer_id = fma.customer_id
                  AND dvc.month_date = DATE_TRUNC(MONTH, CAST(fma.event_start_local_datetime AS DATE))
WHERE fma.event_start_local_datetime >= $start_date
  AND fma.event_start_local_datetime < $end_date
GROUP BY vfa.vip_cohort,
         DATE_TRUNC(MONTH, CAST(fma.event_start_local_datetime AS DATE)),
         st.store_brand,
         vfa.store_country_abbr,
         st.store_region,
         vfa.customer_id,
         dvc.vip_cohort
HAVING count_activations > 0
    OR count_vip_cancellations > 0;

CREATE OR REPLACE TEMPORARY TABLE _most_recent_activating_order AS
SELECT customer_id
     , MAX(order_id) AS max_activating_order
FROM gfb.gfb_order_line_data_set_place_date
WHERE order_type = 'vip activating'
  AND order_classification = 'product order'
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _nonactivating_orders AS
SELECT DISTINCT customer_id, order_id
FROM gfb.gfb_order_line_data_set_place_date
WHERE order_classification = 'product order'
  AND order_type != 'vip activating';

CREATE OR REPLACE TEMPORARY TABLE _activating_order_date AS
SELECT DISTINCT customer_id, order_id, order_date
FROM gfb.gfb_order_line_data_set_place_date
WHERE order_classification = 'product order'
  AND order_type = 'vip activating';

CREATE OR REPLACE TEMPORARY TABLE _cancellations AS
SELECT fma.customer_id, MAX(fma.event_start_local_datetime::DATE) AS cancel_date
FROM edw_prod.data_model_jfb.fact_membership_event fma
         JOIN gfb.vw_store st
              ON st.store_id = fma.store_id
         JOIN edw_prod.data_model_jfb.dim_membership_event_type dmet
              ON dmet.membership_event_type_key = fma.membership_event_type_key
WHERE dmet.membership_event_type = 'Cancellation'
GROUP BY fma.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _cancel_after_one_purchase AS
SELECT ao.customer_id,
       DATE_TRUNC('month', aod.order_date) AS order_month,
       DATE_TRUNC('month', c.cancel_date)  AS cancel_month
FROM _most_recent_activating_order ao
         LEFT JOIN _nonactivating_orders n ON ao.customer_id = n.customer_id
    AND ao.max_activating_order < n.order_id
         LEFT JOIN _activating_order_date aod ON ao.customer_id = aod.customer_id
    AND ao.max_activating_order = aod.order_id
         LEFT JOIN _cancellations c ON ao.customer_id = c.customer_id
    AND aod.order_date <= c.cancel_date
WHERE n.order_id IS NULL
  AND c.cancel_date IS NOT NULL;

CREATE OR REPLACE TEMPORARY TABLE _ltv AS
SELECT vfa.vip_cohort                                                   AS vip_cohort,
       clva.month_date,
       st.store_brand                                                   AS store_brand_name,
       vfa.store_country_abbr,
       st.store_region                                                  AS store_region_abbr,
       vfa.customer_id,
       dvc.vip_cohort                                                   AS new_vip_cohort,
       SUM(clva.cash_gross_profit)                                      AS sum_cgm,
       SUM(clva.product_gross_profit)                                   AS sum_gaap_gross_margin,
       SUM(clva.cash_net_revenue)                                       AS sum_cnr,
       SUM(clva.product_order_landed_product_cost_amount)               AS sum_cogs,
       SUM(clva.monthly_billed_credit_cash_refund_chargeback_amount +
           clva.product_order_cash_refund_amount_and_chargeback_amount) AS sum_refunds_and_chargebacks_amount
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust_jfb AS clvc
         LEFT JOIN edw_prod.analytics_base.customer_lifetime_value_monthly_agg_jfb AS clva
                   ON clva.month_date = clvc.month_date
                       AND clva.customer_id = clvc.customer_id
                       AND clva.activation_key = clvc.activation_key
                       AND clva.vip_store_id = clvc.vip_store_id
         JOIN gfb.vw_store st
              ON st.store_id = clvc.store_id
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = clvc.meta_original_customer_id
         JOIN gfb.gfb_dim_vip_cohort_monthly dvc
              ON dvc.customer_id = clvc.meta_original_customer_id
                  AND dvc.month_date = clva.month_date
WHERE clvc.month_date >= $start_date
  AND clvc.month_date < $end_date
  AND clva.cash_gross_profit != 0
GROUP BY vfa.vip_cohort,
         clva.month_date,
         st.store_brand,
         vfa.store_country_abbr,
         st.store_region,
         vfa.customer_id,
         dvc.vip_cohort;

CREATE OR REPLACE TEMPORARY TABLE _membership_credit_activity AS
SELECT vfa.vip_cohort                                                                       AS vip_cohort,
       DATE_TRUNC(MONTH, CAST(ce.credit_activity_local_datetime AS DATE))                   AS month_date,
       s.store_brand                                                                        AS store_brand_name,
       vfa.store_country_abbr,
       s.store_region                                                                       AS store_region_abbr,
       vfa.customer_id,
       dvc.vip_cohort                                                                       AS new_vip_cohort,
       COUNT(DISTINCT CASE WHEN ce.credit_activity_type = 'Redeemed' THEN c.credit_id END)  AS count_credits_redeemed,
       COUNT(DISTINCT CASE WHEN ce.credit_activity_type = 'Cancelled' THEN c.credit_id END) AS count_credits_cancelled,
       SUM(CASE
               WHEN ce.credit_activity_type = 'Redeemed' THEN ce.credit_activity_local_amount *
                                                              ce.credit_activity_usd_conversion_rate
               ELSE 0 END)                                                                  AS credit_redeemed_amount,
       COUNT(DISTINCT CASE
                          WHEN LOWER(ce.credit_activity_type_reason) LIKE '%chargeback%'
                              THEN c.credit_id END)                                         AS count_credits_cancelled_chargeback
FROM edw_prod.data_model_jfb.fact_credit_event ce
         JOIN edw_prod.data_model_jfb.dim_credit c
              ON ce.credit_key = c.credit_key
         JOIN gfb.vw_store s
              ON c.store_id = s.store_id
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = c.customer_id
         JOIN gfb.gfb_dim_vip_cohort_monthly dvc
              ON dvc.customer_id = c.customer_id
                  AND dvc.month_date = DATE_TRUNC(MONTH, CAST(ce.credit_activity_local_datetime AS DATE))
WHERE ce.credit_activity_local_datetime >= $start_date
  AND ce.credit_activity_local_datetime < $end_date
  AND c.credit_reason = 'Membership Credit'
  AND ce.credit_activity_type IN ('Redeemed', 'Cancelled')
GROUP BY vfa.vip_cohort,
         DATE_TRUNC(MONTH, CAST(ce.credit_activity_local_datetime AS DATE)),
         s.store_brand,
         vfa.store_country_abbr,
         s.store_region,
         vfa.customer_id,
         dvc.vip_cohort;

CREATE OR REPLACE TEMPORARY TABLE _base AS
SELECT DISTINCT COALESCE(oi.vip_cohort, ca.vip_cohort, va.vip_cohort, ltv.vip_cohort, mca.vip_cohort) AS vip_cohort,
                COALESCE(oi.order_month_date, ca.period_month_date, va.month_date, ltv.month_date,
                         mca.month_date)                                                              AS activity_month,
                UPPER(COALESCE(oi.store_brand_name, ca.store_brand_name, va.store_brand_name, ltv.store_brand_name,
                               mca.store_brand_name))                                                 AS store_brand_name,
                UPPER(COALESCE(oi.store_country_abbr, ca.store_country_abbr, va.store_country_abbr,
                               ltv.store_country_abbr,
                               mca.store_country_abbr))                                               AS store_country,
                UPPER(COALESCE(oi.store_region_abbr, ca.store_region_abbr, va.store_region_abbr, ltv.store_region_abbr,
                               mca.store_region_abbr))                                                AS store_region_abbr,
                COALESCE(oi.customer_id, ca.customer_id, va.customer_id, ltv.customer_id,
                         mca.customer_id)                                                             AS customer_id,
                COALESCE(oi.new_vip_cohort, ca.new_vip_cohort, va.new_vip_cohort, ltv.new_vip_cohort,
                         mca.new_vip_cohort)                                                          AS new_vip_cohort
FROM _order_info oi
         FULL JOIN _customer_activity ca
                   ON ca.customer_id = oi.customer_id
                       AND ca.period_month_date = oi.order_month_date
         FULL JOIN _vip_activations va
                   ON va.customer_id = oi.customer_id
                       AND va.month_date = oi.order_month_date
         FULL JOIN _ltv ltv
                   ON ltv.customer_id = oi.customer_id
                       AND ltv.month_date = oi.order_month_date
         FULL JOIN _membership_credit_activity mca
                   ON mca.customer_id = oi.customer_id
                       AND mca.month_date = oi.order_month_date;

CREATE OR REPLACE TEMPORARY TABLE _aged_lead AS
SELECT vfa.customer_id,
       DATEDIFF(DAY, vfa.registration_date, COALESCE(vfa.first_activating_date, CURRENT_DATE())) +
       1                              AS lead_tenure_by_day,
       (CASE
            WHEN lead_tenure_by_day > 7 THEN 'Aged Lead'
            ELSE 'Non Aged Lead' END) AS aged_lead_flag
FROM _vip_first_activation vfa
WHERE lead_tenure_by_day > 0;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb001_customer_lifetime_value AS
SELECT COALESCE(cs.vip_cohort, b.vip_cohort)                        AS vip_cohort,
       b.activity_month,
       b.store_brand_name,
       b.store_brand_name                                           AS store_name,
       b.store_country,
       b.store_region_abbr,
       cs.custom_segment_category,
       cs.custom_segment,
       gamers.custom_segment                                        AS is_gamer_flag,
       ppcc.custom_segment                                          AS is_ppcc_activating,
       al.aged_lead_flag,
       b.new_vip_cohort,
       COUNT(DISTINCT cs.customer_id)                               AS customers_count,
       SUM(COALESCE(oi.count_activating_orders, 0))                 AS count_activating_orders,
       SUM(COALESCE(oi.sum_activating_order_discount, 0))           AS sum_activating_order_discount,
       SUM(COALESCE(oi.sum_activating_shipping_revenue_amount, 0))  AS sum_activating_shipping_revenue_amount,
       SUM(COALESCE(oi.sum_activating_subtotal_amount, 0))          AS sum_activating_subtotal_amount,
       SUM(COALESCE(oi.sum_activating_non_cash_credit_amount, 0))   AS sum_activating_non_cash_credit_amount,
       SUM(COALESCE(oi.sum_activating_cash_credit_amount, 0))       AS sum_activating_cash_credit_amount,
       SUM(COALESCE(oi.sum_activating_order_num_items, 0))          AS sum_activating_order_num_items,
       SUM(COALESCE(oi.sum_activating_order_cogs, 0))               AS sum_activating_order_cogs,
       SUM(COALESCE(oi.count_repeat_orders, 0))                     AS count_repeat_orders,
       SUM(COALESCE(oi.sum_repeat_discount_amount, 0))              AS sum_repeat_discount_amount,
       SUM(COALESCE(oi.sum_repeat_shipping_revenue_amount, 0))      AS sum_repeat_shipping_revenue_amount,
       SUM(COALESCE(oi.sum_repeat_subtotal_amount, 0))              AS sum_repeat_subtotal_amount,
       SUM(COALESCE(oi.sum_repeat_non_cash_credit_amount, 0))       AS sum_repeat_non_cash_credit_amount,
       SUM(COALESCE(oi.sum_repeat_cash_credit_amount, 0))           AS sum_repeat_cash_credit_amount,
       SUM(COALESCE(oi.sum_repeat_num_items, 0))                    AS sum_repeat_num_items,
       COUNT(DISTINCT CASE
                          WHEN oi.count_repeat_orders_first_5_days > 0
                              THEN oi.customer_id END)              AS purchased_customer_count_first_5_days,
       SUM(COALESCE(oi.sum_repeat_cogs, 0))                         AS sum_repeat_cogs,
       SUM(COALESCE(oi.sum_repeat_cash_collected_excl_shipping, 0)) AS sum_repeat_cash_collected_excl_shipping,
       SUM(COALESCE(ca.count_merch_purchases, 0))                   AS count_merch_purchases,
       SUM(COALESCE(ca.count_skips, 0))                             AS count_skips,
       SUM(COALESCE(ca.count_credit_billings, 0))                   AS count_credit_billings,
       SUM(COALESCE(ca.count_failed_credit_billings, 0))            AS count_failed_credit_billings,
       SUM(COALESCE(ca.count_bop_vips, 0))                          AS count_bop_vips,
       SUM(COALESCE(va.count_activations, 0) -
           COALESCE(va.count_failed_activation, 0))                 AS count_activations,
       SUM(COALESCE(va.count_reactivations, 0))                     AS count_reactivations,
       SUM(COALESCE(ltv.sum_cgm, 0))                                AS sum_cash_gross_margin,
       SUM(COALESCE(ltv.sum_gaap_gross_margin, 0))                  AS sum_gaap_gross_margin,
       SUM(COALESCE(ltv.sum_cnr, 0))                                AS sum_cash_net_revenue,
       SUM(COALESCE(ltv.sum_cogs, 0))                               AS sum_cogs,
       SUM(COALESCE(sum_refunds_and_chargebacks_amount, 0))         AS sum_refunds_and_chargebacks_amount,
       SUM(COALESCE(mca.count_credits_cancelled, 0))                AS count_credits_cancelled,
       SUM(COALESCE(mca.count_credits_redeemed, 0))                 AS count_credits_redeemed,
       SUM(COALESCE(mca.count_credits_cancelled_chargeback, 0))     AS count_credits_cancelled_chargeback,
       SUM(COALESCE(va.count_vip_cancellations, 0))                 AS count_vip_cancellations,
       SUM(COALESCE(va.count_vip_passive_cancellations, 0))         AS count_vip_passive_cancellations,
       SUM(COALESCE(va.count_activations_first_9_days, 0) -
           COALESCE(va.count_failed_activation_first_9_days, 0))    AS count_activations_first_9_days,
       SUM(COALESCE(va.count_vip_cancellations_first_9_days, 0))    AS count_vip_cancellations_first_9_days,
       SUM(COALESCE(va.count_activations_first_5_days, 0) -
           COALESCE(va.count_failed_activation_first_5_days, 0))    AS count_activations_first_5_days,
       SUM(COALESCE(va.count_vip_cancellations_first_5_days, 0))    AS count_vip_cancellations_first_5_days,
       SUM(COALESCE(ca.credit_billings, 0))                         AS credit_billings,
       SUM(COALESCE(ca.net_credit_billings, 0))                     AS net_credit_billings,
       SUM(COALESCE(ca.credit_cancellation, 0))                     AS credit_cancellation,
       SUM(COALESCE(ca.net_credit_redemptions, 0))                  AS net_credit_redemptions,
       COUNT(DISTINCT caop.customer_id)                             AS cancel_after_one_purchase
FROM _base b
         JOIN gfb.gfb001_customer_seg cs
              ON cs.customer_id = b.customer_id
                  AND cs.custom_segment_category NOT IN ('Gamers')
         LEFT JOIN _order_info oi
                   ON oi.customer_id = b.customer_id
                       AND oi.order_month_date = b.activity_month
         LEFT JOIN _customer_activity ca
                   ON ca.customer_id = b.customer_id
                       AND ca.period_month_date = b.activity_month
         LEFT JOIN _vip_activations va
                   ON va.customer_id = b.customer_id
                       AND va.month_date = b.activity_month
         LEFT JOIN _ltv ltv
                   ON ltv.customer_id = b.customer_id
                       AND ltv.month_date = b.activity_month
         LEFT JOIN _membership_credit_activity mca
                   ON mca.customer_id = b.customer_id
                       AND mca.month_date = b.activity_month
         LEFT JOIN gfb.gfb001_customer_seg gamers
                   ON gamers.customer_id = b.customer_id
                       AND gamers.custom_segment_category = 'Gamers'
         LEFT JOIN gfb.gfb001_customer_seg ppcc
                   ON ppcc.customer_id = b.customer_id
                       AND ppcc.custom_segment_category = 'Prepaid Credit Card'
         LEFT JOIN _aged_lead al
                   ON al.customer_id = b.customer_id
         LEFT JOIN _cancel_after_one_purchase caop
                   ON caop.customer_id = b.customer_id
                       AND caop.cancel_month = b.activity_month
WHERE b.vip_cohort <= b.activity_month
GROUP BY COALESCE(cs.vip_cohort, b.vip_cohort),
         b.activity_month,
         b.store_brand_name,
         b.store_brand_name,
         b.store_country,
         b.store_region_abbr,
         cs.custom_segment_category,
         cs.custom_segment,
         gamers.custom_segment,
         ppcc.custom_segment,
         al.aged_lead_flag,
         b.new_vip_cohort;
