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
              ON st.store_id = dv.store_id;

CREATE OR REPLACE TEMPORARY TABLE _order_info AS
SELECT vfa.vip_cohort,
       DATE_TRUNC(MONTH, olds.order_date)                     AS order_month_date,
       olds.business_unit                                     AS store_brand_name,
       olds.country                                           AS store_country_abbr,
       olds.region                                            AS store_region_abbr,
       vfa.customer_id,
       COUNT(DISTINCT CASE
                          WHEN olds.order_type = 'vip activating'
                              THEN olds.order_id END)         AS count_activating_orders,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_discount
               ELSE 0 END)                                    AS sum_activating_order_discount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_shipping_revenue
               ELSE 0 END)                                    AS sum_activating_shipping_revenue_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.order_line_subtotal
               ELSE 0 END)                                    AS sum_activating_subtotal_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_non_cash_credit_amount
               ELSE 0 END)                                    AS sum_activating_non_cash_credit_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_cash_credit_amount
               ELSE 0 END)                                    AS sum_activating_cash_credit_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip activating' THEN olds.total_qty_sold
               ELSE 0 END)                                    AS sum_activating_order_num_items,
       COUNT(DISTINCT CASE
                          WHEN olds.order_type = 'vip repeat'
                              THEN olds.order_id END)         AS count_repeat_orders,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_discount
               ELSE 0 END)                                    AS sum_repeat_discount_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_shipping_revenue
               ELSE 0 END)                                    AS sum_repeat_shipping_revenue_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.order_line_subtotal
               ELSE 0 END)                                    AS sum_repeat_subtotal_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_non_cash_credit_amount
               ELSE 0 END)                                    AS sum_repeat_non_cash_credit_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_cash_credit_amount
               ELSE 0 END)                                    AS sum_repeat_cash_credit_amount,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_qty_sold
               ELSE 0 END)                                    AS sum_repeat_num_items,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_cogs
               ELSE 0 END)                                    AS sum_repeat_cogs_items,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_shipping_cost
               ELSE 0 END)                                    AS sum_repeat_shipping_cost,
       SUM(CASE
               WHEN olds.order_type = 'vip repeat' THEN olds.total_product_revenue
               ELSE 0 END)                                    AS sum_repeat_product_revenue,
       COUNT(DISTINCT CASE
                          WHEN olds.order_type = 'vip repeat' AND DAYOFMONTH(olds.order_date) < 6
                              THEN olds.order_id END)         AS count_repeat_orders_first_5_days,
       SUM(olds.total_product_revenue)                        AS total_product_revenue,
       SUM(olds.total_cogs)                                   AS total_cogs,
       SUM(olds.total_product_revenue) - SUM(olds.total_cogs) AS total_product_gm_amount,
       SUM(olds.cash_collected_amount)                        AS cash_collected_amount
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olds
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = olds.customer_id
WHERE olds.order_classification = 'product order'
  AND olds.order_type IN ('vip activating', 'vip repeat')
GROUP BY vfa.vip_cohort,
         DATE_TRUNC(MONTH, olds.order_date),
         olds.business_unit,
         olds.country,
         olds.region,
         vfa.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _customer_activity_base AS
SELECT vfa.vip_cohort                               AS vip_cohort,
       clvc.month_date                              AS period_month_date,
       st.store_brand                               AS store_brand_name,
       vfa.store_country_abbr,
       st.store_region                              AS store_region_abbr,
       vfa.customer_id,
       CASE
           WHEN customer_action_category = 'Merch Purchaser'
               OR customer_action_category = 'Merch Purchase and Skip'
               OR customer_action_category = 'Merch Purchase and Successful Billing'
               OR customer_action_category = 'Merch Purchase and Failed Billing'
               THEN TRUE
           ELSE FALSE
           END                                      AS is_merch_purchaser_new,
       CASE
           WHEN customer_action_category = 'Skip Only'
               OR customer_action_category = 'Merch Purchase and Skip'
               THEN TRUE
           ELSE FALSE
           END                                      AS is_skip_new,
       CASE
           WHEN customer_action_category = 'Successful Billing'
               OR customer_action_category = 'Merch Purchase and Successful Billing'
               THEN TRUE
           ELSE FALSE
           END                                      AS is_successful_billing_new,
       CASE
           WHEN customer_action_category = 'Failed Billing'
               OR customer_action_category = 'Merch Purchase and Failed Billing'
               THEN TRUE
           ELSE FALSE
           END                                      AS is_failed_billing_new,
       is_bop_vip,
       ROW_NUMBER() OVER (PARTITION BY
           vfa.vip_cohort,
           clvc.month_date,
           st.store_brand,
           vfa.store_country_abbr,
           st.store_region,
           vfa.customer_id
           ORDER BY is_merch_purchaser_new DESC)    AS row_num_merch_purchaser,
       ROW_NUMBER() OVER (PARTITION BY
           vfa.vip_cohort,
           clvc.month_date,
           st.store_brand,
           vfa.store_country_abbr,
           st.store_region,
           vfa.customer_id
           ORDER BY is_skip_new DESC)               AS row_num_skip,
       ROW_NUMBER() OVER (PARTITION BY
           vfa.vip_cohort,
           clvc.month_date,
           st.store_brand,
           vfa.store_country_abbr,
           st.store_region,
           vfa.customer_id
           ORDER BY is_successful_billing_new DESC) AS row_num_successful,
       ROW_NUMBER() OVER (PARTITION BY
           vfa.vip_cohort,
           clvc.month_date,
           st.store_brand,
           vfa.store_country_abbr,
           st.store_region,
           vfa.customer_id
           ORDER BY is_failed_billing_new DESC)     AS row_num_failed,
       ROW_NUMBER() OVER (PARTITION BY
           vfa.vip_cohort,
           clvc.month_date,
           st.store_brand,
           vfa.store_country_abbr,
           st.store_region,
           vfa.customer_id
           ORDER BY is_bop_vip DESC)                AS row_num_bop_vip
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvc
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = clvc.store_id
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = clvc.meta_original_customer_id
WHERE vfa.vip_cohort != clvc.month_date;

CREATE OR REPLACE TEMPORARY TABLE _customer_activity AS
SELECT vip_cohort,
       period_month_date,
       store_brand_name,
       store_country_abbr,
       store_region_abbr,
       customer_id,
       SUM(CASE
               WHEN is_merch_purchaser_new = 1 AND row_num_merch_purchaser = 1 THEN 1 ELSE 0
           END) AS count_merch_purchases,
       SUM(CASE
               WHEN is_skip_new = 1 AND row_num_skip = 1 THEN 1 ELSE 0
           END) AS count_skips,
       SUM(CASE
               WHEN is_successful_billing_new = 1 AND row_num_successful = 1 THEN 1 ELSE 0
           END) AS count_credit_billings,
       SUM(CASE
               WHEN is_failed_billing_new = 1 AND row_num_failed = 1 THEN 1 ELSE 0
           END) AS count_failed_credit_billings,
       SUM(CASE
               WHEN is_bop_vip = 1 AND row_num_bop_vip = 1 THEN 1 ELSE 0
           END) AS count_bop_vips
FROM _customer_activity_base
GROUP BY vip_cohort,
         period_month_date,
         store_brand_name,
         store_country_abbr,
         store_region_abbr,
         customer_id;

CREATE OR REPLACE TEMPORARY TABLE _vip_activations AS
SELECT vfa.vip_cohort,
       DATE_TRUNC(MONTH, CAST(fma.event_start_local_datetime AS DATE)) AS month_date,
       st.store_brand                                                  AS store_brand_name,
       vfa.store_country_abbr,
       st.store_region                                                 AS store_region_abbr,
       vfa.customer_id,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Activation') THEN 1
               ELSE 0 END)                                             AS count_activations,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Activation') AND fa.is_reactivated_vip = 1 THEN 1
               ELSE 0 END)                                             AS count_reactivations,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Cancellation') THEN 1
               ELSE 0 END)                                             AS count_vip_cancellations,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Cancellation') AND fma.membership_type_detail = 'Passive'
                   THEN 1
               ELSE 0 END)                                             AS count_vip_passive_cancellations,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Failed Activation') THEN 1
               ELSE 0 END)                                             AS count_failed_activation,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Activation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 10 THEN 1
               ELSE 0 END)                                             AS count_activations_first_9_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Cancellation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 10 THEN 1
               ELSE 0 END)                                             AS count_vip_cancellations_first_9_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Failed Activation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 10 THEN 1
               ELSE 0 END)                                             AS count_failed_activation_first_9_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Activation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 6 THEN 1
               ELSE 0 END)                                             AS count_activations_first_5_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Cancellation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 6 THEN 1
               ELSE 0 END)                                             AS count_vip_cancellations_first_5_days,
       SUM(CASE
               WHEN dmet.membership_event_type IN ('Failed Activation') AND
                    DAYOFMONTH(fma.event_start_local_datetime) < 6 THEN 1
               ELSE 0 END)                                             AS count_failed_activation_first_5_days
FROM edw_prod.data_model_jfb.fact_membership_event fma
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = fma.store_id
         JOIN edw_prod.data_model_jfb.dim_membership_event_type dmet
              ON dmet.membership_event_type_key = fma.membership_event_type_key
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = fma.customer_id
         LEFT JOIN edw_prod.data_model_jfb.fact_activation fa
                   ON fa.membership_event_key = fma.membership_event_key
GROUP BY vfa.vip_cohort,
         DATE_TRUNC(MONTH, CAST(fma.event_start_local_datetime AS DATE)),
         st.store_brand,
         vfa.store_country_abbr,
         st.store_region,
         vfa.customer_id
HAVING count_activations > 0
    OR count_vip_cancellations > 0;

CREATE OR REPLACE TEMPORARY TABLE _ltv AS
SELECT vfa.vip_cohort                                                   AS vip_cohort,
       clva.month_date,
       st.store_brand                                                   AS store_brand_name,
       vfa.store_country_abbr,
       st.store_region                                                  AS store_region_abbr,
       vfa.customer_id,
       SUM(clva.cash_gross_profit)                                      AS sum_cgm,
       SUM(clva.product_gross_profit)                                   AS sum_gaap_gross_margin,
       SUM(clva.cash_net_revenue)                                       AS sum_cnr,
       SUM(clva.product_order_landed_product_cost_amount)               AS sum_cogs,
       SUM(clva.monthly_billed_credit_cash_refund_chargeback_amount +
           clva.product_order_cash_refund_amount_and_chargeback_amount) AS sum_refunds_and_chargebacks_amount
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust AS clvc
         LEFT JOIN edw_prod.analytics_base.customer_lifetime_value_monthly_agg AS clva
               ON clva.month_date = clvc.month_date
                   AND clva.customer_id = clvc.customer_id
                   AND clva.activation_key = clvc.activation_key
                   AND clva.vip_store_id = clvc.vip_store_id
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = clvc.store_id
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = clvc.meta_original_customer_id
WHERE clva.cash_gross_profit != 0
GROUP BY vfa.vip_cohort,
         clva.month_date,
         st.store_brand,
         vfa.store_country_abbr,
         st.store_region,
         vfa.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _membership_credit_activity AS
SELECT vfa.vip_cohort,
       DATE_TRUNC(MONTH, CAST(ce.credit_activity_local_datetime AS DATE))                  AS month_date,
       s.store_brand                                                                       AS store_brand_name,
       vfa.store_country_abbr,
       s.store_region                                                                      AS store_region_abbr,
       vfa.customer_id,
       COUNT(DISTINCT CASE
                          WHEN ce.credit_activity_type = 'Redeemed' THEN c.credit_id END)  AS count_credits_redeemed,
       COUNT(DISTINCT CASE
                          WHEN ce.credit_activity_type = 'Cancelled' THEN c.credit_id END) AS count_credits_cancelled,
       SUM(CASE
               WHEN ce.credit_activity_type = 'Redeemed' THEN ce.credit_activity_local_amount *
                                                              ce.credit_activity_usd_conversion_rate
               ELSE 0 END)                                                                 AS credit_redeemed_amount
FROM edw_prod.data_model_jfb.fact_credit_event ce
         JOIN edw_prod.data_model_jfb.dim_credit c
              ON ce.credit_key = c.credit_key
         JOIN reporting_prod.gfb.vw_store s
              ON c.store_id = s.store_id
         JOIN _vip_first_activation vfa
              ON vfa.customer_id = c.customer_id
WHERE c.credit_reason = 'Membership Credit'
  AND ce.credit_activity_type IN ('Redeemed', 'Cancelled')
GROUP BY vfa.vip_cohort,
         DATE_TRUNC(MONTH, CAST(ce.credit_activity_local_datetime AS DATE)),
         s.store_brand,
         vfa.store_country_abbr,
         s.store_region,
         vfa.customer_id;

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
                         mca.customer_id)                                                             AS customer_id
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

CREATE OR REPLACE TEMPORARY TABLE _customers_with_session AS
SELECT DISTINCT vfa.customer_id,
                vfa.vip_cohort,
                'VIP With Session'                                   AS custom_segment_category,
                (CASE
                     WHEN s.customer_id IS NOT NULL THEN 'VIP With Session In First 5 Days'
                     ELSE 'VIP Without Session In First 5 Days' END) AS custom_segment
FROM _vip_first_activation vfa
         LEFT JOIN reporting_base_prod.shared.session s
                   ON edw_prod.stg.udf_unconcat_brand(s.customer_id) = vfa.customer_id
                       AND s.session_local_datetime >= DATE_TRUNC(MONTH, CURRENT_DATE())
                       AND DAYOFMONTH(s.session_local_datetime) < 6
                       AND DATE_TRUNC(MONTH, s.session_local_datetime) >= vfa.vip_cohort;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb001_02_customer_lifetime_value_no_cust_seg AS
SELECT b.vip_cohort,
       b.activity_month,
       b.store_brand_name,
       b.store_brand_name                                          AS store_name,
       b.store_country,
       b.store_region_abbr,
       cws.custom_segment_category,
       cws.custom_segment,
       SUM(COALESCE(oi.count_activating_orders, 0))                AS count_activating_orders,
       SUM(COALESCE(oi.sum_activating_order_discount, 0))          AS sum_activating_order_discount,
       SUM(COALESCE(oi.sum_activating_shipping_revenue_amount, 0)) AS sum_activating_shipping_revenue_amount,
       SUM(COALESCE(oi.sum_activating_subtotal_amount, 0))         AS sum_activating_subtotal_amount,
       SUM(COALESCE(oi.sum_activating_non_cash_credit_amount, 0))  AS sum_activating_non_cash_credit_amount,
       SUM(COALESCE(oi.sum_activating_cash_credit_amount, 0))      AS sum_activating_cash_credit_amount,
       SUM(COALESCE(oi.sum_activating_order_num_items, 0))         AS sum_activating_order_num_items,
       SUM(COALESCE(oi.count_repeat_orders, 0))                    AS count_repeat_orders,
       SUM(COALESCE(oi.sum_repeat_discount_amount, 0))             AS sum_repeat_discount_amount,
       SUM(COALESCE(oi.sum_repeat_shipping_revenue_amount, 0))     AS sum_repeat_shipping_revenue_amount,
       SUM(COALESCE(oi.sum_repeat_subtotal_amount, 0))             AS sum_repeat_subtotal_amount,
       SUM(COALESCE(oi.sum_repeat_non_cash_credit_amount, 0))      AS sum_repeat_non_cash_credit_amount,
       SUM(COALESCE(oi.sum_repeat_cash_credit_amount, 0))          AS sum_repeat_cash_credit_amount,
       SUM(COALESCE(oi.sum_repeat_num_items, 0))                   AS sum_repeat_num_items,
       SUM(COALESCE(oi.sum_repeat_cogs_items, 0))                  AS sum_repeat_cogs,
       SUM(COALESCE(oi.sum_repeat_shipping_cost, 0))               AS sum_repeat_shipping_cost,
       SUM(COALESCE(oi.sum_repeat_product_revenue, 0))             AS sum_repeat_product_revenue,
       COUNT(DISTINCT CASE
                          WHEN oi.count_repeat_orders_first_5_days > 0
                              THEN oi.customer_id END)             AS purchased_customer_count_first_5_days,
       SUM(COALESCE(oi.total_product_revenue, 0))                  AS total_product_revenue,
       SUM(COALESCE(oi.total_product_gm_amount, 0))                AS total_product_gm_amount,
       SUM(COALESCE(oi.cash_collected_amount, 0))                  AS cash_collected_amount,
       SUM(COALESCE(ca.count_merch_purchases, 0))                  AS count_merch_purchases,
       SUM(COALESCE(ca.count_skips, 0))                            AS count_skips,
       SUM(COALESCE(ca.count_credit_billings, 0))                  AS count_credit_billings,
       SUM(COALESCE(ca.count_failed_credit_billings, 0))           AS count_failed_credit_billings,
       SUM(COALESCE(ca.count_bop_vips, 0))                         AS count_bop_vips,
       SUM(COALESCE(va.count_activations, 0) -
           COALESCE(va.count_failed_activation, 0))                AS count_activations,
       SUM(COALESCE(va.count_reactivations, 0))                    AS count_reactivations,
       SUM(COALESCE(ltv.sum_cgm, 0))                               AS sum_cash_gross_margin,
       SUM(COALESCE(ltv.sum_gaap_gross_margin, 0))                 AS sum_gaap_gross_margin,
       SUM(COALESCE(ltv.sum_cnr, 0))                               AS sum_cash_net_revenue,
       SUM(COALESCE(ltv.sum_cogs, 0))                              AS sum_cogs,
       SUM(COALESCE(sum_refunds_and_chargebacks_amount, 0))        AS sum_refunds_and_chargebacks_amount,
       SUM(COALESCE(mca.count_credits_cancelled, 0))               AS count_credits_cancelled,
       SUM(COALESCE(mca.count_credits_redeemed, 0))                AS count_credits_redeemed,
       SUM(COALESCE(va.count_vip_cancellations, 0))                AS count_vip_cancellations,
       SUM(COALESCE(va.count_vip_passive_cancellations, 0))        AS count_vip_passive_cancellations,
       SUM(COALESCE(va.count_activations_first_5_days, 0) -
           COALESCE(va.count_failed_activation_first_5_days, 0))   AS count_activations_first_5_days,
       SUM(COALESCE(va.count_vip_cancellations_first_5_days, 0))   AS count_vip_cancellations_first_5_days
FROM _base b
         JOIN _customers_with_session cws
              ON cws.customer_id = b.customer_id
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
WHERE b.vip_cohort <= b.activity_month
GROUP BY b.vip_cohort,
         b.activity_month,
         b.store_brand_name,
         b.store_brand_name,
         b.store_country,
         b.store_region_abbr,
         cws.custom_segment_category,
         cws.custom_segment;
