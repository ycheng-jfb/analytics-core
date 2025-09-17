SET update_datetime = CURRENT_TIMESTAMP;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb_vip_lifetime_value_monthly AS
SELECT clvm.vip_cohort_month_date                                               AS vip_cohort,
       DATE_TRUNC(YEAR, clvm.vip_cohort_month_date)                             AS activation_year,
       clvm.month_date,
       st.store_brand,
       st.store_country,
       st.store_region,
       CASE
           WHEN cltd.online_product_order_count > 0 AND cltd.mobile_app_product_order_count = 0
               THEN 'Online Only'
           WHEN cltd.online_product_order_count > 0 AND cltd.mobile_app_product_order_count > 0
               THEN 'Online + Mobile App'
           WHEN cltd.online_product_order_count = 0 AND cltd.mobile_app_product_order_count > 0
               THEN 'Mobile App Only'
           ELSE 'reactivation with no product order' END                        AS ltv_type,
       COALESCE(clvm.is_reactivated_vip, FALSE)                                 AS is_reactivated_vip,
       clvm.cumulative_cash_gross_profit_decile,
       clvm.cumulative_product_gross_profit_decile,
       cltd.product_order_count                                                 AS product_purchases,
       DATEDIFF(MONTH, vip_cohort, clvm.month_date) + 1                         AS tenure,
       COUNT(DISTINCT clvm.meta_original_customer_id)                           AS customer_count,
       SUM(clvm.product_gross_revenue)                                          AS product_gross_revenue,
       SUM(clvm.product_net_revenue)                                            AS product_net_revenue,
       SUM(clvm.product_margin_pre_return)                                      AS pre_return_product_margin,
       SUM(clvm.product_gross_profit)                                           AS product_gross_margin,
       SUM(clvm.product_order_cash_gross_revenue_amount)                        AS product_order_cash_gross_revenue,
       SUM(clvm.product_order_cash_net_revenue)                                 AS product_order_cash_net_revenue,
       SUM(clvm.cash_gross_revenue)                                             AS cash_gross_revenue,
       SUM(clvm.cash_net_revenue)                                               AS cash_net_revenue,
       SUM(clvm.product_order_cash_margin_pre_return)                           AS product_order_pre_return_cash_margin,
       SUM(clvm.cash_gross_profit)                                              AS cash_margin,
       SUM(clvm.product_order_cash_gross_profit)                                AS product_order_cash_margin,
       SUM(clvm.product_order_count)                                            AS product_order_count,
       SUM(clvm.product_order_unit_count)                                       AS product_order_units,
       SUM(clvm.product_order_subtotal_excl_tariff_amount)                      AS product_order_subtotal,
       SUM(clvm.product_order_product_discount_amount)                          AS product_order_discount,
       SUM(clvm.product_order_landed_product_cost_amount)                       AS product_order_product_cost,
       SUM(clvm.billed_cash_credit_issued_equivalent_count)                     AS credit_billing_count,
       SUM(clvm.billed_cash_credit_issued_amount)                               AS credit_billing_amount,
       SUM(clvm.billed_cash_credit_redeemed_equivalent_count)                   AS product_order_membership_credit_redeemed_count,
       SUM(clvm.billed_cash_credit_issued_amount)                               AS product_order_membership_credit_redeemed_amount,
       COUNT(DISTINCT CASE
                          WHEN (clvm.is_merch_purchaser >= 1 OR
                                clvm.is_successful_billing >= 1 AND clvm.is_bop_vip >= 1)
                              THEN clvm.customer_id END)                        AS cash_purchasers,
       COUNT(DISTINCT CASE
                          WHEN clvm.is_merch_purchaser >= 1 AND clvm.is_bop_vip >= 1
                              THEN clvm.customer_id END)                        AS merch_purchasers,
       COUNT(DISTINCT CASE
                          WHEN clvm.is_successful_billing >= 1 AND clvm.is_bop_vip >= 1
                              THEN clvm.customer_id END)                        AS credit_billees,
       COUNT(DISTINCT CASE
                          WHEN clvm.is_skip >= 1 AND clvm.is_bop_vip >= 1
                              THEN clvm.customer_id END)                        AS skippers,
       COUNT(DISTINCT CASE
                          WHEN clvm.is_failed_billing >= 1 AND clvm.is_bop_vip >= 1
                              THEN clvm.customer_id END)                        AS failed_billees,
       COUNT(DISTINCT CASE WHEN clvm.is_bop_vip >= 1 THEN clvm.customer_id END) AS bop_vips,
       COUNT(DISTINCT CASE
                          WHEN clvm.billed_cash_credit_cancelled_equivalent_count >= 1 AND clvm.is_bop_vip >= 1
                              THEN clvm.customer_id END)                        AS credit_cancellers,
       COUNT(DISTINCT CASE
                          WHEN clvm.billed_cash_credit_redeemed_equivalent_count >= 1 AND clvm.is_bop_vip >= 1
                              THEN clvm.customer_id END)                        AS credit_redeemers,
       $update_datetime                                                         AS update_datetime
FROM edw_prod.analytics_base.customer_lifetime_value_monthly clvm
     JOIN edw_prod.data_model_jfb.dim_store AS st
          ON st.store_id = clvm.store_id
     LEFT JOIN edw_prod.analytics_base.customer_lifetime_value_ltd cltd
               ON clvm.customer_id = cltd.customer_id
                   AND clvm.activation_key = cltd.activation_key
WHERE clvm.activation_key <> -1
GROUP BY clvm.vip_cohort_month_date,
         DATE_TRUNC(YEAR, clvm.vip_cohort_month_date),
         clvm.month_date,
         st.store_brand,
         st.store_country,
         st.store_region,
         (CASE
              WHEN cltd.online_product_order_count > 0 AND cltd.mobile_app_product_order_count = 0
                  THEN 'Online Only'
              WHEN cltd.online_product_order_count > 0 AND cltd.mobile_app_product_order_count > 0
                  THEN 'Online + Mobile App'
              WHEN cltd.online_product_order_count = 0 AND cltd.mobile_app_product_order_count > 0
                  THEN 'Mobile App Only'
              ELSE 'reactivation with no product order' END),
         COALESCE(clvm.is_reactivated_vip, FALSE),
         clvm.cumulative_cash_gross_profit_decile,
         clvm.cumulative_product_gross_profit_decile,
         cltd.product_order_count,
         DATEDIFF(MONTH, vip_cohort, clvm.month_date) + 1;
