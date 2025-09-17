CREATE OR REPLACE TEMPORARY TABLE _month_scaffold AS
SELECT DISTINCT DATE_TRUNC('month', full_date) AS activity_month
FROM edw_prod.data_model_jfb.dim_date
WHERE full_date >= '2022-01-01'
  AND full_date <= CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _jf_fk_customers AS
SELECT m.activity_month,
       fa.customer_id,
       LOWER(dc.email) AS email,
       s.store_brand_abbr
FROM _month_scaffold m
     JOIN edw_prod.data_model_jfb.fact_activation fa
          ON m.activity_month >= DATE_TRUNC('month', fa.activation_local_datetime::DATE)
              AND m.activity_month <= DATE_TRUNC('month', fa.cancellation_local_datetime::DATE)
     JOIN edw_prod.data_model_jfb.dim_customer dc
          ON fa.customer_id = dc.customer_id
     JOIN edw_prod.data_model_jfb.dim_store s
          ON fa.store_id = s.store_id
WHERE s.store_brand_abbr IN ('JF','FK')
  AND s.store_country = 'US';

CREATE OR REPLACE TEMPORARY TABLE _customer_credits_jf_fk AS
SELECT c.customer_id,
       c.activity_month,
       c.store_brand_abbr,
       SUM(IFF(credit_activity_type = 'Redeemed' AND
               activity_month = DATE_TRUNC('month', credit_activity_local_datetime::DATE), credit_activity_local_amount,
               0))                               AS credits_redeemed,
       SUM(IFF(credit_activity_type = 'Redeemed' AND
               activity_month >= DATE_TRUNC('month', credit_activity_local_datetime::DATE),
               credit_activity_local_amount, 0)) AS credits_redeemed_till_activity_month,
       SUM(IFF(credit_activity_type = 'Issued' AND
               activity_month >= DATE_TRUNC('month', credit_activity_local_datetime::DATE),
               credit_activity_local_amount, 0))
           - SUM(IFF(credit_activity_type = 'Redeemed' AND
                     activity_month >= DATE_TRUNC('month', credit_activity_local_datetime::DATE),
                     credit_activity_local_amount,
                     0))
           - SUM(IFF(credit_activity_type = 'Cancelled' AND
                     activity_month >= DATE_TRUNC('month', credit_activity_local_datetime::DATE),
                     credit_activity_local_amount,
                     0))
           - SUM(IFF(credit_activity_type = 'Expired' AND
                     activity_month >= DATE_TRUNC('month', credit_activity_local_datetime::DATE),
                     credit_activity_local_amount,
                     0))                         AS outstanding_amount
FROM edw_prod.data_model_jfb.fact_credit_event fce
     JOIN _jf_fk_customers c
          ON c.customer_id = fce.customer_id
GROUP BY c.customer_id,
         c.activity_month,
         c.store_brand_abbr;

CREATE OR REPLACE TEMPORARY TABLE _jf_fk_data AS
SELECT c.activity_month,
       c.email,
       c.customer_id,
       DATEDIFF('day', fa.activation_local_datetime::DATE, CURRENT_DATE()) AS tenure,
       c.store_brand_abbr,
       SUM(clvm.product_order_count)                                       AS product_order_count,
       SUM(clvm.product_order_unit_count)                                  AS product_order_unit_count,
       SUM(clvm.product_net_revenue)                                       AS product_net_revenue,
       SUM(clvm.billing_cash_net_revenue)                                  AS billing_cash_net_revenue,
       SUM(cc.credits_redeemed)                                            AS credits_redeemed,
       SUM(cc.credits_redeemed_till_activity_month)                        AS credits_redeemed_till_activity_month,
       SUM(cc.outstanding_amount)                                          AS outstanding_credits_amount
FROM _jf_fk_customers c
     LEFT JOIN edw_prod.analytics_base.customer_lifetime_value_monthly clvm
               ON c.customer_id = clvm.meta_original_customer_id
                   AND c.activity_month = clvm.month_date
                   AND clvm.is_bop_vip = 1
     LEFT JOIN edw_prod.data_model_jfb.fact_activation fa
               ON fa.activation_key = clvm.first_activation_key
     LEFT JOIN _customer_credits_jf_fk cc
               ON c.customer_id = cc.customer_id
                   AND c.activity_month = cc.activity_month
GROUP BY c.activity_month,
         c.email,
         c.customer_id,
         tenure,
         c.store_brand_abbr;

CREATE OR REPLACE TEMPORARY TABLE _crossover_member_analysis AS
SELECT COALESCE(jf.activity_month, fk.activity_month)                AS activity_month,
       COALESCE(jf.email, fk.email)                                  AS email,
       jf.customer_id                                                AS jf_customer_id,
       fk.customer_id                                                AS fk_customer_id,
       jf.tenure                                                     AS jf_tenure,
       fk.tenure                                                     AS fk_tenure,
       jf.product_order_count                                        AS jf_orders,
       fk.product_order_count                                        AS fk_orders,
       jf.product_order_unit_count                                   AS jf_units,
       fk.product_order_unit_count                                   AS fk_units,
       jf.product_net_revenue                                        AS jf_product_net_revenue,
       fk.product_net_revenue                                        AS fk_product_net_revenue,
       jf.billing_cash_net_revenue                                   AS jf_billing_net_revenue,
       fk.billing_cash_net_revenue                                   AS fk_billing_net_revenue,
       jf.credits_redeemed                                           AS jf_credits_redeemed,
       fk.credits_redeemed                                           AS fk_credits_redeemed,
       jf.credits_redeemed_till_activity_month                       AS jf_credits_redeemed_till_activity_month,
       fk.credits_redeemed_till_activity_month                       AS fk_credits_redeemed_till_activity_month,
       jf.outstanding_credits_amount                                 AS jf_outstanding_credits,
       fk.outstanding_credits_amount                                 AS fk_outstanding_credits,
       CASE
           WHEN jf_orders > 0 AND fk_orders = 0 THEN 'JF order only'
           WHEN fk_orders > 0 AND jf_orders = 0 THEN 'FK order only'
           WHEN fk_orders > 0 AND jf_orders > 0 THEN 'ordered in both'
           WHEN jf_orders = 0 AND fk_orders = 0 THEN 'no orders' END AS jf_fk_order_category,
       CASE
           WHEN jf_billing_net_revenue > 0 AND fk_billing_net_revenue = 0 THEN 'JF billing only'
           WHEN fk_billing_net_revenue > 0 AND jf_billing_net_revenue = 0 THEN 'FK billing only'
           WHEN fk_billing_net_revenue > 0 AND jf_billing_net_revenue > 0 THEN 'billed in both'
           WHEN jf_billing_net_revenue = 0 AND fk_billing_net_revenue = 0
               THEN 'no billings' END                                AS jf_fk_billing_category,
       CASE
           WHEN jf_credits_redeemed > 0 AND fk_credits_redeemed = 0 THEN 'JF redemption only'
           WHEN fk_credits_redeemed > 0 AND jf_credits_redeemed = 0 THEN 'FK redemption only'
           WHEN fk_credits_redeemed > 0 AND jf_credits_redeemed > 0 THEN 'redeemed in both'
           WHEN jf_credits_redeemed = 0 AND fk_credits_redeemed = 0
               THEN 'no redemptions' END                             AS jf_fk_redemption_category
FROM _jf_fk_data jf
         JOIN _jf_fk_data fk
              ON jf.activity_month = fk.activity_month
                  AND jf.email = fk.email
                  AND jf.store_brand_abbr = 'JF'
                  AND fk.store_brand_abbr = 'FK';

CREATE OR REPLACE TRANSIENT TABLE gfb.jf_fk_dual_members_monthly AS
SELECT activity_month,
       jf_fk_order_category,
       jf_fk_billing_category,
       jf_fk_redemption_category,
       COUNT(DISTINCT COALESCE(jf_customer_id, fk_customer_id))                  AS _crossover_count,
---order metrics
       COUNT(DISTINCT CASE
                          WHEN jf_orders > 0 OR fk_orders > 0
                              THEN COALESCE(jf_customer_id, fk_customer_id) END) AS product_order_customer_count,
       COUNT(DISTINCT CASE
                          WHEN jf_fk_order_category = 'no orders'
                              THEN COALESCE(jf_customer_id, fk_customer_id) END) AS no_orders_customer_count,
       SUM(jf_orders + fk_orders)                                                AS orders,
       SUM(jf_units + fk_units)                                                  AS units,
       SUM(jf_product_net_revenue + fk_product_net_revenue)                      AS product_net_rev,
---billing metrics
       COUNT(DISTINCT CASE
                          WHEN jf_billing_net_revenue > 0 OR fk_billing_net_revenue > 0
                              THEN COALESCE(jf_customer_id, fk_customer_id) END) AS billing_customer_count,
       SUM(jf_billing_net_revenue + fk_billing_net_revenue)                      AS billing_net_rev,
--redemption metrics
       COUNT(DISTINCT CASE
                          WHEN jf_credits_redeemed > 0 OR fk_credits_redeemed > 0
                              THEN COALESCE(jf_customer_id, fk_customer_id) END) AS redemption_customer_count,
       SUM(jf_credits_redeemed + fk_credits_redeemed)                            AS redemption_rev
FROM _crossover_member_analysis
GROUP BY activity_month,
         jf_fk_order_category,
         jf_fk_billing_category,
         jf_fk_redemption_category;
