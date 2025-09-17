SET execution_start_datetime = CURRENT_TIMESTAMP();

CREATE OR REPLACE TEMPORARY TABLE _date_scaffold AS
SELECT DISTINCT full_date AS date
FROM edw_prod.data_model_jfb.dim_date
WHERE full_date >= '2022-07-01'
  AND full_date <= CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _jf_sd_fk_customers AS
SELECT d.date, dc.customer_id, LOWER(dc.email) AS email, store_brand_abbr
FROM _date_scaffold d
     JOIN edw_prod.data_model_jfb.fact_activation fa
          ON d.date >= fa.activation_local_datetime::DATE
              AND d.date <= fa.cancellation_local_datetime::DATE
     JOIN edw_prod.stg.dim_customer dc
          ON fa.customer_id = dc.meta_original_customer_id
     JOIN edw_prod.data_model_jfb.dim_store s
          ON fa.store_id = s.store_id
WHERE s.store_brand_abbr IN ('JF','SD','FK')
  AND s.store_country = 'US';

CREATE OR REPLACE TEMPORARY TABLE _jf_sd_fk_fso AS
SELECT c.date
     , c.email
     , c.store_brand_abbr
     , SUM(fso.product_order_count)                                                 AS product_order_count
     , SUM(fso.product_net_revenue)                                                 AS product_net_revenue
     , SUM(fso.billed_cash_credit_issued_equivalent_count)                          AS billed_cash_credit_issued_equivalent_count
     , SUM(fso.billing_cash_net_revenue)                                            AS billing_cash_net_revenue
     , SUM(fso.billed_cash_credit_cancelled_amount)                                 AS billed_cash_credit_cancelled_amount
     , SUM(fso.cash_net_revenue)                                                    AS cash_net_revenue
     , COUNT(CASE WHEN c.date = fso.cancellation_local_datetime:: DATE THEN 1 END) AS membership_cancels
FROM _jf_sd_fk_customers c
     LEFT JOIN edw_prod.analytics_base.finance_sales_ops fso
               ON c.customer_id = fso.customer_id
                   AND c.date = fso.date
                   AND fso.is_bop_vip = 1
                   AND fso.date_object = 'placed'
                   AND fso.currency_object = 'usd'
GROUP BY c.date
       , c.email
       , c.store_brand_abbr;

CREATE OR REPLACE TRANSIENT TABLE gfb.jf_sd_crossover_customers_behaviour AS
SELECT COALESCE(jf.date, sd.date)                                                AS date
     , COUNT(DISTINCT CASE WHEN jf.email = sd.email THEN jf.email END)           AS active_crossover_customers
     , COUNT(DISTINCT jf.email)                                                  AS active_jf_customers
     , COUNT(DISTINCT sd.email)                                                  AS active_sd_customers

--PRODUCT_ORDER_COUNT
     , SUM(CASE WHEN jf.email = sd.email THEN sd.product_order_count ELSE 0 END) AS sd_cross_product_order_count
     , SUM(sd.product_order_count)                                               AS sd_product_order_count

     , SUM(CASE WHEN jf.email = sd.email THEN jf.product_order_count ELSE 0 END) AS jf_cross_product_order_count
     , SUM(jf.product_order_count)                                               AS jf_product_order_count

--PRODUCT_NET_REVENUE
     , SUM(CASE WHEN jf.email = sd.email THEN sd.product_net_revenue ELSE 0 END) AS sd_cross_product_net_revenue
     , SUM(sd.product_net_revenue)                                               AS sd_product_net_revenue

     , SUM(CASE WHEN jf.email = sd.email THEN jf.product_net_revenue ELSE 0 END) AS jf_cross_product_net_revenue
     , SUM(jf.product_net_revenue)                                               AS jf_product_net_revenue

--BILLED_CREDIT
     , SUM(CASE
               WHEN jf.email = sd.email THEN sd.billed_cash_credit_issued_equivalent_count
               ELSE 0 END)                                                       AS sd_cross_billed_credit
     , SUM(sd.billed_cash_credit_issued_equivalent_count)                        AS sd_billed_credit

     , SUM(CASE
               WHEN jf.email = sd.email THEN jf.billed_cash_credit_issued_equivalent_count
               ELSE 0 END)                                                       AS jf_cross_billed_credit
     , SUM(jf.billed_cash_credit_issued_equivalent_count)                        AS jf_billed_credit

--BILLING REVENUE
     , SUM(CASE
               WHEN jf.email = sd.email THEN sd.billing_cash_net_revenue
               ELSE 0 END)                                                       AS sd_cross_billing_revenue
     , SUM(sd.billing_cash_net_revenue)                                          AS sd_billing_revenue

     , SUM(CASE
               WHEN jf.email = sd.email THEN jf.billing_cash_net_revenue
               ELSE 0 END)                                                       AS jf_cross_billing_revenue
     , SUM(jf.billing_cash_net_revenue)                                          AS jf_billing_revenue

--CREDIT REFUND REVENUE
     , SUM(CASE
               WHEN jf.email = sd.email THEN sd.billed_cash_credit_cancelled_amount
               ELSE 0 END)                                                       AS sd_cross_credit_refund_revenue
     , SUM(sd.billed_cash_credit_cancelled_amount)                               AS sd_credit_refund_revenue

     , SUM(CASE
               WHEN jf.email = sd.email THEN jf.billed_cash_credit_cancelled_amount
               ELSE 0 END)                                                       AS jf_cross_credit_refund_revenue
     , SUM(jf.billed_cash_credit_cancelled_amount)                               AS jf_credit_refund_revenue

--CASH_NET_REVENUE
     , SUM(CASE WHEN jf.email = sd.email THEN sd.cash_net_revenue ELSE 0 END)    AS sd_cross_cash_net_revenue
     , SUM(sd.cash_net_revenue)                                                  AS sd_cash_net_revenue

     , SUM(CASE WHEN jf.email = sd.email THEN jf.cash_net_revenue ELSE 0 END)    AS jf_cross_cash_net_revenue
     , SUM(jf.cash_net_revenue)                                                  AS jf_cash_net_revenue

--MEMBERSHIP_CANCELS
     , COUNT(DISTINCT CASE
                          WHEN jf.email = sd.email AND sd.membership_cancels > 0
                              THEN sd.email END)                                 AS sd_cross_membership_cancels
     , COUNT(DISTINCT CASE WHEN sd.membership_cancels > 0 THEN sd.email END)     AS sd_membership_cancels

     , COUNT(DISTINCT CASE
                          WHEN jf.email = sd.email AND jf.membership_cancels > 0
                              THEN jf.email END)                                 AS jf_cross_membership_cancels
     , COUNT(DISTINCT CASE WHEN jf.membership_cancels > 0 THEN jf.email END)     AS jf_membership_cancels
     , $execution_start_datetime                                                 AS meta_create_datetime
FROM (SELECT * FROM _jf_sd_fk_fso WHERE store_brand_abbr = 'JF') jf
     FULL JOIN (SELECT * FROM _jf_sd_fk_fso WHERE store_brand_abbr = 'SD') sd
               ON jf.date = sd.date
                   AND jf.email = sd.email
GROUP BY COALESCE(jf.date, sd.date), meta_create_datetime;

CREATE OR REPLACE TRANSIENT TABLE gfb.jf_fk_crossover_customers_behaviour AS
SELECT COALESCE(jf.date, fk.date)                                                AS date
     , COUNT(DISTINCT CASE WHEN jf.email = fk.email THEN jf.email END)           AS active_crossover_customers
     , COUNT(DISTINCT jf.email)                                                  AS active_jf_customers
     , COUNT(DISTINCT fk.email)                                                  AS active_sd_customers

--PRODUCT_ORDER_COUNT
     , SUM(CASE WHEN jf.email = fk.email THEN fk.product_order_count ELSE 0 END) AS fk_cross_product_order_count
     , SUM(fk.product_order_count)                                               AS fk_product_order_count

     , SUM(CASE WHEN jf.email = fk.email THEN jf.product_order_count ELSE 0 END) AS jf_cross_product_order_count
     , SUM(jf.product_order_count)                                               AS jf_product_order_count

--PRODUCT_NET_REVENUE
     , SUM(CASE WHEN jf.email = fk.email THEN fk.product_net_revenue ELSE 0 END) AS fk_cross_product_net_revenue
     , SUM(fk.product_net_revenue)                                               AS fk_product_net_revenue

     , SUM(CASE WHEN jf.email = fk.email THEN jf.product_net_revenue ELSE 0 END) AS jf_cross_product_net_revenue
     , SUM(jf.product_net_revenue)                                               AS jf_product_net_revenue

--BILLED_CREDIT
     , SUM(CASE
               WHEN jf.email = fk.email THEN fk.billed_cash_credit_issued_equivalent_count
               ELSE 0 END)                                                       AS fk_cross_billed_credit
     , SUM(fk.billed_cash_credit_issued_equivalent_count)                        AS fk_billed_credit

     , SUM(CASE
               WHEN jf.email = fk.email THEN jf.billed_cash_credit_issued_equivalent_count
               ELSE 0 END)                                                       AS jf_cross_billed_credit
     , SUM(jf.billed_cash_credit_issued_equivalent_count)                        AS jf_billed_credit

--BILLING REVENUE
     , SUM(CASE
               WHEN jf.email = fk.email THEN fk.billing_cash_net_revenue
               ELSE 0 END)                                                       AS fk_cross_billing_revenue
     , SUM(fk.billing_cash_net_revenue)                                          AS fk_billing_revenue

     , SUM(CASE
               WHEN jf.email = fk.email THEN jf.billing_cash_net_revenue
               ELSE 0 END)                                                       AS jf_cross_billing_revenue
     , SUM(jf.billing_cash_net_revenue)                                          AS jf_billing_revenue

--CREDIT REFUND REVENUE
     , SUM(CASE
               WHEN jf.email = fk.email THEN fk.billed_cash_credit_cancelled_amount
               ELSE 0 END)                                                       AS fk_cross_credit_refund_revenue
     , SUM(fk.billed_cash_credit_cancelled_amount)                               AS fk_credit_refund_revenue

     , SUM(CASE
               WHEN jf.email = fk.email THEN jf.billed_cash_credit_cancelled_amount
               ELSE 0 END)                                                       AS jf_cross_credit_refund_revenue
     , SUM(jf.billed_cash_credit_cancelled_amount)                               AS jf_credit_refund_revenue

--CASH_NET_REVENUE
     , SUM(CASE WHEN jf.email = fk.email THEN fk.cash_net_revenue ELSE 0 END)    AS fk_cross_cash_net_revenue
     , SUM(fk.cash_net_revenue)                                                  AS fk_cash_net_revenue

     , SUM(CASE WHEN jf.email = fk.email THEN jf.cash_net_revenue ELSE 0 END)    AS jf_cross_cash_net_revenue
     , SUM(jf.cash_net_revenue)                                                  AS jf_cash_net_revenue

--MEMBERSHIP_CANCELS
     , COUNT(DISTINCT CASE
                          WHEN jf.email = fk.email AND fk.membership_cancels > 0
                              THEN fk.email END)                                 AS fk_cross_membership_cancels
     , COUNT(DISTINCT CASE WHEN fk.membership_cancels > 0 THEN fk.email END)     AS fk_membership_cancels

     , COUNT(DISTINCT CASE
                          WHEN jf.email = fk.email AND jf.membership_cancels > 0
                              THEN jf.email END)                                 AS jf_cross_membership_cancels
     , COUNT(DISTINCT CASE WHEN jf.membership_cancels > 0 THEN jf.email END)     AS jf_membership_cancels
     , $execution_start_datetime                                                 AS meta_create_datetime
FROM (SELECT * FROM _jf_sd_fk_fso WHERE store_brand_abbr = 'JF') jf
     FULL JOIN (SELECT * FROM _jf_sd_fk_fso WHERE store_brand_abbr = 'FK') fk
               ON jf.date = fk.date
                   AND jf.email = fk.email
GROUP BY COALESCE(jf.date, fk.date), meta_create_datetime;
