SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);


BEGIN TRANSACTION;

CREATE OR REPLACE TEMP TABLE _fso_agg AS
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND c.is_scrubs_customer = TRUE AND
                UPPER(c.gender) != 'M' THEN 'Scrubs Women'
           WHEN st.store_brand = 'Fabletics' AND c.is_scrubs_customer = TRUE AND UPPER(c.gender) = 'M'
               THEN 'Scrubs Men'
           WHEN st.store_brand = 'Fabletics' AND UPPER(c.gender) != 'M' THEN 'Fabletics Women'
           WHEN st.store_brand = 'Fabletics' AND UPPER(c.gender) = 'M' THEN 'Fabletics Men'
           WHEN st.store_brand = 'Yitty' THEN 'Yitty'
           END                                                                                 AS customer_segment,
       st.store_brand                                                                          AS brand,
       DATE_TRUNC('month', CAST(CONVERT_TIMEZONE('America/Los_Angeles',
                                                 fo.order_completion_local_datetime) AS DATE)) AS vip_cohort,
       fso.date                                                                                AS payment_date,
       'transaction or monthly billing'                                                        AS billing_type,
       store_region,
       COUNT(DISTINCT fso.customer_id)                                                         AS customer_count,
       SUM(fso.cash_gross_revenue)                                                             AS cash_gross_revenue,
       SUM(fso.cash_net_revenue)                                                               AS cash_net_rev,
       SUM(fso.product_order_cash_margin_pre_return) +
       SUM(billing_cash_gross_revenue)                                                         AS cash_margin_pre_return,
       SUM(cash_gross_profit)                                                                  AS cash_gross_profit,
       SUM(product_order_cash_refund_amount)                                                   AS product_order_cash_refund_amount,
       SUM(product_order_cash_chargeback_amount)                                               AS product_order_cash_chargeback_amount,
       SUM(billing_cash_refund_amount)                                                         AS billing_cash_refund_amount,
       SUM(billing_cash_chargeback_amount)                                                     AS billing_cash_chargeback_amount,
       SUM(product_order_landed_product_cost_amount)                                           AS product_order_landed_product_cost_amount,
       SUM(product_order_exchange_product_cost_amount)                                         AS product_order_exchange_product_cost_amount,
       SUM(product_order_reship_product_cost_amount)                                           AS product_order_reship_product_cost_amount,
       SUM(product_order_shipping_cost_amount)                                                 AS product_order_shipping_cost_amount,
       SUM(product_order_reship_shipping_cost_amount)                                          AS product_order_reship_shipping_cost_amount,
       SUM(product_order_exchange_shipping_cost_amount)                                        AS product_order_exchange_shipping_cost_amount,
       SUM(product_order_return_shipping_cost_amount)                                          AS product_order_return_shipping_cost_amount,
       SUM(product_order_shipping_supplies_cost_amount)                                        AS product_order_shipping_supplies_cost_amount,
       SUM(product_order_exchange_shipping_supplies_cost_amount)                               AS product_order_exchange_shipping_supplies_cost_amount,
       SUM(product_order_reship_shipping_supplies_cost_amount)                                 AS product_order_reship_shipping_supplies_cost_amount,
       SUM(product_order_misc_cogs_amount)                                                     AS product_order_misc_cogs_amount,
       SUM(product_order_cost_product_returned_resaleable_amount)                              AS product_order_cost_product_returned_resaleable_amount,
       SUM(product_order_variable_gms_cost_amount)                                             AS product_order_variable_gms_cost_amount,
       SUM(billing_variable_gms_cost_amount)                                                   AS billing_variable_gms_cost_amount,
       SUM(product_order_payment_processing_cost_amount)                                       AS product_order_payment_processing_cost_amount,
       SUM(billing_payment_processing_cost_amount)                                             AS billing_payment_processing_cost_amount,
       SUM(product_order_variable_warehouse_cost_amount)                                       AS product_order_variable_warehouse_cost_amount
FROM edw_prod.analytics_base.finance_sales_ops fso
         JOIN edw_prod.stg.fact_activation a ON a.activation_key = fso.activation_key
         JOIN edw_prod.stg.fact_order fo ON fo.order_id = a.order_id
         JOIN edw_prod.stg.dim_store st ON st.store_id = fso.store_id
         JOIN edw_prod.stg.dim_customer c ON c.customer_id = fso.customer_id
WHERE currency_object = 'usd'
  AND date_object = 'shipped'
  AND store_region IN ('NA', 'EU')
  AND store_brand IN ('Fabletics', 'Yitty')
  AND vip_cohort >= '2019-01-01'
GROUP BY customer_segment,
         brand,
         vip_cohort,
         payment_date,
         store_region;

CREATE OR REPLACE TEMP TABLE _gc_expenses_nov24 AS
SELECT month_date,
       segment,
       gms_fixed_pct,
       gfc_fixed_pct,
       selling_exp_pct,
       gms_back_office_pct,
       gfc_back_office_pct,
       CASE
           WHEN segment = 'FLNA' THEN 'NA'
           WHEN segment = 'FLEU' THEN 'EU' END AS store_region
FROM fabletics.gc_fixed_variable_expenses_nov24
WHERE segment IN ('FLEU', 'FLNA');


TRUNCATE TABLE fabletics.gc_historical_customer_request;

INSERT INTO fabletics.gc_historical_customer_request
(customer_segment,
 brand,
 vip_cohort,
 payment_date,
 billing_type,
 store_region,
 customer_count,
 cash_gross_revenue,
 cash_net_rev,
 cash_margin_pre_return,
 cash_gross_profit,
 product_order_cash_refund_amount,
 product_order_cash_chargeback_amount,
 billing_cash_refund_amount,
 billing_cash_chargeback_amount,
 product_order_landed_product_cost_amount,
 product_order_exchange_product_cost_amount,
 product_order_reship_product_cost_amount,
 product_order_shipping_cost_amount,
 product_order_reship_shipping_cost_amount,
 product_order_exchange_shipping_cost_amount,
 product_order_return_shipping_cost_amount,
 product_order_shipping_supplies_cost_amount,
 product_order_exchange_shipping_supplies_cost_amount,
 product_order_reship_shipping_supplies_cost_amount,
 product_order_misc_cogs_amount,
 product_order_cost_product_returned_resaleable_amount,
 product_order_variable_gms_cost_amount,
 billing_variable_gms_cost_amount,
 product_order_payment_processing_cost_amount,
 billing_payment_processing_cost_amount,
 product_order_variable_warehouse_cost_amount,
 selling_expenses,
 gfc_fixed,
 gfc_back_office,
 gms_fixed,
 gms_back_office,
 meta_create_datetime)
SELECT fso.customer_segment,
       fso.brand,
       fso.vip_cohort,
       fso.payment_date,
       fso.billing_type,
       fso.store_region,
       fso.customer_count,
       fso.cash_gross_revenue,
       fso.cash_net_rev,
       fso.cash_margin_pre_return,
       fso.cash_gross_profit,
       fso.product_order_cash_refund_amount,
       fso.product_order_cash_chargeback_amount,
       fso.billing_cash_refund_amount,
       fso.billing_cash_chargeback_amount,
       fso.product_order_landed_product_cost_amount,
       fso.product_order_exchange_product_cost_amount,
       fso.product_order_reship_product_cost_amount,
       fso.product_order_shipping_cost_amount,
       fso.product_order_reship_shipping_cost_amount,
       fso.product_order_exchange_shipping_cost_amount,
       fso.product_order_return_shipping_cost_amount,
       fso.product_order_shipping_supplies_cost_amount,
       fso.product_order_exchange_shipping_supplies_cost_amount,
       fso.product_order_reship_shipping_supplies_cost_amount,
       fso.product_order_misc_cogs_amount,
       fso.product_order_cost_product_returned_resaleable_amount,
       fso.product_order_variable_gms_cost_amount,
       fso.billing_variable_gms_cost_amount,
       fso.product_order_payment_processing_cost_amount,
       fso.billing_payment_processing_cost_amount,
       fso.product_order_variable_warehouse_cost_amount,
       gc.selling_exp_pct * fso.cash_net_rev     AS selling_expenses,
       gc.gfc_fixed_pct * fso.cash_net_rev       AS gfc_fixed,
       gc.gfc_back_office_pct * fso.cash_net_rev AS gfc_back_office,
       gc.gms_fixed_pct * fso.cash_net_rev       AS gms_fixed,
       gc.gms_back_office_pct * fso.cash_net_rev AS gms_back_office,
       $execution_start_time
FROM _fso_agg fso
         JOIN _gc_expenses_nov24 gc
WHERE DATE_TRUNC('month', fso.payment_date) = gc.month_date
  AND fso.store_region = gc.store_region;

COMMIT;
