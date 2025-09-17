SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

SET processing_date = (IFF((DAYOFWEEK(CURRENT_DATE) = 0), '1900-01-01', DATE_TRUNC('month',DATEADD(YEAR, -3, CURRENT_DATE))));

CREATE OR REPLACE TEMP TABLE _cohort_waterfall_activating AS
SELECT customer_id,
       activation_key,
       SUM(nvl(activating_product_order_count, 0))                       AS activating_product_order_count,
       SUM(nvl(activating_product_order_unit_count, 0))                  AS activating_product_order_unit_count,
       SUM(nvl(activating_product_order_product_discount_amount, 0))     AS activating_product_order_product_discount_amount,
       SUM(nvl(activating_product_order_subtotal_excl_tariff_amount, 0)) AS activating_product_order_subtotal_excl_tariff_amount,
       SUM(nvl(activating_product_order_product_subtotal_amount, 0))     AS activating_product_order_product_subtotal_amount,
       SUM(nvl(activating_product_order_shipping_revenue_amount, 0))     AS activating_product_order_shipping_revenue_amount,
       SUM(nvl(activating_product_order_landed_product_cost_amount, 0))  AS activating_product_order_landed_product_cost_amount,
       SUM(nvl(activating_product_gross_revenue, 0))                     AS activating_product_gross_revenue

FROM analytics_base.customer_lifetime_value_ltd
GROUP BY customer_id,
         activation_key;

CREATE OR REPLACE TEMPORARY TABLE _order_count_with_credit_redemption AS
SELECT COALESCE(fa.vip_cohort_month_date, '1900-01-01'::DATE) AS vip_cohort_month_date,
       fa.membership_type,
       CASE WHEN ds.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
            WHEN ds.store_brand = 'Fabletics' THEN 'Fabletics Womens'
            WHEN ds.store_brand = 'Yitty' THEN 'Yitty'
       ELSE ds.store_brand END AS store_brand,
       DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE AS month_date,
       CASE WHEN fa.vip_cohort_month_date = DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE AND COALESCE(fa.is_reactivated_vip, FALSE) = FALSE THEN 'Activation'
            WHEN fa.vip_cohort_month_date = DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE AND COALESCE(fa.is_reactivated_vip, FALSE) = TRUE THEN 'Reactivation'
            WHEN fa.activation_local_datetime < month_date AND fa.cancellation_local_datetime::DATE >= month_date THEN 'BOP'
            WHEN DATEDIFF('MONTH', month_date, fa.cancellation_local_datetime) = 0 THEN 'Cancelled'
       END AS membership_monthly_status,
       CASE WHEN COALESCE(fa.is_reactivated_vip, FALSE) THEN 'Reactivated Membership'
            WHEN COALESCE(fa.is_reactivated_vip, FALSE) = FALSE THEN 'First Membership'
       END AS membership_sequence,
       ds.store_region,
       ds.store_country,
       ds.store_name,
       COUNT(1) AS order_count_with_credit_redemption
FROM data_model.fact_order fo
JOIN data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l1 = 'Product Order'
JOIN data_model.dim_order_status dos ON dos.order_status_key = fo.order_status_key
    AND dos.order_status IN ('Success', 'Pending')
JOIN data_model.dim_order_membership_classification domc ON domc.order_membership_classification_key = fo.order_membership_classification_key
JOIN data_model.fact_activation fa ON fa.activation_key = fo.activation_key
JOIN data_model.dim_store ds ON ds.store_id = IFF(fa.activation_key = -1, fo.store_id, fa.store_id)
JOIN data_model.dim_customer dc ON dc.customer_id = fo.customer_id
WHERE fo.cash_credit_local_amount > 0
GROUP BY COALESCE(fa.vip_cohort_month_date, '1900-01-01'::DATE),
       fa.membership_type,
       CASE WHEN ds.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
            WHEN ds.store_brand = 'Fabletics' THEN 'Fabletics Womens'
            WHEN ds.store_brand = 'Yitty' THEN 'Yitty'
       ELSE ds.store_brand END,
       DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE,
       CASE WHEN fa.vip_cohort_month_date = DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE AND COALESCE(fa.is_reactivated_vip, FALSE) = FALSE THEN 'Activation'
            WHEN fa.vip_cohort_month_date = DATE_TRUNC('MONTH', fo.order_local_datetime)::DATE AND COALESCE(fa.is_reactivated_vip, FALSE) = TRUE THEN 'Reactivation'
            WHEN fa.activation_local_datetime < month_date AND fa.cancellation_local_datetime::DATE >= month_date THEN 'BOP'
            WHEN DATEDIFF('MONTH', month_date, fa.cancellation_local_datetime) = 0 THEN 'Cancelled'
       END,
       CASE WHEN COALESCE(fa.is_reactivated_vip, FALSE) THEN 'Reactivated Membership'
            WHEN COALESCE(fa.is_reactivated_vip, FALSE) = FALSE THEN 'First Membership'
       END,
       ds.store_region,
       ds.store_country,
       ds.store_name;

CREATE OR REPLACE TEMP TABLE _cohort_waterfall_final_output_stg AS
SELECT clvm.vip_cohort_month_date
     , clvm.membership_type
     , CASE
           WHEN clvm.is_scrubs_customer = 1 AND clvm.vip_cohort_month_date >= '2023-02-01' AND clvm.GENDER = 'M' THEN 'Scrubs Mens'
           WHEN clvm.is_scrubs_customer = 1 AND clvm.vip_cohort_month_date >= '2023-02-01' THEN 'Scrubs Womens'
           WHEN store_brand = 'Fabletics' AND clvm.GENDER = 'M' THEN 'Fabletics Mens'
           WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
           WHEN store_brand = 'Yitty' THEN 'Yitty'
           ELSE store_brand END                                                                                         AS store_brand
     , clvm.month_date
     , CASE
           WHEN clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = FALSE THEN 'Activation'
           WHEN clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = TRUE THEN 'Reactivation'
           WHEN clvm.is_bop_vip = TRUE THEN 'BOP'
           WHEN clvm.IS_CANCEL = TRUE THEN 'Cancelled'
    END                                                                                                                 AS membership_monthly_status

     , case
           when clvm.is_reactivated_vip = TRUE then 'Reactivated Membership'
           when clvm.is_reactivated_vip = false
               then 'First Membership' end                                                                              AS membership_sequence
     , ds.store_region
     , ds.store_country
     , ds.store_name
     , sum(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_unit_count,
               0))                                                                                                      AS activating_product_order_unit_count
     , sum(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_count,
               0))                                                                                                      AS activating_product_order_count
     , sum(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_product_discount_amount,
               0))                                                                                                      AS activating_product_order_product_discount_amount
     , sum(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_subtotal_excl_tariff_amount,
               0))                                                                                                      AS activating_product_order_subtotal_excl_tariff_amount
     , sum(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_shipping_revenue_amount,
               0))                                                                                                      AS activating_product_order_shipping_revenue_amount
     , sum(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_gross_revenue,
               0))                                                                                                      AS activating_product_gross_revenue
     , sum(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_landed_product_cost_amount,
               0))                                                                                                      AS activating_product_order_landed_product_cost_amount
     , sum(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_unit_count,
               (clvm.product_order_unit_count - cwa.activating_product_order_unit_count)))                              AS nonactivating_product_order_unit_count
     , sum(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_count,
               (NVL(clvm.product_order_count, 0) - cwa.activating_product_order_count)))                                AS nonactivating_product_order_count
     , sum(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_product_discount_amount,
               (NVL(clvm.product_order_product_discount_amount, 0) -
                cwa.activating_product_order_product_discount_amount)))                                                 AS nonactivating_product_order_product_discount_amount
     , sum(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_subtotal_excl_tariff_amount,
               (NVL(clvm.product_order_subtotal_excl_tariff_amount, 0) -
                cwa.activating_product_order_subtotal_excl_tariff_amount)))                                             AS nonactivating_product_order_subtotal_excl_tariff_amount
     , sum(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_product_subtotal_amount,
               (NVL(clvm.product_order_product_subtotal_amount, 0) -
                cwa.activating_product_order_product_subtotal_amount)))                                                 AS nonactivating_product_order_subtotal_amount
     , sum(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_shipping_revenue_amount,
               (NVL(clvm.product_order_shipping_revenue_amount, 0) -
                cwa.activating_product_order_shipping_revenue_amount)))                                                 AS nonactivating_product_order_shipping_revenue_amount
     , sum(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_gross_revenue,
               (NVL(clvm.product_gross_revenue, 0) -
                cwa.activating_product_gross_revenue)))                                                                 AS nonactivating_product_gross_revenue
     , sum(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_landed_product_cost_amount,
               (NVL(clvm.product_order_landed_product_cost_amount, 0) -
                cwa.activating_product_order_landed_product_cost_amount)))                                              AS nonactivating_product_order_landed_product_cost_amount
     , sum((clvm.is_cancel = TRUE)::int)                                                                                AS vip_cancellation_count
     , sum((clvm.is_bop_vip = TRUE)::int)                                                                               AS bop_vip_count
     , sum((clvm.vip_cohort_month_date = clvm.month_date)::int)                                                         AS activation_count
     , sum((clvm.is_bop_vip = TRUE AND clvm.is_failed_billing = TRUE)::int)                                             AS failed_billing_count
     , sum((clvm.is_bop_vip = TRUE AND clvm.is_merch_purchaser = TRUE)::int)                                            AS merch_purchase_count
     , sum((clvm.is_bop_vip = TRUE AND (clvm.is_merch_purchaser = TRUE OR
                                        (clvm.is_successful_billing = TRUE AND clvm.is_pending_billing = FALSE)))::int) AS purchase_count
     , sum((clvm.vip_cohort_month_date = clvm.month_date AND
            clvm.is_reactivated_vip = TRUE)::int)                                                                       AS reactivation_count
     , sum((clvm.is_bop_vip = TRUE AND clvm.is_skip = TRUE)::int)                                                       AS skip_count
     , sum((clvm.is_bop_vip = TRUE AND clvm.is_passive_cancel = TRUE AND
            clvm.is_cancel = TRUE)::int)                                                                                AS vip_passive_cancellation_count
     , sum((clvm.is_successful_billing = TRUE)::int)                                                                    AS is_successful_billing_count
     , count(distinct clvm.customer_id)                                                                                 AS initial_cohort_count
     , sum(ifnull(clvm.online_product_order_count, 0))                                                                  AS online_product_order_count
     , sum(ifnull(clvm.retail_product_order_count, 0))                                                                  AS retail_product_order_count
     , sum(ifnull(clvm.mobile_app_product_order_count, 0))                                                              AS mobile_app_product_order_count
     , sum(ifnull(clvm.online_product_order_unit_count, 0))                                                             AS online_product_order_unit_count
     , sum(ifnull(clvm.retail_product_order_unit_count, 0))                                                             AS retail_product_order_unit_count
     , sum(ifnull(clvm.mobile_app_product_order_unit_count, 0))                                                         AS mobile_app_product_order_unit_count
     , sum((clvm.is_bop_vip = TRUE AND clvm.customer_action_category='Merch Purchase and Skip')::int)                   AS skip_and_purchase_count
     , sum((clvm.is_bop_vip = TRUE AND clvm.customer_action_category='Merch Purchase and Successful Billing')::int)     AS billing_and_purchase_count
     , sum((clvm.is_bop_vip = TRUE AND clvm.is_login = TRUE)::int)                                                      AS login_count
     , sum(
        ifnull(clvm.online_product_order_subtotal_excl_tariff_amount, 0))                                               AS online_product_order_subtotal_excl_tariff_amount
     , sum(
        ifnull(clvm.retail_product_order_subtotal_excl_tariff_amount, 0))                                               AS retail_product_order_subtotal_excl_tariff_amount
     , sum(
        ifnull(clvm.mobile_app_product_order_subtotal_excl_tariff_amount, 0))                                           AS mobile_app_product_order_subtotal_excl_tariff_amount
     , sum(ifnull(clvm.online_product_order_product_discount_amount, 0))                                                AS online_product_order_product_discount_amount
     , sum(ifnull(clvm.retail_product_order_product_discount_amount, 0))                                                AS retail_product_order_product_discount_amount
     , sum(
        ifnull(clvm.mobile_app_product_order_product_discount_amount, 0))                                               AS mobile_app_product_order_product_discount_amount
     , sum(ifnull(clvm.online_product_order_shipping_revenue_amount, 0))                                                AS online_product_order_shipping_revenue_amount
     , sum(ifnull(clvm.retail_product_order_shipping_revenue_amount, 0))                                                AS retail_product_order_shipping_revenue_amount
     , sum(
        ifnull(clvm.mobile_app_product_order_shipping_revenue_amount, 0))                                               AS mobile_app_product_order_shipping_revenue_amount
     , sum(ifnull(clvm.product_order_direct_cogs_amount, 0))                                                            AS product_order_direct_cogs_amount
     , sum(ifnull(clvm.online_product_order_direct_cogs_amount, 0))                                                     AS online_product_order_direct_cogs_amount
     , sum(ifnull(clvm.retail_product_order_direct_cogs_amount, 0))                                                     AS retail_product_order_direct_cogs_amount
     , sum(ifnull(clvm.mobile_app_product_order_direct_cogs_amount, 0))                                                 AS mobile_app_product_order_direct_cogs_amount
     , sum(ifnull(clvm.product_order_selling_expenses_amount, 0))                                                       AS product_order_selling_expenses_amount
     , sum(ifnull(clvm.online_product_order_selling_expenses_amount, 0))                                                AS online_product_order_selling_expenses_amount
     , sum(ifnull(clvm.retail_product_order_selling_expenses_amount, 0))                                                AS retail_product_order_selling_expenses_amount
     , sum(
        ifnull(clvm.mobile_app_product_order_selling_expenses_amount, 0))                                               AS mobile_app_product_order_selling_expenses_amount
     , sum(ifnull(clvm.product_order_cash_refund_amount_and_chargeback_amount,
                  0))                                                                                                   AS product_order_cash_refund_amount_and_chargeback_amount
     , sum(
        ifnull(clvm.online_product_order_cash_refund_chargeback_amount, 0))                                             AS online_product_order_cash_refund_chargeback_amount
     , sum(
        ifnull(clvm.retail_product_order_cash_refund_chargeback_amount, 0))                                             AS retail_product_order_cash_refund_chargeback_amount
     , sum(ifnull(clvm.mobile_app_product_order_cash_refund_chargeback_amount,
                  0))                                                                                                   AS mobile_app_product_order_cash_refund_chargeback_amount
     , sum(ifnull(clvm.product_order_cash_credit_refund_amount, 0))                                                     AS product_order_cash_credit_refund_amount
     , sum(ifnull(clvm.online_product_order_cash_credit_refund_amount, 0))                                              AS online_product_order_cash_credit_refund_amount
     , sum(ifnull(clvm.retail_product_order_cash_credit_refund_amount, 0))                                              AS retail_product_order_cash_credit_refund_amount
     , sum(
        ifnull(clvm.mobile_app_product_order_cash_credit_refund_amount, 0))                                             AS mobile_app_product_order_cash_credit_refund_amount
     , sum(ifnull(clvm.product_order_reship_exchange_order_count, 0))                                                   AS product_order_reship_exchange_order_count
     , sum(
        ifnull(clvm.online_product_order_reship_exchange_order_count, 0))                                               AS online_product_order_reship_exchange_order_count
     , sum(
        ifnull(clvm.retail_product_order_reship_exchange_order_count, 0))                                               AS retail_product_order_reship_exchange_order_count
     , sum(
        ifnull(clvm.mobile_app_product_order_reship_exchange_order_count, 0))                                           AS mobile_app_product_order_reship_exchange_order_count
     , sum(ifnull(clvm.product_order_reship_exchange_unit_count, 0))                                                    AS product_order_reship_exchange_unit_count
     , sum(
        ifnull(clvm.online_product_order_reship_exchange_unit_count, 0))                                                AS online_product_order_reship_exchange_unit_count
     , sum(
        ifnull(clvm.retail_product_order_reship_exchange_unit_count, 0))                                                AS retail_product_order_reship_exchange_unit_count
     , sum(
        ifnull(clvm.mobile_app_product_order_reship_exchange_unit_count, 0))                                            AS mobile_app_product_order_reship_exchange_unit_count
     , sum(
        ifnull(clvm.product_order_reship_exchange_direct_cogs_amount, 0))                                               AS product_order_reship_exchange_direct_cogs_amount
     , sum(ifnull(clvm.online_product_order_reship_exchange_direct_cogs_amount,
                  0))                                                                                                   AS online_product_order_reship_exchange_direct_cogs_amount
     , sum(ifnull(clvm.retail_product_order_reship_exchange_direct_cogs_amount,
                  0))                                                                                                   AS retail_product_order_reship_exchange_direct_cogs_amount
     , sum(ifnull(clvm.mobile_app_product_order_reship_exchange_direct_cogs_amount,
                  0))                                                                                                   AS mobile_app_product_order_reship_exchange_direct_cogs_amount
     , sum(ifnull(clvm.product_order_return_cogs_amount, 0))                                                            AS product_order_return_cogs_amount
     , sum(ifnull(clvm.online_product_order_return_cogs_amount, 0))                                                     AS online_product_order_return_cogs_amount
     , sum(ifnull(clvm.retail_product_order_return_cogs_amount, 0))                                                     AS retail_product_order_return_cogs_amount
     , sum(ifnull(clvm.mobile_app_product_order_return_cogs_amount, 0))                                                 AS mobile_app_product_order_return_cogs_amount
     , sum(ifnull(clvm.product_order_return_unit_count, 0))                                                             AS product_order_return_unit_count
     , sum(ifnull(clvm.online_product_order_return_unit_count, 0))                                                      AS online_product_order_return_unit_count
     , sum(ifnull(clvm.retail_product_order_return_unit_count, 0))                                                      AS retail_product_order_return_unit_count
     , sum(ifnull(clvm.mobile_app_product_order_return_unit_count, 0))                                                  AS mobile_app_product_order_return_unit_count
     , sum(ifnull(clvm.product_order_amount_to_pay, 0))                                                                 AS product_order_amount_to_pay
     , sum(ifnull(clvm.online_product_order_amount_to_pay, 0))                                                          AS online_product_order_amount_to_pay
     , sum(ifnull(clvm.retail_product_order_amount_to_pay, 0))                                                          AS retail_product_order_amount_to_pay
     , sum(ifnull(clvm.mobile_app_product_order_amount_to_pay, 0))                                                      AS mobile_app_product_order_amount_to_pay
     , sum(ifnull(clvm.product_gross_revenue_excl_shipping, 0))                                                         AS product_gross_revenue_excl_shipping
     , sum(ifnull(clvm.online_product_gross_revenue_excl_shipping, 0))                                                  AS online_product_gross_revenue_excl_shipping
     , sum(ifnull(clvm.retail_product_gross_revenue_excl_shipping, 0))                                                  AS retail_product_gross_revenue_excl_shipping
     , sum(ifnull(clvm.mobile_app_product_gross_revenue_excl_shipping, 0))                                              AS mobile_app_product_gross_revenue_excl_shipping
     , sum(ifnull(clvm.product_gross_revenue, 0))                                                                       AS product_gross_revenue
     , sum(ifnull(clvm.online_product_gross_revenue, 0))                                                                AS online_product_gross_revenue
     , sum(ifnull(clvm.retail_product_gross_revenue, 0))                                                                AS retail_product_gross_revenue
     , sum(ifnull(clvm.mobile_app_product_gross_revenue, 0))                                                            AS mobile_app_product_gross_revenue
     , sum(ifnull(clvm.product_net_revenue, 0))                                                                         AS product_net_revenue
     , sum(ifnull(clvm.online_product_net_revenue, 0))                                                                  AS online_product_net_revenue
     , sum(ifnull(clvm.retail_product_net_revenue, 0))                                                                  AS retail_product_net_revenue
     , sum(ifnull(clvm.mobile_app_product_net_revenue, 0))                                                              AS mobile_app_product_net_revenue
     , sum(ifnull(clvm.product_margin_pre_return, 0))                                                                   AS product_margin_pre_return
     , sum(ifnull(clvm.online_product_margin_pre_return, 0))                                                            AS online_product_margin_pre_return
     , sum(ifnull(clvm.retail_product_margin_pre_return, 0))                                                            AS retail_product_margin_pre_return
     , sum(ifnull(clvm.product_margin_pre_return_excl_shipping, 0))                                                     AS product_margin_pre_return_excl_shipping
     , sum(ifnull(clvm.online_product_margin_pre_return_excl_shipping, 0))                                              AS online_product_margin_pre_return_excl_shipping
     , sum(ifnull(clvm.retail_product_margin_pre_return_excl_shipping, 0))                                              AS retail_product_margin_pre_return_excl_shipping
     , sum(ifnull(clvm.product_gross_profit, 0))                                                                        AS product_gross_profit
     , sum(ifnull(clvm.online_product_gross_profit, 0))                                                                 AS online_product_gross_profit
     , sum(ifnull(clvm.retail_product_gross_profit, 0))                                                                 AS retail_product_gross_profit
     , sum(ifnull(clvm.mobile_app_product_gross_profit, 0))                                                             AS mobile_app_product_gross_profit
     , sum(ifnull(clvm.product_variable_contribution_profit, 0))                                                        AS product_variable_contribution_profit
     , sum(ifnull(clvm.online_product_variable_contribution_profit, 0))                                                 AS online_product_variable_contribution_profit
     , sum(ifnull(clvm.retail_product_variable_contribution_profit, 0))                                                 AS retail_product_variable_contribution_profit
     , sum(
        ifnull(clvm.mobile_app_product_variable_contribution_profit, 0))                                                AS mobile_app_product_variable_contribution_profit
     , sum(ifnull(clvm.product_order_cash_gross_revenue_amount, 0))                                                     AS product_order_cash_gross_revenue_amount
     , sum(ifnull(clvm.online_product_order_cash_gross_revenue_amount, 0))                                              AS online_product_order_cash_gross_revenue_amount
     , sum(ifnull(clvm.retail_product_order_cash_gross_revenue_amount, 0))                                              AS retail_product_order_cash_gross_revenue_amount
     , sum(
        ifnull(clvm.mobile_app_product_order_cash_gross_revenue_amount, 0))                                             AS mobile_app_product_order_cash_gross_revenue_amount
     , sum(ifnull(clvm.product_order_cash_net_revenue, 0))                                                              AS product_order_cash_net_revenue
     , sum(ifnull(clvm.online_product_order_cash_net_revenue, 0))                                                       AS online_product_order_cash_net_revenue
     , sum(ifnull(clvm.retail_product_order_cash_net_revenue, 0))                                                       AS retail_product_order_cash_net_revenue
     , sum(ifnull(clvm.mobile_app_product_order_cash_net_revenue, 0))                                                   AS mobile_app_product_order_cash_net_revenue
     , sum(ifnull(clvm.billing_cash_gross_revenue, 0))                                                                  AS billing_cash_gross_revenue
     , sum(ifnull(clvm.billing_cash_net_revenue, 0))                                                                    AS billing_cash_net_revenue
     , sum(ifnull(clvm.cash_gross_revenue, 0))                                                                          AS cash_gross_revenue
     , sum(ifnull(clvm.cash_net_revenue, 0))                                                                            AS cash_net_revenue
     , sum(ifnull(clvm.cash_gross_profit, 0))                                                                           AS cash_gross_profit
     , sum(ifnull(clvm.cash_variable_contribution_profit, 0))                                                           AS cash_variable_contribution_profit
     , sum(ifnull(clvm.monthly_billed_credit_cash_transaction_amount, 0))                                               AS monthly_billed_credit_cash_transaction_amount
     , sum(ifnull(clvm.membership_fee_cash_transaction_amount, 0))                                                      AS membership_fee_cash_transaction_amount
     , sum(ifnull(clvm.gift_card_transaction_amount, 0))                                                                AS gift_card_transaction_amount
     , sum(ifnull(clvm.legacy_credit_cash_transaction_amount, 0))                                                       AS legacy_credit_cash_transaction_amount
     , sum(
        ifnull(clvm.monthly_billed_credit_cash_refund_chargeback_amount, 0))                                            AS monthly_billed_credit_cash_refund_chargeback_amount
     , sum(ifnull(clvm.membership_fee_cash_refund_chargeback_amount, 0))                                                AS membership_fee_cash_refund_chargeback_amount
     , sum(ifnull(clvm.gift_card_cash_refund_chargeback_amount, 0))                                                     AS gift_card_cash_refund_chargeback_amount
     , sum(ifnull(clvm.legacy_credit_cash_refund_chargeback_amount, 0))                                                 AS legacy_credit_cash_refund_chargeback_amount
     , sum(ifnull(clvm.cumulative_cash_gross_profit, 0))                                                                AS cumulative_cash_gross_profit
     , sum(ifnull(clvm.cumulative_product_gross_profit, 0))                                                             AS cumulative_product_gross_profit
     , sum(ifnull(clvm.cumulative_cash_gross_profit_decile, 0))                                                         AS cumulative_cash_gross_profit_decile
     , sum(ifnull(clvm.cumulative_product_gross_profit_decile, 0))                                                      AS cumulative_product_gross_profit_decile
     , sum(ifnull(clvm.billed_cash_credit_cancelled_equivalent_count, 0))                                               AS billed_cash_credit_cancelled_equivalent_count
     , sum(ifnull(clvm.billed_cash_credit_redeemed_equivalent_count, 0))                                                AS billed_cash_credit_redeemed_equivalent_count
     , sum(ifnull(clvltd.customer_acquisition_cost, 0))                                                                 AS media_spend
     , sum(ifnull(clvm.billed_cash_credit_issued_amount, 0))                                                            AS billed_cash_credit_issued_amount
     , sum(ifnull(clvm.billed_cash_credit_redeemed_amount, 0))                                                          AS billed_cash_credit_redeemed_amount
     , sum(ifnull(clvm.billed_cash_credit_cancelled_amount, 0))                                                         AS billed_cash_credit_cancelled_amount
     , sum(ifnull(clvm.billed_cash_credit_expired_amount, 0))                                                           AS billed_cash_credit_expired_amount
     , sum(ifnull(clvm.billed_cash_credit_issued_equivalent_count, 0))                                                  AS billed_cash_credit_issued_equivalent_count
     , sum(ifnull(clvm.billed_cash_credit_expired_equivalent_count, 0))                                                 AS billed_cash_credit_expired_equivalent_count
FROM analytics_base.customer_lifetime_value_monthly AS clvm
         JOIN data_model.dim_store AS ds
              ON clvm.store_id = ds.store_id
         JOIN _cohort_waterfall_activating AS cwa
              ON cwa.customer_id = clvm.customer_id
                  AND cwa.activation_key = clvm.activation_key
         LEFT JOIN analytics_base.customer_lifetime_value_ltd as clvltd
                   ON clvm.customer_id = clvltd.customer_id
                       AND clvm.vip_cohort_month_date = clvltd.vip_cohort_month_date
                       AND clvm.month_date = clvltd.vip_cohort_month_date
WHERE clvm.month_date > $processing_date
GROUP BY clvm.vip_cohort_month_date,
         clvm.membership_type,
         CASE
             WHEN clvm.is_scrubs_customer = 1 AND clvm.vip_cohort_month_date >= '2023-02-01' AND clvm.GENDER = 'M' THEN 'Scrubs Mens'
             WHEN clvm.is_scrubs_customer = 1 AND clvm.vip_cohort_month_date >= '2023-02-01' THEN 'Scrubs Womens'
             WHEN store_brand = 'Fabletics' AND clvm.GENDER = 'M' THEN 'Fabletics Mens'
             WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
             WHEN store_brand = 'Yitty' THEN 'Yitty'
             ELSE store_brand END,
         clvm.month_date,
         CASE
             WHEN clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = FALSE THEN 'Activation'
             WHEN clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = TRUE THEN 'Reactivation'
             WHEN clvm.is_bop_vip = TRUE THEN 'BOP'
             WHEN clvm.IS_CANCEL = TRUE THEN 'Cancelled'
             END,
         case
             when clvm.is_reactivated_vip = TRUE then 'Reactivated Membership'
             when clvm.is_reactivated_vip = false then 'First Membership' end,
         ds.store_region,
         ds.store_country,
         ds.store_name;

BEGIN;

DELETE FROM  reporting.cohort_waterfall_final_output WHERE month_date > $processing_date;

INSERT INTO reporting.cohort_waterfall_final_output
( vip_cohort_month_date
, membership_type
, store_brand
, month_date
, membership_monthly_status
, membership_sequence
, store_region
, store_country
, store_name
, activating_product_order_unit_count
, activating_product_order_count
, activating_product_order_product_discount_amount
, activating_product_order_subtotal_excl_tariff_amount
, activating_product_order_shipping_revenue_amount
, activating_product_gross_revenue
, activating_product_order_landed_product_cost_amount
, nonactivating_product_order_unit_count
, nonactivating_product_order_count
, nonactivating_product_order_product_discount_amount
, nonactivating_product_order_subtotal_excl_tariff_amount
, nonactivating_product_order_subtotal_amount
, nonactivating_product_order_shipping_revenue_amount
, nonactivating_product_gross_revenue
, nonactivating_product_order_landed_product_cost_amount
, vip_cancellation_count
, bop_vip_count
, activation_count
, failed_billing_count
, merch_purchase_count
, purchase_count
, reactivation_count
, skip_count
, vip_passive_cancellation_count
, is_successful_billing_count
, initial_cohort_count
, online_product_order_count
, retail_product_order_count
, mobile_app_product_order_count
, online_product_order_unit_count
, retail_product_order_unit_count
, mobile_app_product_order_unit_count
, skip_and_purchase_count
, billing_and_purchase_count
, login_count
, online_product_order_subtotal_excl_tariff_amount
, retail_product_order_subtotal_excl_tariff_amount
, mobile_app_product_order_subtotal_excl_tariff_amount
, online_product_order_product_discount_amount
, retail_product_order_product_discount_amount
, mobile_app_product_order_product_discount_amount
, online_product_order_shipping_revenue_amount
, retail_product_order_shipping_revenue_amount
, mobile_app_product_order_shipping_revenue_amount
, product_order_direct_cogs_amount
, online_product_order_direct_cogs_amount
, retail_product_order_direct_cogs_amount
, mobile_app_product_order_direct_cogs_amount
, product_order_selling_expenses_amount
, online_product_order_selling_expenses_amount
, retail_product_order_selling_expenses_amount
, mobile_app_product_order_selling_expenses_amount
, product_order_cash_refund_amount_and_chargeback_amount
, online_product_order_cash_refund_chargeback_amount
, retail_product_order_cash_refund_chargeback_amount
, mobile_app_product_order_cash_refund_chargeback_amount
, product_order_cash_credit_refund_amount
, online_product_order_cash_credit_refund_amount
, retail_product_order_cash_credit_refund_amount
, mobile_app_product_order_cash_credit_refund_amount
, product_order_reship_exchange_order_count
, online_product_order_reship_exchange_order_count
, retail_product_order_reship_exchange_order_count
, mobile_app_product_order_reship_exchange_order_count
, product_order_reship_exchange_unit_count
, online_product_order_reship_exchange_unit_count
, retail_product_order_reship_exchange_unit_count
, mobile_app_product_order_reship_exchange_unit_count
, product_order_reship_exchange_direct_cogs_amount
, online_product_order_reship_exchange_direct_cogs_amount
, retail_product_order_reship_exchange_direct_cogs_amount
, mobile_app_product_order_reship_exchange_direct_cogs_amount
, product_order_return_cogs_amount
, online_product_order_return_cogs_amount
, retail_product_order_return_cogs_amount
, mobile_app_product_order_return_cogs_amount
, product_order_return_unit_count
, online_product_order_return_unit_count
, retail_product_order_return_unit_count
, mobile_app_product_order_return_unit_count
, product_order_amount_to_pay
, online_product_order_amount_to_pay
, retail_product_order_amount_to_pay
, mobile_app_product_order_amount_to_pay
, product_gross_revenue_excl_shipping
, online_product_gross_revenue_excl_shipping
, retail_product_gross_revenue_excl_shipping
, mobile_app_product_gross_revenue_excl_shipping
, product_gross_revenue
, online_product_gross_revenue
, retail_product_gross_revenue
, mobile_app_product_gross_revenue
, product_net_revenue
, online_product_net_revenue
, retail_product_net_revenue
, mobile_app_product_net_revenue
, product_margin_pre_return
, online_product_margin_pre_return
, retail_product_margin_pre_return
, product_margin_pre_return_excl_shipping
, online_product_margin_pre_return_excl_shipping
, retail_product_margin_pre_return_excl_shipping
, product_gross_profit
, online_product_gross_profit
, retail_product_gross_profit
, mobile_app_product_gross_profit
, product_variable_contribution_profit
, online_product_variable_contribution_profit
, retail_product_variable_contribution_profit
, mobile_app_product_variable_contribution_profit
, product_order_cash_gross_revenue_amount
, online_product_order_cash_gross_revenue_amount
, retail_product_order_cash_gross_revenue_amount
, mobile_app_product_order_cash_gross_revenue_amount
, product_order_cash_net_revenue
, online_product_order_cash_net_revenue
, retail_product_order_cash_net_revenue
, mobile_app_product_order_cash_net_revenue
, billing_cash_gross_revenue
, billing_cash_net_revenue
, cash_gross_revenue
, cash_net_revenue
, cash_gross_profit
, cash_variable_contribution_profit
, monthly_billed_credit_cash_transaction_amount
, membership_fee_cash_transaction_amount
, gift_card_transaction_amount
, legacy_credit_cash_transaction_amount
, monthly_billed_credit_cash_refund_chargeback_amount
, membership_fee_cash_refund_chargeback_amount
, gift_card_cash_refund_chargeback_amount
, legacy_credit_cash_refund_chargeback_amount
, cumulative_cash_gross_profit
, cumulative_product_gross_profit
, cumulative_cash_gross_profit_decile
, cumulative_product_gross_profit_decile
, billed_cash_credit_cancelled_equivalent_count
, billed_cash_credit_redeemed_equivalent_count
, media_spend
, billed_cash_credit_issued_amount
, billed_cash_credit_redeemed_amount
, billed_cash_credit_cancelled_amount
, billed_cash_credit_expired_amount
, billed_cash_credit_issued_equivalent_count
, billed_cash_credit_expired_equivalent_count
, order_count_with_credit_redemption
, meta_create_datetime
, meta_update_datetime)

SELECT cwfos.vip_cohort_month_date
     , cwfos.membership_type
     , cwfos.store_brand
     , cwfos.month_date
     , cwfos.membership_monthly_status
     , cwfos.membership_sequence
     , cwfos.store_region
     , cwfos.store_country
     , cwfos.store_name
     , cwfos.activating_product_order_unit_count
     , cwfos.activating_product_order_count
     , cwfos.activating_product_order_product_discount_amount
     , cwfos.activating_product_order_subtotal_excl_tariff_amount
     , cwfos.activating_product_order_shipping_revenue_amount
     , cwfos.activating_product_gross_revenue
     , cwfos.activating_product_order_landed_product_cost_amount
     , cwfos.nonactivating_product_order_unit_count
     , cwfos.nonactivating_product_order_count
     , cwfos.nonactivating_product_order_product_discount_amount
     , cwfos.nonactivating_product_order_subtotal_excl_tariff_amount
     , cwfos.nonactivating_product_order_subtotal_amount
     , cwfos.nonactivating_product_order_shipping_revenue_amount
     , cwfos.nonactivating_product_gross_revenue
     , cwfos.nonactivating_product_order_landed_product_cost_amount
     , cwfos.vip_cancellation_count
     , cwfos.bop_vip_count
     , cwfos.activation_count
     , cwfos.failed_billing_count
     , cwfos.merch_purchase_count
     , cwfos.purchase_count
     , cwfos.reactivation_count
     , cwfos.skip_count
     , cwfos.vip_passive_cancellation_count
     , cwfos.is_successful_billing_count
     , cwfos.initial_cohort_count
     , cwfos.online_product_order_count
     , cwfos.retail_product_order_count
     , cwfos.mobile_app_product_order_count
     , cwfos.online_product_order_unit_count
     , cwfos.retail_product_order_unit_count
     , cwfos.mobile_app_product_order_unit_count
     , cwfos.skip_and_purchase_count
     , cwfos.billing_and_purchase_count
     , cwfos.login_count
     , cwfos.online_product_order_subtotal_excl_tariff_amount
     , cwfos.retail_product_order_subtotal_excl_tariff_amount
     , cwfos.mobile_app_product_order_subtotal_excl_tariff_amount
     , cwfos.online_product_order_product_discount_amount
     , cwfos.retail_product_order_product_discount_amount
     , cwfos.mobile_app_product_order_product_discount_amount
     , cwfos.online_product_order_shipping_revenue_amount
     , cwfos.retail_product_order_shipping_revenue_amount
     , cwfos.mobile_app_product_order_shipping_revenue_amount
     , cwfos.product_order_direct_cogs_amount
     , cwfos.online_product_order_direct_cogs_amount
     , cwfos.retail_product_order_direct_cogs_amount
     , cwfos.mobile_app_product_order_direct_cogs_amount
     , cwfos.product_order_selling_expenses_amount
     , cwfos.online_product_order_selling_expenses_amount
     , cwfos.retail_product_order_selling_expenses_amount
     , cwfos.mobile_app_product_order_selling_expenses_amount
     , cwfos.product_order_cash_refund_amount_and_chargeback_amount
     , cwfos.online_product_order_cash_refund_chargeback_amount
     , cwfos.retail_product_order_cash_refund_chargeback_amount
     , cwfos.mobile_app_product_order_cash_refund_chargeback_amount
     , cwfos.product_order_cash_credit_refund_amount
     , cwfos.online_product_order_cash_credit_refund_amount
     , cwfos.retail_product_order_cash_credit_refund_amount
     , cwfos.mobile_app_product_order_cash_credit_refund_amount
     , cwfos.product_order_reship_exchange_order_count
     , cwfos.online_product_order_reship_exchange_order_count
     , cwfos.retail_product_order_reship_exchange_order_count
     , cwfos.mobile_app_product_order_reship_exchange_order_count
     , cwfos.product_order_reship_exchange_unit_count
     , cwfos.online_product_order_reship_exchange_unit_count
     , cwfos.retail_product_order_reship_exchange_unit_count
     , cwfos.mobile_app_product_order_reship_exchange_unit_count
     , cwfos.product_order_reship_exchange_direct_cogs_amount
     , cwfos.online_product_order_reship_exchange_direct_cogs_amount
     , cwfos.retail_product_order_reship_exchange_direct_cogs_amount
     , cwfos.mobile_app_product_order_reship_exchange_direct_cogs_amount
     , cwfos.product_order_return_cogs_amount
     , cwfos.online_product_order_return_cogs_amount
     , cwfos.retail_product_order_return_cogs_amount
     , cwfos.mobile_app_product_order_return_cogs_amount
     , cwfos.product_order_return_unit_count
     , cwfos.online_product_order_return_unit_count
     , cwfos.retail_product_order_return_unit_count
     , cwfos.mobile_app_product_order_return_unit_count
     , cwfos.product_order_amount_to_pay
     , cwfos.online_product_order_amount_to_pay
     , cwfos.retail_product_order_amount_to_pay
     , cwfos.mobile_app_product_order_amount_to_pay
     , cwfos.product_gross_revenue_excl_shipping
     , cwfos.online_product_gross_revenue_excl_shipping
     , cwfos.retail_product_gross_revenue_excl_shipping
     , cwfos.mobile_app_product_gross_revenue_excl_shipping
     , cwfos.product_gross_revenue
     , cwfos.online_product_gross_revenue
     , cwfos.retail_product_gross_revenue
     , cwfos.mobile_app_product_gross_revenue
     , cwfos.product_net_revenue
     , cwfos.online_product_net_revenue
     , cwfos.retail_product_net_revenue
     , cwfos.mobile_app_product_net_revenue
     , cwfos.product_margin_pre_return
     , cwfos.online_product_margin_pre_return
     , cwfos.retail_product_margin_pre_return
     , cwfos.product_margin_pre_return_excl_shipping
     , cwfos.online_product_margin_pre_return_excl_shipping
     , cwfos.retail_product_margin_pre_return_excl_shipping
     , cwfos.product_gross_profit
     , cwfos.online_product_gross_profit
     , cwfos.retail_product_gross_profit
     , cwfos.mobile_app_product_gross_profit
     , cwfos.product_variable_contribution_profit
     , cwfos.online_product_variable_contribution_profit
     , cwfos.retail_product_variable_contribution_profit
     , cwfos.mobile_app_product_variable_contribution_profit
     , cwfos.product_order_cash_gross_revenue_amount
     , cwfos.online_product_order_cash_gross_revenue_amount
     , cwfos.retail_product_order_cash_gross_revenue_amount
     , cwfos.mobile_app_product_order_cash_gross_revenue_amount
     , cwfos.product_order_cash_net_revenue
     , cwfos.online_product_order_cash_net_revenue
     , cwfos.retail_product_order_cash_net_revenue
     , cwfos.mobile_app_product_order_cash_net_revenue
     , cwfos.billing_cash_gross_revenue
     , cwfos.billing_cash_net_revenue
     , cwfos.cash_gross_revenue
     , cwfos.cash_net_revenue
     , cwfos.cash_gross_profit
     , cwfos.cash_variable_contribution_profit
     , cwfos.monthly_billed_credit_cash_transaction_amount
     , cwfos.membership_fee_cash_transaction_amount
     , cwfos.gift_card_transaction_amount
     , cwfos.legacy_credit_cash_transaction_amount
     , cwfos.monthly_billed_credit_cash_refund_chargeback_amount
     , cwfos.membership_fee_cash_refund_chargeback_amount
     , cwfos.gift_card_cash_refund_chargeback_amount
     , cwfos.legacy_credit_cash_refund_chargeback_amount
     , cwfos.cumulative_cash_gross_profit
     , cwfos.cumulative_product_gross_profit
     , cwfos.cumulative_cash_gross_profit_decile
     , cwfos.cumulative_product_gross_profit_decile
     , cwfos.billed_cash_credit_cancelled_equivalent_count
     , cwfos.billed_cash_credit_redeemed_equivalent_count
     , cwfos.media_spend
	 , cwfos.billed_cash_credit_issued_amount
	 , cwfos.billed_cash_credit_redeemed_amount
	 , cwfos.billed_cash_credit_cancelled_amount
	 , cwfos.billed_cash_credit_expired_amount
	 , cwfos.billed_cash_credit_issued_equivalent_count
	 , cwfos.billed_cash_credit_expired_equivalent_count
     , ocwcr.order_count_with_credit_redemption
     , $execution_start_time
     , $execution_start_time
FROM _cohort_waterfall_final_output_stg cwfos
LEFT JOIN _order_count_with_credit_redemption ocwcr ON ocwcr.vip_cohort_month_date = cwfos.vip_cohort_month_date
    AND ocwcr.membership_type = cwfos.membership_type
    AND ocwcr.store_brand = cwfos.store_brand
    AND ocwcr.month_date = cwfos.month_date
    AND ocwcr.membership_monthly_status = cwfos.membership_monthly_status
    AND ocwcr.membership_sequence = cwfos.membership_sequence
    AND ocwcr.store_region = cwfos.store_region
    AND ocwcr.store_country = cwfos.store_country
    AND ocwcr.store_name = cwfos.store_name
ORDER BY cwfos.vip_cohort_month_date,
         cwfos.month_date;
COMMIT;
