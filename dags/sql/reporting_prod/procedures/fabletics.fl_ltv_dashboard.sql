SET target_table = 'fabletics.fl013_fl_ltv_dashboard';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
ALTER SESSION SET QUERY_TAG = $target_table;
-- Runs full refresh on Saturday and an incremental refresh on other week days
SET month_date = IFF(DAYOFWEEK($execution_start_time) = 6, '1900-01-01'::DATE,
                     DATEADD(MONTH, -12, DATE_TRUNC(MONTH, $execution_start_time))::DATE);
-- Deletes data according to the month_date variable
DELETE
FROM fabletics.fl013_fl_ltv_dashboard
WHERE month_date >= $month_date;

CREATE OR REPLACE TEMPORARY TABLE _customer_lifetime__customer_prebase AS
SELECT cltv.customer_id,
       cltv.vip_cohort_month_date,
       cltv.guest_cohort_month_date,
       cltv.month_date,
       cltv.is_reactivated_vip,
       cltv.cumulative_cash_gross_profit_decile,
       cltv.cumulative_product_gross_profit_decile,
       cltv.is_scrubs_customer,
       cltv.activation_key,
       cltv.store_id
FROM edw_prod.analytics_base.customer_lifetime_value_monthly cltv
         JOIN edw_prod.data_model_fl.dim_store AS st ON st.store_id = cltv.store_id
WHERE st.store_brand IN ('Fabletics', 'Yitty')
  AND cltv.month_date >= $month_date;

CREATE OR REPLACE TEMPORARY TABLE _shopping_channel AS
SELECT CASE
           WHEN a.shopped_retail_flag = 1 AND a.shopped_online_flag = 0 AND a.shopped_mobile_app_flag = 0
               THEN 'Retail Only'
           WHEN a.shopped_retail_flag = 1 AND a.shopped_online_flag = 1 AND a.shopped_mobile_app_flag = 0
               THEN 'Online + Retail'
           WHEN a.shopped_retail_flag = 1 AND a.shopped_online_flag = 1 AND a.shopped_mobile_app_flag = 1
               THEN 'Online + Retail + Mobile App'
           WHEN a.shopped_retail_flag = 0 AND a.shopped_online_flag = 1 AND a.shopped_mobile_app_flag = 0
               THEN 'Online Only'
           WHEN a.shopped_retail_flag = 0 AND a.shopped_online_flag = 1 AND a.shopped_mobile_app_flag = 1
               THEN 'Online + Mobile App'
           WHEN a.shopped_retail_flag = 0 AND a.shopped_online_flag = 0 AND a.shopped_mobile_app_flag = 1
               THEN 'Mobile App Only'
           WHEN a.shopped_retail_flag = 1 AND a.shopped_online_flag = 0 AND a.shopped_mobile_app_flag = 1
               THEN 'Retail + Mobile App'
           ELSE 'reactivation with no product order' END AS ltv_type,
       a.customer_id,
       a.vip_cohort
FROM (SELECT cltv.customer_id,
             cltv.vip_cohort_month_date                                          AS vip_cohort,
             CASE WHEN SUM(retail_product_order_count) > 0 THEN 1 ELSE 0 END     AS shopped_retail_flag,
             CASE WHEN SUM(online_product_order_count) > 0 THEN 1 ELSE 0 END     AS shopped_online_flag,
             CASE WHEN SUM(mobile_app_product_order_count) > 0 THEN 1 ELSE 0 END AS shopped_mobile_app_flag
      FROM _customer_lifetime__customer_prebase base
               JOIN edw_prod.analytics_base.customer_lifetime_value_monthly cltv
                    ON base.customer_id = cltv.customer_id
                        AND base.vip_cohort_month_date = cltv.vip_cohort_month_date
      GROUP BY cltv.customer_id, cltv.vip_cohort_month_date) a
;

CREATE OR REPLACE TEMPORARY TABLE _all AS
SELECT sc.ltv_type,
       CASE WHEN ltvm.vip_cohort_month_date = '1900-01-01' THEN 'Guest' ELSE 'VIP' END AS membership_state,
       ltvm.vip_cohort_month_date                                                      AS vip_cohort,
       ltvm.guest_cohort_month_date                                                    AS guest_cohort,
       DATE_TRUNC(YEAR, ltvm.vip_cohort_month_date)                                    AS activation_year,
       ltvm.month_date,
       CASE
           WHEN dc.gender ILIKE 'M' AND dc.registration_local_datetime < '2020-01-01' THEN 'F'
           WHEN dc.gender ILIKE 'M' THEN 'M'
           ELSE 'F'
           END                                                                         AS customer_gender,
       COALESCE(a.is_retail_vip, FALSE)                                                AS is_retail_vip,
       COALESCE(ltvm.is_reactivated_vip, FALSE)                                        AS is_reactivated_vip,
       CASE
           WHEN a.is_retail_varsity_vip = 1 THEN 'Varsity'
           WHEN (a.is_legging_bar_vip = 1 OR a.is_kiosk_vip = 1 OR a.store_sub_type ILIKE 'Legging Bar')
               THEN 'Legging Bar'
           WHEN a.store_sub_type ILIKE 'Tough Mudder' THEN 'Tough Mudder'
           WHEN (a.is_retail_vip = 1 AND a.is_retail_varsity_vip = 0 AND a.is_legging_bar_vip = 0 AND
                 a.is_kiosk_vip = 0) THEN '4-Wall'
           ELSE 'Guest' END                                                            AS store_type,
       ltvm.cumulative_cash_gross_profit_decile,
       ltvm.cumulative_product_gross_profit_decile,
       IFF(ltvm.is_scrubs_customer = 1 AND ltvm.vip_cohort_month_date >= '2023-02-01', 'Scrubs',
           st.store_brand)                                                             AS store_brand_name,
       st.store_country                                                                AS store_country_abbr,
       st.store_region                                                                 AS store_region_abbr,
       CASE
           WHEN pf.product_order_count < 6 THEN pf.product_order_count
           ELSE 6 END                                                                  AS product_purchases,
       --,mat.max_active_tenure
       --,mat.max_month_date
       DATEDIFF(MONTH, vip_cohort, ltvm.month_date) + 1                                AS tenure,
       COUNT(DISTINCT ltvm.customer_id)                                                AS customer_count,
       SUM(ltvm.product_gross_revenue)                                                 AS product_gross_revenue,
       SUM(ltvm.product_net_revenue)                                                   AS product_net_revenue,
       SUM(ltvm.product_margin_pre_return)                                             AS pre_return_product_margin,
       SUM(ltvm.product_gross_profit)                                                  AS product_margin,
       SUM(ltvm.product_order_cash_gross_revenue_amount)                               AS product_order_cash_gross_revenue,
       SUM(ltvm.product_order_cash_net_revenue)                                        AS product_order_cash_net_revenue,
       SUM(ltvm.cash_gross_revenue)                                                    AS cash_gross_revenue,
       SUM(ltvm.cash_net_revenue)                                                      AS cash_net_revenue,
       SUM(ltvm.product_order_cash_margin_pre_return)                                  AS product_order_pre_return_cash_margin,
       SUM(ltvm.cash_gross_profit)                                                     AS cash_margin,
       SUM(ltvm.product_order_cash_gross_profit)                                       AS product_order_cash_margin,
       SUM(ltvm.product_order_count)                                                   AS product_order_count,
       SUM(ltvm.product_order_unit_count)                                              AS product_order_units,
       SUM(ltvm.product_order_subtotal_excl_tariff_amount)                             AS product_order_subtotal,
       SUM(ltvm.product_order_product_discount_amount)                                 AS product_order_discount,
       SUM(ltvm.product_order_landed_product_cost_amount)                              AS product_order_product_cost,
       SUM(ltvm.billed_cash_credit_issued_equivalent_count)                            AS credit_billing_count,
       SUM(ltvm.billed_cash_credit_issued_amount)                                      AS credit_billing_amount,
       SUM(ltvm.billed_cash_credit_redeemed_equivalent_count)                          AS product_order_membership_credit_redeemed_count,
       SUM(ltvm.billed_cash_credit_issued_amount)                                      AS product_order_membership_credit_redeemed_amount,
       COUNT(DISTINCT CASE
                          WHEN (ltvm.is_merch_purchaser >= 1 OR
                                ltvm.is_successful_billing >= 1 AND ltvm.is_bop_vip >= 1)
                              THEN ltvm.customer_id END)                               AS cash_purchasers,
       COUNT(DISTINCT CASE
                          WHEN ltvm.is_merch_purchaser >= 1 AND ltvm.is_bop_vip >= 1
                              THEN ltvm.customer_id END)                               AS merch_purchasers,
       COUNT(DISTINCT CASE
                          WHEN ltvm.is_successful_billing >= 1 AND ltvm.is_bop_vip >= 1
                              THEN ltvm.customer_id END)                               AS credit_billees,
       COUNT(DISTINCT CASE
                          WHEN ltvm.is_skip >= 1 AND ltvm.is_bop_vip >= 1
                              THEN ltvm.customer_id END)                               AS skippers,
       COUNT(DISTINCT CASE
                          WHEN ltvm.is_failed_billing >= 1 AND ltvm.is_bop_vip >= 1
                              THEN ltvm.customer_id END)                               AS failed_billees,
       COUNT(DISTINCT CASE WHEN ltvm.is_bop_vip >= 1 THEN ltvm.customer_id END)        AS bop_vips,
       COUNT(DISTINCT CASE
                          WHEN ltvm.billed_cash_credit_cancelled_equivalent_count >= 1 AND ltvm.is_bop_vip >= 1
                              THEN ltvm.customer_id END)                               AS credit_cancellers,
       COUNT(DISTINCT CASE
                          WHEN ltvm.billed_cash_credit_redeemed_equivalent_count >= 1 AND ltvm.is_bop_vip >= 1
                              THEN ltvm.customer_id END)                               AS credit_redeemers,
       COUNT(DISTINCT CASE
                          WHEN ltvm.is_login >= 1 AND ltvm.is_bop_vip >= 1
                              THEN ltvm.customer_id END)                               AS logged_in,
       COUNT(DISTINCT CASE WHEN ltvm.is_cancel >= 1 THEN ltvm.customer_id END)         AS cancels,
       COUNT(DISTINCT CASE WHEN ltvm.vip_cohort_month_date = ltvm.month_date then  ltvm.customer_id END) as new_vips
FROM _customer_lifetime__customer_prebase base
         JOIN edw_prod.analytics_base.customer_lifetime_value_monthly ltvm
              ON base.customer_id = ltvm.customer_id
                  AND base.vip_cohort_month_date = ltvm.vip_cohort_month_date
                  AND base.guest_cohort_month_date = ltvm.guest_cohort_month_date
                  AND base.month_date = ltvm.month_date
                  AND base.is_reactivated_vip = ltvm.is_reactivated_vip
                  AND base.cumulative_cash_gross_profit_decile = ltvm.cumulative_cash_gross_profit_decile
                  AND base.cumulative_product_gross_profit_decile = ltvm.cumulative_product_gross_profit_decile
                  AND base.is_scrubs_customer = ltvm.is_scrubs_customer
                  AND base.activation_key = ltvm.activation_key
                  AND base.store_id = ltvm.store_id
         JOIN edw_prod.data_model_fl.dim_customer dc
              ON dc.customer_id = edw_prod.stg.udf_unconcat_brand(ltvm.customer_id)
         LEFT JOIN edw_prod.data_model_fl.fact_activation a ON a.activation_key = ltvm.activation_key
         JOIN edw_prod.data_model_fl.dim_store AS st ON st.store_id = ltvm.store_id
         JOIN _shopping_channel sc ON sc.customer_id = ltvm.customer_id AND sc.vip_cohort = ltvm.vip_cohort_month_date
         LEFT JOIN edw_prod.analytics_base.customer_lifetime_value_ltd pf
                   ON ltvm.customer_id = pf.customer_id AND ltvm.activation_key = pf.activation_key
--LEFT JOIN _max_active_tenure mat ON mat.customer_id = ltvm.customer_id AND mat.activation_key = ltvm.activation_key
WHERE dc.is_test_customer = 0
  AND st.store_brand IN ('Fabletics', 'Yitty')
  AND ltvm.VIP_COHORT_MONTH_DATE != '1900-01-01'
  AND ltvm.MONTH_DATE>= '2019-01-01'
--AND vip_cohort < DATE_TRUNC('month',CURRENT_DATE()) --filter out incomplete months
--AND ltvm.month_date < DATE_TRUNC('month',CURRENT_DATE()) --filter out incomplete months
GROUP BY all;
-- Inserts data into final table
INSERT INTO fabletics.fl013_fl_ltv_dashboard
(ltv_type,
 membership_state,
 vip_cohort,
 guest_cohort,
 activation_year,
 month_date,
 customer_gender,
 is_retail_vip,
 is_reactivated_vip,
 store_type,
 cumulative_cash_gross_profit_decile,
 cumulative_product_gross_profit_decile,
 store_brand_name,
 store_country_abbr,
 store_region_abbr,
 product_purchases,
 tenure,
 customer_count,
 product_gross_revenue,
 product_net_revenue,
 pre_return_product_margin,
 product_margin,
 product_order_cash_gross_revenue,
 product_order_cash_net_revenue,
 cash_gross_revenue,
 cash_net_revenue,
 product_order_pre_return_cash_margin,
 cash_margin,
 product_order_cash_margin,
 product_order_count,
 product_order_units,
 product_order_subtotal,
 product_order_discount,
 product_order_product_cost,
 credit_billing_count,
 credit_billing_amount,
 product_order_membership_credit_redeemed_count,
 product_order_membership_credit_redeemed_amount,
 cash_purchasers,
 merch_purchasers,
 credit_billees,
 skippers,
 failed_billees,
 bop_vips,
 credit_cancellers,
 credit_redeemers,
 logged_in,
 cancels,
 new_vips, --calced above
 eop_vips,
 datetime_added)
SELECT *,
       LEAD(SUM(bop_vips)) OVER (PARTITION BY
                           ltv_type,
                           membership_state,
                           vip_cohort,
                           activation_year,
                           customer_gender,
                           is_retail_vip,
                           is_reactivated_vip,
                           store_type,
                           cumulative_cash_gross_profit_decile,
                           cumulative_product_gross_profit_decile,
                           store_brand_name,
                           store_country_abbr,
                           store_region_abbr,
                           product_purchases
                           ORDER BY month_date) AS eop_vips,
       CURRENT_TIMESTAMP()      AS datetime_added
FROM _all
group by all;
