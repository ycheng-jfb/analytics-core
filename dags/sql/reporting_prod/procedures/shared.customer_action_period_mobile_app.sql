SET target_table = 'reporting_prod.shared.customer_action_period_mobile_app';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));
SET last_refreshed_time = (SELECT MAX(CONVERT_TIMEZONE('America/Los_Angeles', session_local_datetime))::TIMESTAMP_NTZ(3)
                           FROM shared.sessions_by_platform);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (SELECT $target_table                               AS table_name,
                  NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
                  new_high_watermark_datetime                 AS new_high_watermark_datetime
           FROM (SELECT -- For self table
                        NULL                      AS dependent_table_name,
                        MAX(meta_update_datetime) AS new_high_watermark_datetime
                 FROM shared.customer_action_period_mobile_app
                 UNION ALL
                 SELECT 'reporting_prod.shared.sessions_by_platform' AS dependent_table_name,
                        MAX(meta_update_datetime)                        AS new_high_watermark_datetime
                 FROM shared.sessions_by_platform
                 UNION ALL
                 SELECT 'reporting_prod.shared.sessions_order_stg' AS dependent_table_name,
                        MAX(meta_update_datetime)                  AS new_high_watermark_datetime
                 FROM shared.sessions_order_stg
                 UNION ALL
                 SELECT 'edw_prod.analytics_base.customer_lifetime_value_monthly' AS dependent_table_name,
                        MAX(meta_update_datetime)                                 AS new_high_watermark_datetime
                 FROM edw_prod.analytics_base.customer_lifetime_value_monthly
                 UNION ALL
                 SELECT 'edw_prod.stg.dim_customer' AS dependent_table_name,
                        MAX(meta_update_datetime)   AS new_high_watermark_datetime
                 FROM edw_prod.stg.dim_customer) AS h) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
        t.new_high_watermark_datetime = s.new_high_watermark_datetime,
        t.meta_update_datetime = CURRENT_TIMESTAMP::timestamp_ltz(3)
    WHEN NOT MATCHED
        THEN INSERT (
                     table_name,
                     dependent_table_name,
                     high_watermark_datetime,
                     new_high_watermark_datetime
        )
        VALUES (s.table_name,
                s.dependent_table_name,
                '1900-01-01'::timestamp_ltz,
                s.new_high_watermark_datetime);

SET wm_reporting_prod_shared_sessions_by_platform = public.udf_get_watermark($target_table,
                                                                             'reporting_prod.shared.sessions_by_platform');
SET wm_reporting_prod_shared_sessions_order_stg = public.udf_get_watermark($target_table,
                                                                           'reporting_prod.shared.sessions_order_stg');

SET wm_edw_prod_analytics_base_cltv_monthly = public.udf_get_watermark($target_table,
                                                                       'edw_prod.analytics_base.customer_lifetime_value_monthly');
SET wm_edw_prod_stg_dim_customer = public.udf_get_watermark($target_table,
                                                            'edw_prod.stg.dim_customer');

CREATE OR REPLACE TEMP TABLE _customer_action_period_mobile_app__base
(
    customer_id NUMBER
);

-- full refresh
INSERT INTO _customer_action_period_mobile_app__base
SELECT clvm.customer_id
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvm
         JOIN edw_prod.data_model.dim_store AS st ON st.store_id = clvm.store_id
WHERE st.store_brand IN ('JustFab', 'Fabletics')
  AND st.store_country IN ('DE', 'ES', 'FR', 'UK', 'US')
  AND clvm.month_date >= '2019-01-01'
  AND clvm.is_bop_vip = TRUE
  AND $is_full_refresh = TRUE;

-- incremental refresh
INSERT INTO _customer_action_period_mobile_app__base
SELECT DISTINCT customer_id
FROM (SELECT customer_id, store_id
      FROM edw_prod.analytics_base.customer_lifetime_value_monthly
      WHERE meta_update_datetime > $wm_edw_prod_analytics_base_cltv_monthly
      UNION ALL
      SELECT customer_id, session_store_id
      FROM shared.sessions_by_platform
      WHERE meta_update_datetime > $wm_reporting_prod_shared_sessions_by_platform
      UNION ALL
      SELECT customer_id, session_store_id
      FROM shared.sessions_order_stg
      WHERE meta_update_datetime > $wm_reporting_prod_shared_sessions_order_stg
      UNION ALL
      SELECT customer_id, store_id
      FROM edw_prod.stg.dim_customer
      WHERE meta_update_datetime > $wm_edw_prod_stg_dim_customer) AS incr
         JOIN edw_prod.data_model.dim_store AS st ON st.store_id = incr.store_id
WHERE st.store_brand IN ('JustFab', 'Fabletics')
  AND st.store_country IN ('DE', 'ES', 'FR', 'UK', 'US')
  AND $is_full_refresh = FALSE;


CREATE OR REPLACE TEMP TABLE _customer_action_period__skips AS
SELECT p.customer_id,
       p.session_month_date,
       p.session_platform,
       p.operating_system,
       RANK() OVER (PARTITION BY p.customer_id, p.session_month_date ORDER BY p.session_id DESC) AS rnk_skip
FROM _customer_action_period_mobile_app__base base
         JOIN shared.sessions_by_platform p
              ON base.customer_id = p.customer_id
WHERE p.is_skip_month_action = TRUE
QUALIFY rnk_skip = 1;

CREATE OR REPLACE TEMP TABLE _customer_action_period__orders AS
SELECT o.customer_id,
       DATE_TRUNC('month', o.order_local_datetime)::DATE AS redemption_month_date,
       SUM(o.membership_credits_redeemed_count)          AS membership_credits_redeemed_count,
       SUM(o.membership_credits_redeemed_amount)         AS membership_credits_redeemed_amount,
       SUM(o.membership_token_redeemed_count)            AS membership_token_redeemed_count,
       SUM(o.membership_token_redeemed_amount)           AS membership_token_redeemed_amount,
       SUM(o.membership_credits_redeemed_count) +
       SUM(o.membership_token_redeemed_count)            AS membership_token_credits_redeemed_count_total,
       SUM(o.membership_credits_redeemed_amount +
           o.membership_token_redeemed_amount)           AS membership_token_credits_redeemed_amount_total
FROM _customer_action_period_mobile_app__base base
         JOIN shared.sessions_order_stg o
              ON base.customer_id = o.customer_id
GROUP BY o.customer_id,
         DATE_TRUNC('month', o.order_local_datetime)::DATE;

CREATE OR REPLACE TEMP TABLE _customer_action_period_mobile_app_stg AS
SELECT clvm.customer_id,
       st.store_brand                                                                AS brand,
       st.store_region                                                               AS region,
       st.store_country                                                              AS country,
       clvm.month_date                                                               AS date,
       clvm.is_bop_vip,
       dc.mobile_app_cohort_month_date                                               AS app_login_month_date,
       dc.mobile_app_cohort_os,
       IFF(clvm.month_date >= dc.mobile_app_cohort_month_date, 1, 0)                 AS is_app_customer,
       IFF(clvm.month_date >= DATE_TRUNC('month', dc.first_mobile_app_order_local_datetime)::DATE, 1,
           0)                                                                        AS is_converted_app_customer,
       dc.gender                                                                     AS customer_gender,
       clvm.vip_cohort_month_date                                                    AS vip_cohort,
       DATEDIFF(MM, clvm.vip_cohort_month_date, clvm.month_date) + 1                 AS vip_tenure,
       CASE
           WHEN DATEDIFF(MM, clvm.vip_cohort_month_date, clvm.month_date) + 1 BETWEEN 13 AND 24 THEN 'M13-24'
           WHEN DATEDIFF(MM, clvm.vip_cohort_month_date, clvm.month_date) + 1 >= 25 THEN 'M25+'
           ELSE CONCAT('M', DATEDIFF(MM, clvm.vip_cohort_month_date, clvm.month_date) +
                            1) END                                                   AS vip_tenure_group,
       clvm.membership_type,
       clvm.customer_action_category                                                 AS customer_action_category,
       CASE
           WHEN clvm.customer_action_category = 'Failed Billing' THEN 'Failed Credit Billing'
           WHEN clvm.customer_action_category = 'Merch Purchase and Failed Billing'
               THEN 'Failed Credit Billing + Merch Purchase'
           WHEN clvm.customer_action_category = 'Successful Billing' THEN 'Successful Credit Billing'
           WHEN clvm.customer_action_category = 'Merch Purchase and Successful Billing'
               THEN 'Successful Credit Billing + Merch Purchase'
           WHEN clvm.customer_action_category = 'Merch Purchaser' THEN 'Merch Purchase'
           WHEN clvm.customer_action_category = 'Skip Only' THEN 'Skipped Month'
           WHEN clvm.customer_action_category = 'Merch Purchase and Skip' THEN 'Skipped Month + Merch Purchase'
           WHEN clvm.customer_action_category = 'Other/No Action' THEN 'Other/No Action'
           WHEN clvm.customer_action_category = 'Cancel Only'
               THEN 'Cancel Only' END                                                AS customer_action_category_grouped,
       IFF(clvm.is_merch_purchaser = TRUE AND clvm.is_bop_vip = TRUE, 1, 0)          AS is_merch_purchase,
       IFF(cao.membership_token_credits_redeemed_count_total >= 1, 1, 0)             AS is_merch_credit_billing_redeemed,
       IFF(clvm.customer_action_category ILIKE '%Skip%', cas.session_platform, NULL) AS skip_platform,
       IFF(cas.operating_system ILIKE 'Mac%%' OR cas.operating_system = 'iOS', 'Apple',
           'Other')                                                                  AS skip_operating_system,
       IFF(clvm.customer_action_category = 'Skip Only', clvm.customer_id, NULL)      AS is_skip,
       CASE
           WHEN clvm.customer_action_category = 'Merch Purchase and Skip'
               THEN clvm.customer_id END                                             AS is_skip_purchase,
       CASE
           WHEN clvm.customer_action_category = 'Successful Billing'
               THEN clvm.customer_id END                                             AS is_successful_biling,
       CASE
           WHEN clvm.customer_action_category ILIKE 'Merch Purchase and Successful Billing'
               THEN clvm.customer_id END                                             AS is_successful_billing_purchase,
       CASE
           WHEN clvm.customer_action_category ILIKE '%Failed Billing%'
               THEN clvm.customer_id END                                             AS is_failed_billing,
       CASE
           WHEN clvm.customer_action_category ILIKE 'Billed Credit - Failed /%'
               THEN clvm.customer_id END                                             AS is_failed_billing_purchase,
       CASE
           WHEN clvm.customer_action_category = 'Merch Purchaser'
               THEN clvm.customer_id END                                             AS is_merch_purch_only,
       CASE WHEN clvm.is_cancel = TRUE THEN dc.customer_id END                       AS membership_cancel,
       CASE WHEN clvm.is_snooze = TRUE THEN dc.customer_id END                       AS membership_snooze,
       clvm.cash_net_revenue,
       clvm.cash_gross_revenue,
       clvm.cash_gross_profit,
       clvm.product_margin_pre_return,
       clvm.product_order_count,
       clvm.mobile_app_product_order_count,
       clvm.product_gross_revenue,
       clvm.mobile_app_product_gross_revenue,
       IFF(is_successful_billing = TRUE, 1, 0)                                       AS memcredit_charged,
       cao.membership_token_credits_redeemed_count_total,
       cao.membership_credits_redeemed_count,
       cao.membership_token_credits_redeemed_amount_total,
       cao.membership_credits_redeemed_amount,
       cao.membership_token_redeemed_count,
       cao.membership_token_redeemed_amount
FROM _customer_action_period_mobile_app__base base
         JOIN edw_prod.analytics_base.customer_lifetime_value_monthly AS clvm
              ON base.customer_id = clvm.customer_id
         JOIN edw_prod.data_model.dim_store AS st ON st.store_id = clvm.store_id
         LEFT JOIN edw_prod.data_model.dim_customer AS dc
                   ON dc.customer_id = clvm.customer_id
         LEFT JOIN _customer_action_period__skips cas
                   ON clvm.customer_id = cas.customer_id
                       AND clvm.month_date = cas.session_month_date
                       AND clvm.customer_action_category ILIKE '%Skip%'
         LEFT JOIN _customer_action_period__orders cao
                   ON cao.customer_id = clvm.customer_id AND cao.redemption_month_date = clvm.month_date
WHERE st.store_brand IN ('JustFab', 'Fabletics')
  AND st.store_country IN ('DE', 'ES', 'FR', 'UK', 'US')
  AND clvm.month_date >= '2019-01-01'
  AND clvm.is_bop_vip = TRUE;

DELETE
FROM shared.customer_action_period_mobile_app t
    USING _customer_action_period_mobile_app__base s
WHERE s.customer_id = t.customer_id;


INSERT INTO shared.customer_action_period_mobile_app (
    customer_id,
    brand,
    region,
    country,
    date,
    is_bop_vip,
    apploginmonthdate,
    mobile_app_cohort_os,
    isappcustomer,
    isconvertedappcustomer,
    customergender,
    vipcohort,
    viptenure,
    viptenuregroup,
    membership_type,
    customeractioncategory,
    customeractioncategorygrouped,
    ismerchpurchase,
    ismerchcreditbillingredeemed,
    skipplatform,
    skipoperatingsystem,
    skipcounts,
    skippurchasecounts,
    successfulbillingcounts,
    successfulbillingpurchasecounts,
    failedbillingcounts,
    failedbillingpurchasecounts,
    merchpurchonlycounts,
    membershipcancels,
    membershipsnoozes,
    customers,
    cashnetrevenue,
    cashgrossrevenue,
    cashgrossmargin,
    productgrossmargin,
    productordercount,
    productordermobileappcount,
    productgrossrevenue,
    productgrossrevenuemobileappcount,
    memcreditchargedcount,
    memcreditredeemedcount,
    memcreditredeemedamount,
    memcreditbillingredeemedcount,
    memcreditbillingredeemedamount,
    memtokenredeemedcount,
    memtokenredeemedamount,
    lastrefreshedtime,
    meta_create_datetime,
    meta_update_datetime
)
SELECT customer_id,
       brand,
       region,
       country,
       date,
       is_bop_vip,
       app_login_month_date,
       mobile_app_cohort_os,
       is_app_customer,
       is_converted_app_customer,
       customer_gender,
       vip_cohort,
       vip_tenure,
       vip_tenure_group,
       membership_type,
       customer_action_category,
       customer_action_category_grouped,
       is_merch_purchase,
       is_merch_credit_billing_redeemed,
       skip_platform,
       skip_operating_system,
       COUNT(is_skip)                                      AS skip_count,
       COUNT(is_skip_purchase)                             AS skip_purchase_count,
       COUNT(is_successful_biling)                         AS successful_billing_count,
       COUNT(is_successful_billing_purchase)               AS successful_billing_purchase_count,
       COUNT(is_failed_billing)                            AS failed_billing_count,
       COUNT(is_failed_billing_purchase)                   AS failed_billing_purchase_count,
       COUNT(is_merch_purch_only)                          AS merch_purch_onlycount,
       COUNT(membership_cancel)                            AS membership_cancels,
       COUNT(membership_snooze)                            AS membership_snoozes,
       COUNT(*)                                            AS customers,
       SUM(cash_net_revenue)                               AS cash_net_revenue,
       SUM(cash_gross_revenue)                             AS cash_gross_revenue,
       SUM(cash_gross_profit)                              AS cash_gross_margin,
       SUM(product_margin_pre_return)                      AS product_gross_margin,
       SUM(product_order_count)                            AS product_order_count,
       SUM(mobile_app_product_order_count)                 AS product_order_mobile_appcount,
       SUM(product_gross_revenue)                          AS product_gross_revenue,
       SUM(mobile_app_product_gross_revenue)               AS product_gross_revenue_mobile_app_count,
       SUM(memcredit_charged)                              AS mem_credit_charged_count,
       SUM(membership_token_credits_redeemed_count_total)  AS mem_credit_redeemed_count,
       SUM(membership_token_credits_redeemed_amount_total) AS mem_credit_redeemed_amount,
       SUM(membership_credits_redeemed_count)              AS mem_credit_billing_redeemed_count,
       SUM(membership_credits_redeemed_amount)             AS mem_credit_billing_redeemed_amount,
       SUM(membership_token_redeemed_count)                AS mem_token_redeemed_count,
       SUM(membership_token_redeemed_amount)               AS mem_token_redeemed_amount,
       $last_refreshed_time                                AS last_refreshed_time,
       $execution_start_time                               AS meta_create_datetime,
       $execution_start_time                               AS meta_update_datetime
FROM _customer_action_period_mobile_app_stg
GROUP BY customer_id,
         brand,
         region,
         country,
         date,
         is_bop_vip,
         app_login_month_date,
         mobile_app_cohort_os,
         is_app_customer,
         is_converted_app_customer,
         customer_gender,
         vip_cohort,
         vip_tenure,
         vip_tenure_group,
         membership_type,
         customer_action_category,
         customer_action_category_grouped,
         is_merch_purchase,
         is_merch_credit_billing_redeemed,
         skip_platform,
         skip_operating_system;

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3)
WHERE table_name = $target_table;
