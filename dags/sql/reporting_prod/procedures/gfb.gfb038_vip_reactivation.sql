CREATE OR REPLACE TEMPORARY TABLE _vip_activation_cancellation AS
SELECT v.customer_id,
       v.business_unit,
       v.region,
       v.country,
       v.first_activating_cohort,
       CAST(fa.activation_local_datetime AS DATE) AS activation_date,
       CAST(fa.cancellation_local_datetime AS DATE) AS cancellation_date,
       IFF(fa.next_activation_local_datetime::DATE='9999-12-31', NULL, DATE_TRUNC('month',fa.next_activation_local_datetime::DATE)) AS next_activation_month,
       DATE_TRUNC('month', activation_date)                                                   AS activation_month,
       DATE_TRUNC('month', cancellation_date)                                                 AS cancellation_month,
       v.current_membership_status,
       CASE WHEN dc.default_state_province IN ('CA', 'CAL') THEN 1 ELSE 0 END               AS is_california_customer,
       IFF(cancellation_date='9999-12-31', NULL, (DATEDIFF('month', activation_month, cancellation_month) + 1)) AS month_between_activation_cancellation,
       RANK() OVER(PARTITION BY fa.customer_id ORDER BY activation_date ASC) AS activation_rank,
       IFF(cancellation_date='9999-12-31', NULL,activation_rank) AS cancellation_rank,
       v.cash_gross_margin_decile_ltd               AS cash_gross_margin_decile,
       v.gaap_gross_margin_decile_ltd               AS gaap_gross_margin_decile
FROM edw_prod.data_model_jfb.fact_activation fa
     JOIN edw_prod.data_model_jfb.dim_customer dc
          ON fa.customer_id = dc.customer_id
     JOIN gfb.gfb_dim_vip v
          ON v.customer_id = fa.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _first_cancel_reactivation AS
SELECT customer_id,
       DATEDIFF(MONTH, cancellation_month, next_activation_month) + 1 AS month_between_cancellation_reactivation
FROM _vip_activation_cancellation
WHERE cancellation_rank=1 AND next_activation_month IS NOT NULL;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb038_vip_reactivation AS
SELECT main.business_unit,
       main.region,
       main.country,
       main.customer_id,
       main.first_activating_cohort,
       main.cash_gross_margin_decile,
       main.gaap_gross_margin_decile,
       main.current_membership_status,
       main.is_california_customer,
       main.month_between_activation_cancellation,
       main.tenure_first_cancel,
       main.cash_gross_margin_decile_first_cancel,
       main.gaap_gross_margin_decile_first_cancel,
       main.cash_gross_margin_decile_current,
       main.gaap_gross_margin_decile_current,
       main.total_activations,
       fcr.month_between_cancellation_reactivation
FROM (SELECT vac.business_unit,
             vac.region,
             vac.country,
             vac.customer_id,
             vac.first_activating_cohort,
             vac.cash_gross_margin_decile,
             vac.gaap_gross_margin_decile,
             vac.current_membership_status,
             vac.is_california_customer,
             AVG(vac.month_between_activation_cancellation) AS month_between_activation_cancellation,
             SUM(CASE
                     WHEN vac.cancellation_rank = 1 THEN vac.month_between_activation_cancellation
                     ELSE 0 END)                            AS tenure_first_cancel,
             SUM(CASE
                     WHEN vac.cancellation_rank = 1 THEN clvm.cumulative_cash_gross_profit_decile
                     ELSE 0 END)                            AS cash_gross_margin_decile_first_cancel,
             SUM(CASE
                     WHEN vac.cancellation_rank = 1 THEN clvm.cumulative_product_gross_profit_decile
                     ELSE 0 END)                            AS gaap_gross_margin_decile_first_cancel,
             SUM(CASE
                     WHEN vac.cancellation_rank = 1 THEN vac.cash_gross_margin_decile
                     ELSE 0 END)                            AS cash_gross_margin_decile_current,
             SUM(CASE
                     WHEN vac.cancellation_rank = 1 THEN vac.gaap_gross_margin_decile
                     ELSE 0 END)                            AS gaap_gross_margin_decile_current,
             MAX(vac.activation_rank)                       AS total_activations
      FROM _vip_activation_cancellation vac
           LEFT JOIN edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvm
                    ON clvm.meta_original_customer_id = vac.customer_id
                    AND clvm.month_date = vac.cancellation_month
      GROUP BY vac.business_unit,
               vac.region,
               vac.country,
               vac.customer_id,
               vac.first_activating_cohort,
               vac.cash_gross_margin_decile,
               vac.gaap_gross_margin_decile,
               vac.current_membership_status,
               vac.is_california_customer
      HAVING total_activations > 1) main
     JOIN _first_cancel_reactivation fcr
          ON fcr.customer_id = main.customer_id;
