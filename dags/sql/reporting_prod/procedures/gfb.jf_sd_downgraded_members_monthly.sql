CREATE OR REPLACE TEMPORARY TABLE _month_scaffold AS
SELECT DISTINCT DATE_TRUNC('month', full_date) AS activity_month
FROM edw_prod.data_model_jfb.dim_date
WHERE full_date >= '2022-01-01'
  AND full_date <= CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _jf_members_pre_scaffold AS
SELECT DISTINCT dc.email,
                dc.customer_id,
                MAX(IFF(fa.activation_local_datetime IS NULL, '9999-12-31',
                        fa.activation_local_datetime))   AS activation_date,
                MAX(IFF(fa.cancellation_local_datetime IS NULL, '9999-12-31',
                        fa.cancellation_local_datetime)) AS cancellation_date
FROM edw_prod.data_model_jfb.fact_activation fa
     JOIN edw_prod.data_model_jfb.dim_customer dc
          ON fa.customer_id = dc.customer_id
     JOIN edw_prod.data_model_jfb.dim_store ds
          ON fa.store_id = ds.store_id
              AND store_brand_abbr = 'JF'
GROUP BY dc.email,
         dc.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _jf_members AS
SELECT DISTINCT m.activity_month,
                ps.email,
                ps.customer_id,
                ps.activation_date,
                ps.cancellation_date,
                DATEDIFF('month', ps.cancellation_date, m.activity_month) AS cancel_tenure
FROM _jf_members_pre_scaffold ps
     JOIN _month_scaffold m
          ON m.activity_month >= DATE_TRUNC('month', activation_date)
              AND m.activity_month < DATE_TRUNC('month', cancellation_date);

CREATE OR REPLACE TEMPORARY TABLE _sd_members_pre_scaffold AS
SELECT DISTINCT dc.email,
                dc.customer_id,
                MAX(IFF(fa.activation_local_datetime IS NULL, '9999-12-31',
                        fa.activation_local_datetime))   AS activation_date,
                MAX(IFF(fa.cancellation_local_datetime IS NULL, '9999-12-31',
                        fa.cancellation_local_datetime)) AS cancellation_date
FROM edw_prod.data_model_jfb.fact_activation fa
     JOIN edw_prod.data_model_jfb.dim_customer dc
          ON fa.customer_id = dc.customer_id
     JOIN edw_prod.data_model_jfb.dim_store ds
          ON fa.store_id = ds.store_id
              AND store_brand_abbr = 'SD'
GROUP BY dc.email,
         dc.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _sd_members AS
SELECT DISTINCT m.activity_month,
                ps.email,
                ps.customer_id,
                ps.activation_date,
                ps.cancellation_date,
                DATEDIFF('month', ps.cancellation_date, m.activity_month) AS cancel_tenure
FROM _sd_members_pre_scaffold ps
     JOIN _month_scaffold m
          ON m.activity_month >= DATE_TRUNC('month', activation_date)
              AND m.activity_month < DATE_TRUNC('month', cancellation_date);

CREATE OR REPLACE TEMPORARY TABLE _credits_redeemed_jf AS
SELECT jf.customer_id,
       jf.activity_month,
       COUNT(IFF(credit_activity_type = 'Redeemed' AND
                 activity_month = DATE_TRUNC('month', credit_activity_local_datetime::DATE), credit_id,
                 0)) AS credits_redeemed
FROM edw_prod.data_model_jfb.fact_credit_event fce
     JOIN _jf_members jf
          ON jf.customer_id = fce.customer_id
              AND jf.activity_month = DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)
GROUP BY jf.customer_id,
         jf.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _credits_redeemed_sd AS
SELECT sd.customer_id,
       sd.activity_month,
       COUNT(IFF(credit_activity_type = 'Redeemed' AND
                 activity_month = DATE_TRUNC('month', credit_activity_local_datetime::DATE), credit_id,
                 0)) AS credits_redeemed
FROM edw_prod.data_model_jfb.fact_credit_event fce
     JOIN _sd_members sd
          ON sd.customer_id = fce.customer_id
              AND sd.activity_month = DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)
GROUP BY sd.customer_id,
         sd.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _jf_fso AS
SELECT jf.activity_month,
       jf.email,
       jf.customer_id,
       jf.activation_date,
       jf.cancellation_date,
       jf.cancel_tenure,
       SUM(fso.product_order_count)                                                 AS product_orders,
       SUM(fso.product_order_unit_count)                                            AS product_order_units,
       SUM(fso.product_gross_revenue)                                               AS product_gross_revenue,
       SUM(fso.product_net_revenue)                                                 AS product_net_revenue,
       SUM(fso.billed_cash_credit_issued_amount)                                    AS billing_revenue,
       SUM(fso.billed_cash_credit_issued_equivalent_count)                          AS billings_count,
       SUM(fso.billed_cash_credit_redeemed_equivalent_count)                        AS credits_redeemed,
       SUM(fso.billed_cash_credit_redeemed_amount)                                  AS credit_redeemed_amount,
       COUNT(CASE WHEN fso.cancellation_local_datetime::DATE = fso.date THEN 1 END) AS membership_cancels
FROM _jf_members jf
     LEFT JOIN edw_prod.analytics_base.finance_sales_ops fso
               ON jf.customer_id = edw_prod.stg.udf_unconcat_brand(fso.customer_id)
                   AND DATE_TRUNC('month', fso.date) = jf.activity_month
                   AND fso.date_object = 'placed'
                   AND fso.currency_object = 'usd'
     LEFT JOIN _credits_redeemed_jf cr
               ON jf.customer_id = cr.customer_id
                   AND cr.activity_month = jf.activity_month
GROUP BY jf.activity_month,
         jf.email,
         jf.customer_id,
         jf.activation_date,
         jf.cancellation_date,
         jf.cancel_tenure;

CREATE OR REPLACE TEMPORARY TABLE _sd_fso AS
SELECT sd.activity_month,
       sd.email,
       sd.customer_id,
       sd.activation_date,
       sd.cancellation_date,
       sd.cancel_tenure,
       SUM(fso.product_order_count)                                                 AS product_orders,
       SUM(fso.product_order_unit_count)                                            AS product_order_units,
       SUM(fso.product_gross_revenue)                                               AS product_gross_revenue,
       SUM(fso.product_net_revenue)                                                 AS product_net_revenue,
       SUM(fso.billed_cash_credit_issued_amount)                                    AS billing_revenue,
       SUM(fso.billed_cash_credit_issued_equivalent_count)                          AS billings_count,
       SUM(fso.billed_cash_credit_redeemed_equivalent_count)                        AS credits_redeemed,
       SUM(fso.billed_cash_credit_redeemed_amount)                                  AS credit_redeemed_amount,
       COUNT(CASE WHEN fso.cancellation_local_datetime::DATE = fso.date THEN 1 END) AS membership_cancels
FROM _sd_members sd
     LEFT JOIN edw_prod.analytics_base.finance_sales_ops fso
               ON sd.customer_id = edw_prod.stg.udf_unconcat_brand(fso.customer_id)
                   AND DATE_TRUNC('month', fso.date) = sd.activity_month
                   AND fso.date_object = 'placed'
                   AND fso.currency_object = 'usd'
     LEFT JOIN _credits_redeemed_sd cr
               ON sd.customer_id = cr.customer_id
                   AND cr.activity_month = sd.activity_month
GROUP BY sd.activity_month,
         sd.email,
         sd.customer_id,
         sd.activation_date,
         sd.cancellation_date,
         sd.cancel_tenure;

CREATE OR REPLACE TEMPORARY TABLE _crossover_customer_ids AS
SELECT DISTINCT jf.email,
                jf.customer_id AS jf_customer_id,
                sd.customer_id AS sd_customer_id
FROM _jf_members jf
     JOIN _sd_members sd
          ON TRIM(LOWER(jf.email)) = TRIM(LOWER(sd.email));

CREATE OR REPLACE TEMPORARY TABLE _dual_members AS
SELECT DISTINCT COALESCE(jf.activity_month, sd.activity_month) AS activity_month,
                COALESCE(jf.email, sd.email)                   AS email
FROM _crossover_customer_ids cc
     JOIN _jf_members jf
          ON cc.jf_customer_id = jf.customer_id
     JOIN _sd_members sd
          ON cc.sd_customer_id = sd.customer_id
              AND jf.activity_month = sd.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _last_dual_month AS
SELECT email,
       MAX(activity_month) AS max_month
FROM _dual_members
GROUP BY email;

CREATE OR REPLACE TEMPORARY TABLE _customer_activity_final_stg AS
SELECT DISTINCT CASE
                    WHEN dm.email IS NOT NULL THEN 'Dual Membership'
                    WHEN jf.activity_month > COALESCE(ldm.max_month, '9999-12-31') AND
                         COALESCE(ldm.max_month, '9999-12-31') != '2023-10-01' THEN 'Down from 2 to 1'
                    END                                                      AS activity_flag,
                COALESCE(ldm.max_month, DATE_TRUNC('month', CURRENT_DATE())) AS max_month,
                jf.*
FROM _jf_fso jf
     JOIN _crossover_customer_ids cc
          ON jf.email = cc.email
     LEFT JOIN _dual_members dm
               ON jf.activity_month = dm.activity_month
                   AND jf.email = dm.email
     LEFT JOIN _last_dual_month ldm
               ON jf.email = ldm.email
UNION ALL
SELECT DISTINCT CASE
                    WHEN dm.email IS NOT NULL THEN 'Dual Membership'
                    WHEN sd.activity_month > COALESCE(ldm.max_month, '9999-12-31') AND
                         COALESCE(ldm.max_month, '9999-12-31') != '2023-10-01' THEN 'Down from 2 to 1'
                    END                                                      AS activity_flag,
                COALESCE(ldm.max_month, DATE_TRUNC('month', CURRENT_DATE())) AS max_month,
                sd.*
FROM _sd_fso sd
     JOIN _crossover_customer_ids cc
          ON sd.email = cc.email
     LEFT JOIN _dual_members dm
               ON sd.activity_month = dm.activity_month
                   AND sd.email = dm.email
     LEFT JOIN _last_dual_month ldm
               ON cc.email = ldm.email;

CREATE OR REPLACE TEMPORARY TABLE _customer_activity_final AS
SELECT *
FROM _customer_activity_final_stg
WHERE max_month != DATE_TRUNC('month', CURRENT_DATE);

CREATE OR REPLACE TEMPORARY TABLE _cancel_tenure_update AS
SELECT activity_month,
       activity_flag,
       email,
       max_month,
       DATEDIFF('month', max_month, activity_month) AS cancel_tenure,
       SUM(product_gross_revenue)                   AS product_gross_revenue,
       SUM(product_orders)                          AS product_orders,
       SUM(product_order_units)                     AS product_order_units,
       SUM(product_net_revenue)                     AS product_net_revenue,
       SUM(billing_revenue)                         AS billing_revenue,
       SUM(billings_count)                          AS billings_count,
       SUM(credit_redeemed_amount)                  AS credit_redeemed_amount,
       SUM(credits_redeemed)                        AS credits_redeemed,
       SUM(membership_cancels)                      AS membership_cancels
FROM _customer_activity_final
GROUP BY activity_month,
         activity_flag,
         email,
         max_month;

CREATE OR REPLACE TRANSIENT TABLE gfb.jf_sd_downgraded_members_monthly AS
SELECT activity_month,
       max_month,
       cancel_tenure,
       COUNT(DISTINCT email)                                                            AS total_customers,
       COUNT(DISTINCT CASE WHEN activity_flag = 'Dual Membership' THEN email END)       AS dual_membership_customers,
       COUNT(DISTINCT CASE
                          WHEN activity_flag = 'Down from 2 to 1'
                              THEN email END)                                           AS down_from_two_to_one_customers,
       COUNT(DISTINCT CASE WHEN product_orders > 0 THEN email END)                      AS product_purchasers,
       COUNT(DISTINCT CASE
                          WHEN product_orders > 0
                              AND activity_flag = 'Dual Membership'
                              THEN email END)                                           AS dual_member_product_purchasers,
       COUNT(DISTINCT CASE
                          WHEN product_orders > 0
                              AND activity_flag = 'Down from 2 to 1'
                              THEN email END)                                           AS down_from_two_to_one_product_purchasers,
       DIV0(product_purchasers, total_customers)                                        AS merch_purchase_rate,
       DIV0(dual_member_product_purchasers, dual_membership_customers)                  AS dual_membership_merch_purchase_rate,
       DIV0(down_from_two_to_one_product_purchasers,
            down_from_two_to_one_customers)                                             AS down_from_two_to_one_merch_purchase_rate,
       SUM(product_orders)                                                              AS product_order_count,
       SUM(CASE WHEN activity_flag = 'Dual Membership' THEN product_orders ELSE 0 END)  AS dual_membership_product_order_count,
       SUM(CASE WHEN activity_flag = 'Down from 2 to 1' THEN product_orders ELSE 0 END) AS down_from_two_to_one_product_order_count,
       SUM(product_order_units)                                                         AS product_order_unit_count,
       SUM(CASE
               WHEN activity_flag = 'Dual Membership' THEN product_order_units
               ELSE 0 END)                                                              AS dual_membership_product_order_unit_count,
       SUM(CASE
               WHEN activity_flag = 'Down from 2 to 1' THEN product_order_units
               ELSE 0 END)                                                              AS down_from_two_to_one_product_order_unit_count,
       DIV0(product_order_unit_count, product_order_count)                              AS upt,
       DIV0(dual_membership_product_order_unit_count,
            dual_membership_product_order_count)                                        AS dual_membership_upt,
       DIV0(down_from_two_to_one_product_order_unit_count,
            down_from_two_to_one_product_order_count)                                   AS down_from_two_to_one_upt,
       DIV0(SUM(product_gross_revenue), product_order_count)                            AS aov,
       DIV0(SUM(CASE WHEN activity_flag = 'Dual Membership' THEN product_gross_revenue ELSE 0 END),
            dual_membership_product_order_count)                                        AS dual_membership_aov,
       DIV0(SUM(CASE WHEN activity_flag = 'Down from 2 to 1' THEN product_gross_revenue ELSE 0 END),
            down_from_two_to_one_product_order_count)                                   AS down_from_two_to_one_aov,
       SUM(product_net_revenue)                                                         AS product_net_revenue,
       SUM(CASE
               WHEN activity_flag = 'Dual Membership' THEN product_net_revenue
               ELSE 0 END)                                                              AS dual_membership_product_net_revenue,
       SUM(CASE
               WHEN activity_flag = 'Down from 2 to 1' THEN product_net_revenue
               ELSE 0 END)                                                              AS down_from_two_to_one_product_net_revenue,
       SUM(billing_revenue)                                                             AS billing_revenue,
       SUM(CASE
               WHEN activity_flag = 'Dual Membership' THEN billing_revenue
               ELSE 0 END)                                                              AS dual_membership_billing_revenue,
       SUM(CASE
               WHEN activity_flag = 'Down from 2 to 1' THEN billing_revenue
               ELSE 0 END)                                                              AS down_from_two_to_one_billing_revenue,
       SUM(billings_count)                                                              AS total_billings,
       SUM(CASE
               WHEN activity_flag = 'Dual Membership' THEN billings_count
               ELSE 0 END)                                                              AS dual_membership_billing_count,
       SUM(CASE
               WHEN activity_flag = 'Down from 2 to 1' THEN billings_count
               ELSE 0 END)                                                              AS down_from_two_to_one_billing_count,
       DIV0(total_billings, total_customers)                                            AS billing_rate,
       DIV0(dual_membership_billing_count, dual_membership_customers)                   AS dual_membership_billing_rate,
       DIV0(down_from_two_to_one_billing_count,
            down_from_two_to_one_customers)                                             AS down_from_two_to_one_billing_rate,
       SUM(credits_redeemed)                                                            AS _credits_redeemed,
       SUM(CASE
               WHEN activity_flag = 'Dual Membership' THEN credits_redeemed
               ELSE 0 END)                                                              AS dual_membership_credits_redeemed,
       SUM(CASE
               WHEN activity_flag = 'Down from 2 to 1' THEN credits_redeemed
               ELSE 0 END)                                                              AS down_from_two_to_one_credits_redeemed,
       COUNT(DISTINCT CASE WHEN credits_redeemed > 0 THEN email END)                    AS credits_redeemed_customer_count,
       COUNT(DISTINCT CASE
                          WHEN credits_redeemed > 0 AND activity_flag = 'Dual Membership'
                              THEN email END)                                           AS dual_membership_credits_redeemed_customer_count,
       COUNT(DISTINCT CASE
                          WHEN credits_redeemed > 0 AND activity_flag = 'Down from 2 to 1'
                              THEN email END)                                           AS down_from_two_to_one_credits_redeemed_customer_count,
       DIV0(_credits_redeemed, total_customers)                                         AS credit_redemption_rate,
       DIV0(dual_membership_credits_redeemed, dual_membership_customers)                AS dual_membership_credit_redemption_rate,
       DIV0(down_from_two_to_one_credits_redeemed,
            down_from_two_to_one_customers)                                             AS down_from_two_to_one_credit_redemption_rate,
       SUM(credit_redeemed_amount)                                                      AS credit_redeemed_amount,
       SUM(CASE
               WHEN activity_flag = 'Dual Membership' THEN credit_redeemed_amount
               ELSE 0 END)                                                              AS dual_membership_credit_redeemed_amount,
       SUM(CASE
               WHEN activity_flag = 'Down from 2 to 1' THEN credit_redeemed_amount
               ELSE 0 END)                                                              AS down_from_two_to_one_credit_redeemed_amount,
       SUM(membership_cancels)                                                          AS _membership_cancels,
       SUM(CASE
               WHEN activity_flag = 'Dual Membership' THEN membership_cancels
               ELSE 0 END)                                                              AS dual_membership_membership_cancels,
       SUM(CASE
               WHEN activity_flag = 'Down from 2 to 1' THEN membership_cancels
               ELSE 0 END)                                                              AS down_from_two_to_one_membership_cancels,
       DIV0(_membership_cancels, total_customers)                                       AS membership_cancel_rate,
       DIV0(dual_membership_membership_cancels, dual_membership_customers)              AS dual_membership_cancel_rate,
       DIV0(down_from_two_to_one_membership_cancels,
            down_from_two_to_one_customers)                                             AS down_from_two_to_one_cancel_rate
FROM _cancel_tenure_update
GROUP BY activity_month,
         max_month,
         cancel_tenure;
