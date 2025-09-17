CREATE OR REPLACE VIEW reporting_base_prod.shared.unredeemed_vip_cohort
        (
         vip_cohort_month_date,
         membership_type,
         store_brand,
         month_date,
         tenure,
         membership_monthly_status,
         membership_sequence,
         store_region,
         store_country,
         store_name,
         customer_id,
         monthly_billed_credit_cash_transaction_amount,
         billed_cash_credit_issued_amount,
         billed_cash_credit_redeemed_amount,
         billed_cash_credit_cancelled_amount,
         billed_cash_credit_redeemed_cancelled_amount,
         unredeemed_amount
            )
AS
SELECT clvm.vip_cohort_month_date,
       clvm.membership_type,
       CASE
           WHEN store_brand = 'Fabletics' AND clvm.gender = 'M' THEN 'Fabletics Mens'
           WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
           WHEN store_brand = 'Yitty' THEN 'Yitty'
           ELSE store_brand END                                                            AS store_brand,
       clvm.month_date,
       'M' ||
       CAST(DATEDIFF('month', clvm.vip_cohort_month_date, clvm.month_date) + 1 AS VARCHAR) AS tenure,
       CASE
           WHEN clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = FALSE THEN 'Activation'
           WHEN clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = TRUE THEN 'Reactivation'
           WHEN clvm.is_bop_vip = TRUE THEN 'BOP'
           WHEN clvm.is_cancel = TRUE THEN 'Cancelled'
           END                                                                             AS membership_monthly_status,
       CASE
           WHEN clvm.is_reactivated_vip = TRUE THEN 'Reactivated Membership'
           WHEN clvm.is_reactivated_vip = FALSE
               THEN 'First Membership' END                                                 AS membership_sequence,
       ds.store_region,
       ds.store_country,
       ds.store_name,
       clvm.customer_id,
       IFNULL(clvm.monthly_billed_credit_cash_transaction_amount, 0)                       AS monthly_billed_credit_cash_transaction_amount,
       IFNULL(clvm.billed_cash_credit_issued_amount, 0)                                    AS billed_cash_credit_issued_amount,
       IFNULL(clvm.billed_cash_credit_redeemed_amount, 0)                                  AS billed_cash_credit_redeemed_amount,
       IFNULL(clvm.billed_cash_credit_cancelled_amount, 0)                                 AS billed_cash_credit_cancelled_amount,
       IFNULL(clvm.billed_cash_credit_redeemed_amount, 0) +
       IFNULL(clvm.billed_cash_credit_cancelled_amount, 0)                                 AS billed_cash_credit_redeemed_cancelled_amount,
       IFNULL(clvm.monthly_billed_credit_cash_transaction_amount, 0) -
       billed_cash_credit_redeemed_cancelled_amount                                        AS unredeemed_amount
FROM edw_prod.analytics_base.customer_lifetime_value_monthly AS clvm
         JOIN edw_prod.data_model.dim_store AS ds
              ON clvm.store_id = ds.store_id
WHERE monthly_billed_credit_cash_transaction_amount <> 0
   OR billed_cash_credit_issued_amount <> 0
   OR billed_cash_credit_redeemed_amount <> 0
   OR billed_cash_credit_redeemed_cancelled_amount <> 0
   OR billed_cash_credit_cancelled_amount <> 0
   OR unredeemed_amount <> 0;
