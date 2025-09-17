CREATE OR REPLACE TRANSIENT TABLE shared.emp_token_outstanding
    (
     brand VARCHAR(155),
     store_region VARCHAR(55),
     store_country VARCHAR(55),
     current_membership_state VARCHAR(55),
     membership_token_id INT,
     token_reason_label VARCHAR(55),
     credit_tender VARCHAR(55),
     statuscode INT,
     is_converted_credit BOOLEAN,
     original_issued_month DATE,
     optin_month DATE,
     credit_start_date DATE,
     credit_expire_date DATE,
     credit_expiration_month INT,
     outstanding_amount DECIMAL(20, 4) NULL,
     datetime_updated DATETIME
        ) AS
SELECT CASE
           WHEN ds.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN ds.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE ds.store_brand END AS                                                 brand,
       ds.store_region,
       ds.store_country,
       IFF(fa.customer_id IS NOT NULL, 'VIP', 'Cancelled')                            current_membership_state,
       mt.membership_token_id,
       mt.token_reason_label,
       mt.credit_tender,
       mt.statuscode,
       IFF(scc.converted_membership_token_id IS NULL, FALSE, TRUE)                    is_converted_credit,
       DATE_TRUNC(MONTH, TO_DATE(COALESCE(sc.datetime_added, mt.datetime_added)))     original_issued_month,
       DATE_TRUNC(MONTH, TO_DATE(COALESCE(mt.adjusted_datetime_optin, '9999-12-31'))) optin_month,
       IFF(mt.datetime_added::DATE <= mt.adjusted_datetime_optin,-- mt.datetime_optin,
           mt.adjusted_datetime_optin, --mt.datetime_optin,
           mt.datetime_added::DATE)                                                   credit_start_date,
       COALESCE(mt.date_expires, '9999-12-31')                                        credit_expire_date,
       IFF(credit_expire_date = '9999-12-31', 9999,
           DATEDIFF(MONTH, credit_start_date, credit_expire_date))                    credit_expiration_month,
       SUM(COALESCE(sc.amount, mt.purchase_price))                                    outstanding_amount,
       CURRENT_TIMESTAMP()                                                            datetime_updated

FROM shared.emp_token mt
         JOIN edw_prod.stg.dim_customer dc ON mt.customer_id = dc.customer_id
         JOIN edw_prod.data_model.dim_store ds ON mt.store_id = ds.store_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.store_credit_token_conversion scc
                   ON scc.converted_membership_token_id = mt.membership_token_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.store_credit sc
                   ON scc.original_store_credit_id = sc.store_credit_id
         LEFT JOIN edw_prod.data_model.fact_activation fa ON fa.customer_id = mt.customer_id
    AND fa.activation_local_datetime::DATE < CURRENT_DATE()
    AND cancellation_local_datetime::DATE >= CURRENT_DATE()
WHERE mt.datetime_added::DATE < DATE_TRUNC(MONTH, CURRENT_DATE())
GROUP BY brand,
         ds.store_region,
         ds.store_country,
         current_membership_state,
         mt.membership_token_id,
         mt.token_reason_label,
         mt.credit_tender,
         mt.statuscode,
         is_converted_credit,
         original_issued_month,
         optin_month,
         credit_start_date,
         credit_expire_date,
         credit_expiration_month;

INSERT INTO shared.emp_token_outstanding_snapshot
SELECT *
FROM shared.emp_token_outstanding;

DELETE
FROM shared.emp_token_outstanding_snapshot
WHERE datetime_updated < DATEADD(MONTH, -12, getdate());
