INSERT INTO breakage.original_credit_activity_snapshot
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                   AS brand,
       dcf2.credit_id                                                AS original_credit_id,
       dcf.customer_id                                               AS customer_id,
       st.store_country                                              AS country,
       st.store_region                                               AS region,
       dcf.credit_state                                              AS state,
       fce.credit_activity_type                                      AS activity_type,
       dcf2.credit_tender                                            AS original_credit_tender,
       dcf2.credit_type                                              AS original_credit_type,
       dcf2.credit_reason                                            AS original_credit_reason,
       DATE_TRUNC('month', dcf2.credit_issued_local_datetime::DATE)  AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE) AS activity_month,
       dcf2.deferred_recognition_label_credit                        AS deferred_recognition_label,
       SUM(fce.credit_activity_gross_vat_local_amount)               AS activity_amount_local_gross_vat,
       SUM(fce.credit_activity_gross_vat_local_amount *
           dcf2.credit_issued_usd_conversion_rate)                   AS activity_amount_usd_gross_vat,
       SUM(ROUND(fce.credit_activity_local_amount, 2))               AS activity_amount_local_net_vat,
       SUM(ROUND(fce.credit_activity_local_amount, 2) *
           dcf2.credit_issued_usd_conversion_rate)                   AS activity_amount_usd_net_vat,
       CURRENT_TIMESTAMP()                                           AS update_datetime,
       iff(brand = 'JustFab'
               and finance_specialty_store='CA', 'CA','None')        AS finance_specialty_store,
       dcf.giftco_transfer_status
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf2.customer_id
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf2.store_id
WHERE fce.credit_activity_type IN ('Issued', 'Redeemed', 'Cancelled')
  AND fce.original_credit_activity_type_action = 'Include'
GROUP BY CASE
             WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
             WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
             ELSE st.store_brand END,
         dcf2.credit_id,
         dcf.customer_id,
         st.store_country,
         st.store_region,
         dcf.credit_state,
         fce.credit_activity_type,
         dcf2.credit_tender,
         dcf2.credit_type,
         dcf2.credit_reason,
         DATE_TRUNC('month', dcf2.credit_issued_local_datetime::DATE),
         DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE),
         dcf2.deferred_recognition_label_credit,
         iff(brand = 'JustFab'
               and finance_specialty_store='CA', 'CA','None'),
         dcf.giftco_transfer_status;
