CREATE OR REPLACE TEMP TABLE _bounceback_endowment_giftcards AS
SELECT CASE
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                          AS brand,
       st.store_country                                                     AS country,
       st.store_region,
       dcf.credit_id,
       dcf.credit_order_id,
       IFF(fce.credit_activity_type = 'Redeemed',
           fce.redemption_order_id, NULL)                                   AS redemption_order_id,
       fce.credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
       DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
       ROUND(IFF(fce.credit_activity_type = 'Issued',
                 fce.credit_activity_local_amount_issuance_vat,
                 -1 * fce.credit_activity_local_amount_issuance_vat), 2)    AS activity_amount,
       IFF(fce.credit_activity_type = 'Redeemed',
           o.product_gross_revenue_local_amount, NULL)                      AS redemption_order_amount
FROM reporting_base_prod.shared.dim_credit dcf
         JOIN reporting_base_prod.shared.fact_credit_event fce ON dcf.credit_key = fce.credit_key
         JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
         JOIN edw_prod.data_model.dim_store st ON dcf.store_id = st.store_id
         LEFT JOIN edw_prod.data_model.fact_order o ON fce.redemption_order_id = o.order_id
WHERE original_credit_reason = 'Bounceback Endowment'
  AND fce.original_credit_activity_type_action = 'Include';

CREATE OR REPLACE TEMP TABLE _original_token_info AS
SELECT be.membership_token_id,
       beg.credit_id,
       dcf.credit_issued_local_gross_vat_amount AS membership_token_amount,
       beg.activity_amount                      AS giftcard_value
FROM _bounceback_endowment_giftcards beg
JOIN lake_consolidated_view.ultra_merchant.bounceback_endowment be ON beg.credit_id = be.object_id
JOIN reporting_base_prod.shared.dim_credit dcf ON be.membership_token_id = dcf.credit_id
    AND credit_type = 'Token'
WHERE beg.credit_activity_type = 'Issued';

CREATE OR REPLACE TEMP TABLE _bounceback_type AS
SELECT
        beg.credit_id,
       bt.label AS bounceback_type
FROM _bounceback_endowment_giftcards beg
JOIN lake_consolidated_view.ultra_merchant.bounceback_endowment be ON beg.credit_id = be.object_id
JOIN lake_consolidated_view.ultra_merchant.bounceback_type bt ON bt.bounceback_type_id = be.bounceback_type_id
WHERE beg.credit_activity_type = 'Issued';

CREATE OR REPLACE TEMPORARY TABLE _giftcard_value AS
SELECT beg.credit_id,
    beg.activity_amount AS giftcard_value
FROM _bounceback_endowment_giftcards beg
WHERE beg.credit_activity_type = 'Issued';

CREATE OR REPLACE TABLE reporting_base_prod.shared.bounceback_endowment_detail AS
SELECT brand || ' ' || country                  AS store,
       store_region,
       beg.credit_id,
       credit_order_id,
       redemption_order_id,
       credit_activity_type,
       original_credit_tender,
       original_credit_type,
       original_credit_reason,
       bt.bounceback_type,
       original_issued_month,
       activity_month,
       activity_amount,
       redemption_order_amount,
       membership_token_id,
       membership_token_amount,
       COALESCE(oti.giftcard_value, gv.giftcard_value) AS giftcard_value
FROM _bounceback_endowment_giftcards beg
LEFT JOIN _original_token_info oti ON beg.credit_id = oti.credit_id
LEFT JOIN _bounceback_type bt ON beg.credit_id = bt.credit_id
LEFT JOIN _giftcard_value gv ON gv.credit_id = beg.credit_id
;
