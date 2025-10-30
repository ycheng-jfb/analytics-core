CREATE OR REPLACE TEMPORARY TABLE _giftco_transfer_key AS
SELECT dcf.original_credit_key
FROM lake_jfb_view.ultra_merchant.membership_token_transaction mtt
         JOIN edw_prod.new_stg.dim_credit dcf
              ON mtt.membership_token_id = dcf.credit_id
WHERE mtt.membership_token_transaction_type_id = 50
  AND credit_type = 'Token';

CREATE OR REPLACE TEMPORARY TABLE _giftco_transfer_status AS
SELECT dcf.credit_key,
       CASE
           WHEN gtk.original_credit_key IS NOT NULL
               THEN 'Token to Giftco'
           WHEN ((st.store_brand || ' ' || st.store_country = 'JustFab US' AND dc.finance_specialty_store <> 'CA')
               OR
                 (st.store_brand || ' ' || st.store_country = 'FabKids US')
               OR
                 (st.store_brand || ' ' || st.store_country = 'ShoeDazzle US')
               OR
                 (st.store_brand || ' ' || st.store_country = 'Fabletics US')
               OR st.store_brand = 'Yitty')
               AND dcf.original_credit_issued_local_datetime::DATE < '2018-12-01'
               THEN 'Membership Credit to Giftco'
           ELSE 'None' END AS giftco_transfer_status
FROM edw_prod.new_stg.dim_credit dcf
         JOIN edw_prod.data_model_jfb.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer dc
                   ON dcf.original_customer_id = dc.customer_id
         LEFT JOIN _giftco_transfer_key gtk
                   ON dcf.original_credit_key = gtk.original_credit_key;

CREATE OR REPLACE TEMPORARY TABLE _original_credit_activity AS
SELECT CASE
           WHEN st2.store_brand = 'Fabletics' AND dcf.original_credit_type = 'Giftcard' AND dc2.gender = 'M'
               THEN 'Fabletics Mens'
           WHEN st2.store_brand = 'Fabletics' AND dcf.original_credit_type = 'Giftcard' THEN 'Fabletics Womens'
           WHEN st2.store_brand = 'Yitty' AND dcf.original_credit_type = 'Giftcard' THEN 'Yitty'
           WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
           WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
           ELSE st.store_brand END                                                   AS brand,
       st.store_country                                                              AS country,
       st.store_region                                                               AS region,
       IFF(brand IN ('JustFab', 'ShoeDazzle', 'FabKids') AND dc.finance_specialty_store = 'CA', 'CA',
           'None')                                                                   AS finance_specialty_store,
       gts.giftco_transfer_status,
       credit_activity_type,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_reason,
       DATE_TRUNC('MONTH', dcf.original_credit_issued_local_datetime::DATE)          AS original_issued_month,
       DATE_TRUNC('MONTH', fce.credit_activity_local_datetime::DATE)                 AS activity_month,
       SUM(fce.credit_activity_gross_vat_local_amount)                               AS activity_amount,
       SUM(fce.credit_activity_local_amount)                                         AS local_net_vat_activity_amount,
       SUM(fce.credit_activity_gross_vat_local_amount *
           dcf.credit_issued_usd_conversion_rate)                                    AS usd_gross_vat_activity_amount,
       SUM(fce.credit_activity_local_amount * dcf.credit_issued_usd_conversion_rate) AS usd_net_vat_activity_amount
FROM edw_prod.new_stg.dim_credit dcf
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = dcf.original_customer_id
         JOIN edw_prod.new_stg.dim_credit dcf2 ON dcf.original_credit_key = dcf2.credit_key
         LEFT JOIN _giftco_transfer_status gts ON dcf.credit_key = gts.credit_key
         JOIN edw_prod.data_model_jfb.dim_customer dc2 ON dc2.customer_id = dcf2.original_customer_id
         JOIN EDW_PROD.NEW_STG.FACT_CREDIT_EVENT_all fce
              ON dcf.credit_key :: varchar = fce.credit_key :: varchar -- join to get all activity (need join to be on new to capture all activity, we just pull attributes from the original)
         JOIN edw_prod.data_model_jfb.dim_store st
              ON st.store_id = dcf.store_id
         JOIN edw_prod.data_model_jfb.dim_store st2
              ON st2.store_id = dcf2.store_id
WHERE fce.credit_activity_type IN
      ('Issued', 'Redeemed', 'Cancelled', 'Expired') -- only include the activity we want to consider
  AND fce.original_credit_activity_type_action = 'Include'
-- not including issued-exclude on purpose so that we include the Activity (Redemption,Cancellation,Expiration activity of the credits))
GROUP BY CASE
             WHEN st2.store_brand = 'Fabletics' AND dcf.original_credit_type = 'Giftcard' AND dc2.gender = 'M'
                 THEN 'Fabletics Mens'
             WHEN st2.store_brand = 'Fabletics' AND dcf.original_credit_type = 'Giftcard' THEN 'Fabletics Womens'
             WHEN st2.store_brand = 'Yitty' AND dcf.original_credit_type = 'Giftcard' THEN 'Yitty'
             WHEN st.store_brand = 'Fabletics' AND dc.gender = 'M' THEN 'Fabletics Mens'
             WHEN st.store_brand = 'Fabletics' THEN 'Fabletics Womens'
             ELSE st.store_brand END,
         st.store_country,
         st.store_region,
         IFF(brand IN ('JustFab', 'ShoeDazzle', 'FabKids') AND dc.finance_specialty_store = 'CA', 'CA', 'None'),
         gts.giftco_transfer_status,
         credit_activity_type,
         dcf.original_credit_tender,
         dcf.original_credit_type,
         dcf.original_credit_reason,
         DATE_TRUNC('MONTH', dcf.original_credit_issued_local_datetime::DATE),
         DATE_TRUNC('MONTH', fce.credit_activity_local_datetime::DATE);


CREATE OR REPLACE TEMPORARY TABLE _scaffold AS
SELECT brand,
       country,
       region,
       finance_specialty_store,
       giftco_transfer_status,
       original_issued_month,
       activity_month,
       credit_activity_type,
       original_credit_tender,
       original_credit_reason,
       original_credit_type
FROM (SELECT DISTINCT brand,
                      country,
                      region,
                      finance_specialty_store,
                      giftco_transfer_status,
                      credit_activity_type,
                      original_credit_reason,
                      original_credit_tender,
                      original_credit_type,
                      MIN(original_issued_month) AS min_issued_month
      FROM _original_credit_activity
      GROUP BY brand,
               country,
               region,
               finance_specialty_store,
               giftco_transfer_status,
               credit_activity_type,
               original_credit_reason,
               original_credit_tender,
               original_credit_type) AS c1
         JOIN (SELECT DISTINCT original_issued_month FROM _original_credit_activity) c2
              ON c2.original_issued_month >= min_issued_month
         JOIN (SELECT DISTINCT activity_month FROM _original_credit_activity) c3
              ON c3.activity_month >= c2.original_issued_month;

SET snapshot_datetime = CURRENT_TIMESTAMP();

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.shared.credit_activity_waterfalls_original_cohort AS
SELECT s.brand,
       s.country,
       s.region,
       s.finance_specialty_store,
       s.giftco_transfer_status,
       s.original_issued_month,
       s.activity_month,
       s.credit_activity_type                      AS activity_type,
       s.original_credit_tender,
       s.original_credit_reason,
       s.original_credit_type,
       IFNULL(cc.activity_amount, 0)               AS activity_amount,
       IFNULL(cc.local_net_vat_activity_amount, 0) AS local_net_vat_activity_amount,
       IFNULL(cc.usd_gross_vat_activity_amount, 0) AS usd_gross_vat_activity_amount,
       IFNULL(cc.usd_net_vat_activity_amount, 0)   AS usd_net_vat_activity_amount,
       $snapshot_datetime                          AS snapshot_datetime
FROM _scaffold s
         LEFT JOIN _original_credit_activity cc
                   ON s.brand = cc.brand
                       AND s.country = cc.country
                       AND s.region = cc.region
                       AND s.finance_specialty_store = cc.finance_specialty_store
                       AND s.giftco_transfer_status = cc.giftco_transfer_status
                       AND cc.original_issued_month = s.original_issued_month
                       AND cc.activity_month = s.activity_month
                       AND cc.credit_activity_type = s.credit_activity_type
                       AND cc.original_credit_tender = s.original_credit_tender
                       AND cc.original_credit_reason = s.original_credit_reason
                       AND cc.original_credit_type = s.original_credit_type
WHERE s.original_credit_reason <> 'Gift Card Redemption (Expired Credit)';
