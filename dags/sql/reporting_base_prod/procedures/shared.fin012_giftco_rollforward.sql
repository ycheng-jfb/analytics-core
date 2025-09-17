------------------------------------------------------------------------
-- fix incorrect IDs in source
-- there is a slight issue with getting the right ids from source for one send batch, so this is cleaning up the right IDs
CREATE OR REPLACE TEMPORARY TABLE _use_to_update AS
SELECT edw_prod.stg.udf_unconcat_brand(ctt.store_credit_id) AS store_credit_id,
       ctt.foreign_transaction_id                           AS incorrect_foreign_transaction_id,
       ctt.store_credit_id                                  AS complete_store_credit_id
FROM lake_consolidated_view.ultra_merchant.credit_transfer_transaction ctt
         JOIN lake_consolidated_view.ultra_merchant.credit_transfer_transaction_type cttt
              ON cttt.credit_transfer_transaction_type_id = ctt.credit_transfer_transaction_type_id
         JOIN lake_view.jfgc.gift_card_transaction gct ON gct.gift_card_transaction_id = ctt.foreign_transaction_id
WHERE gct.statuscode = 3010
  AND transaction_type = 'GENERATE'
  AND ctt.statuscode = 4461;

CREATE OR REPLACE TEMPORARY TABLE _use_to_update2 AS
SELECT gct.reference_number         AS store_credit_id,
       gct.gift_card_transaction_id AS correct_foreign_transaction_id
FROM lake_view.jfgc.gift_card_transaction gct
         LEFT JOIN lake_consolidated_view.ultra_merchant.credit_transfer_transaction ctt
                   ON ctt.foreign_transaction_id = gct.gift_card_transaction_id
WHERE ctt.credit_transfer_transaction_id IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _credit_transfer_transaction AS
SELECT *
FROM lake_consolidated_view.ultra_merchant.credit_transfer_transaction;

UPDATE _credit_transfer_transaction
SET foreign_transaction_id = CAST(correct_foreign_transaction_id AS TEXT)
FROM lake_consolidated_view.ultra_merchant.credit_transfer_transaction ctt
         JOIN _use_to_update u
ON CAST (u.complete_store_credit_id AS TEXT) = CAST (ctt.store_credit_id AS TEXT)
    JOIN _use_to_update2 u2 ON CAST (u.store_credit_id AS TEXT) = CAST (u2.store_credit_id AS TEXT)
WHERE ctt.credit_transfer_transaction_type_id < 200
  AND CAST (ctt.credit_transfer_transaction_id AS TEXT) =
    CAST (_credit_transfer_transaction.credit_transfer_transaction_id AS TEXT);

------------------------------------------------------------------------
-- handle credits that were sent to & from giftco

CREATE OR REPLACE TEMPORARY TABLE _store_credit AS
SELECT sc.store_credit_id                                                                AS original_store_credit_id,
       gc.merchant_id,
       iff(gc.merchant_id = 1002, 'FLGC', 'JFGC')                                        AS legal_entity,
--        'JFGC'                                                                            AS legal_entity, -- all store credit should be on JFGC
       iff(transaction_type = 'GENERATE', ctt.store_credit_id, NULL)                     AS transfer_in_id,
       iff(transaction_type = 'GENERATE', 'store_credit_id', NULL)                       AS transfer_in_id_type,
       iff(transaction_type = 'REDEEM', ctt.store_credit_id, NULL)                       AS transfer_out_id,
       iff(transaction_type = 'REDEEM', 'store_credit_id', NULL)                         AS transfer_out_id_type,
       CASE
           WHEN transaction_type = 'GENERATE' THEN 'Transfer To Giftco'
           WHEN transaction_type = 'REDEEM' THEN 'Transfer To Ecom As Store Credit' END  AS new_transaction_type,
       credit_transfer_transaction_type,
       ctt.datetime_transacted,
       iff(new_transaction_type ILIKE '%Transfer To Ecom%', ctt.amount * -1, ctt.amount) AS amount
FROM (SELECT ct.*,
             c.label  AS credit_transfer_transaction_type,
             c.object AS credit_transaction_object
      FROM _credit_transfer_transaction ct
               JOIN lake_consolidated_view.ultra_merchant.credit_transfer_transaction_type c
                    ON c.credit_transfer_transaction_type_id = ct.credit_transfer_transaction_type_id
      WHERE statuscode = 4461) ctt
         JOIN
     (SELECT *
      FROM lake_view.jfgc.gift_card_transaction
      WHERE statuscode = 3001 -- Success
     ) gc ON CAST(ctt.foreign_transaction_id AS TEXT) = CAST(gc.gift_card_transaction_id AS TEXT)
         JOIN lake_view.jfgc.gift_card jgc ON jgc.gift_card_id = gc.gift_card_id
         JOIN lake_consolidated_view.ultra_merchant.store_credit sc
              ON CAST(edw_prod.stg.udf_unconcat_brand(sc.store_credit_id) AS TEXT) = jgc.reference_number
         JOIN lake_consolidated_view.ultra_merchant.store_credit_reason scr
              ON scr.store_credit_reason_id = sc.store_credit_reason_id
WHERE credit_transaction_object = 'store_credit'
  AND credit_transfer_transaction_type_id NOT IN (170, 250, 270);

UPDATE _store_credit
SET legal_entity = 'JFGC'
FROM (SELECT original_store_credit_id
        FROM _store_credit
        WHERE merchant_id = 1002
        AND datetime_transacted::DATE<'2020-01-01'
        AND transfer_in_id IS NOT NULL) jfgc
WHERE jfgc.original_store_credit_id=_store_credit.original_store_credit_id;

------------------------------------------------------------------------
-- handle credits that were originally gift certificates


CREATE OR REPLACE TEMPORARY TABLE _gift_certificate AS
SELECT gcc.gift_certificate_id                                                           AS original_gift_certificate_id,
       gc.merchant_id,
       o.store_id                                                                        AS event_store_id,
       m.store_id                                                                        AS membership_store_id,
--     IFF(gc.MERCHANT_ID = 1002, 'FLGC', 'JFGC')
       iff(ds.store_brand IN ('Fabletics', 'Yitty'), 'FLGC', 'JFGC')                     AS legal_entity, -- FLGC only when store is Fabletics
       iff(transaction_type IN ('GENERATE', 'RELOAD'), ctt.gift_certificate_id,
           NULL)                                                                         AS transfer_in_id,
       iff(transaction_type IN ('GENERATE', 'RELOAD'), 'gift_certificate_id',
           NULL)                                                                         AS transfer_in_id_type,
       CASE
           WHEN transaction_type = 'REDEEM' AND credit_transfer_transaction_type_id = 260
               THEN ifnull(ctt.gift_certificate_id, gcc.gift_certificate_id)
           WHEN transaction_type = 'REDEEM' THEN ctt.store_credit_id
           ELSE NULL END                                                                 AS transfer_out_id,
       CASE
           WHEN transaction_type = 'REDEEM' AND credit_transfer_transaction_type_id = 260 THEN 'gift_certificate_id'
           WHEN transaction_type = 'REDEEM' THEN 'store_credit_id'
           ELSE NULL END                                                                 AS transfer_out_id_type,
       ctt.store_credit_reason,
       CASE
           WHEN transaction_type = 'GENERATE' THEN 'Transfer To Giftco'
           WHEN transaction_type = 'REDEEM' AND credit_transfer_transaction_type_id = 260
               THEN 'Transfer To Ecom As Gift Certificate'
           WHEN transaction_type = 'REDEEM' THEN 'Transfer To Ecom As Store Credit'
           WHEN transaction_type = 'RELOAD'
               THEN 'Reload Giftco' END                                                  AS new_transaction_type,
       iff(m.store_id = 241, 'Yitty Gift Certificates to Gift Card',
           credit_transfer_transaction_type)                                             AS credit_transfer_transaction_type,
       ctt.datetime_transacted,
       o.datetime_added                                                                  AS order_issueddatetime,
       iff(new_transaction_type ILIKE '%Transfer To Ecom%', ctt.amount * -1, ctt.amount) AS amount
FROM (SELECT ct.*,
             c.label   AS credit_transfer_transaction_type,
             c.object  AS credit_transaction_object,
             scr.label AS store_credit_reason
      FROM _credit_transfer_transaction ct
               JOIN lake_consolidated_view.ultra_merchant.credit_transfer_transaction_type c
                    ON c.credit_transfer_transaction_type_id = ct.credit_transfer_transaction_type_id
               LEFT JOIN lake_consolidated_view.ultra_merchant.store_credit sc
                         ON sc.store_credit_id = ct.store_credit_id
               LEFT JOIN lake_consolidated_view.ultra_merchant.store_credit_reason scr
                         ON scr.store_credit_reason_id = sc.store_credit_reason_id
      WHERE ct.statuscode = 4461) ctt
         JOIN
     (SELECT *
      FROM lake_view.jfgc.gift_card_transaction
      WHERE statuscode = 3001 -- Success
     ) gc ON CAST(ctt.foreign_transaction_id AS TEXT) = CAST(gc.gift_card_transaction_id AS TEXT)
         JOIN lake_view.jfgc.gift_card jgc ON jgc.gift_card_id = gc.gift_card_id
         JOIN lake_consolidated_view.ultra_merchant.gift_certificate gcc
              ON CAST(edw_prod.stg.udf_unconcat_brand(gcc.gift_certificate_id) AS TEXT) = jgc.reference_number
         LEFT JOIN lake_consolidated_view.ultra_merchant."ORDER" o
                   ON gcc.order_id = o.order_id AND o.processing_statuscode NOT IN (2335, 6215, 3158, 2205, 3438, 3358)
         LEFT JOIN lake_consolidated_view.ultra_merchant_history.membership m
                   ON o.customer_id = m.customer_id
                       AND o.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
         LEFT JOIN edw_prod.data_model.dim_store ds ON ds.store_id = o.store_id
WHERE (credit_transaction_object = 'gift_certificate'
    OR credit_transfer_transaction_type_id = 250);

------------------------------------------------------------------------
-- NEW : handle credits that were originally gift certificates and didn't go through giftco,
-- but directly transfer to store_credit from ecomm

CREATE OR REPLACE TEMPORARY TABLE _gift_certificate_to_ecom AS

SELECT gcsc.gift_certificate_id                                      AS original_gift_certificate_id,
       gcsc.store_credit_id,
       NULL                                                          AS merchant_id,
       o.store_id                                                    AS event_store_id,
       m.store_id                                                    AS membership_store_id,
       iff(ds.store_brand IN ('Fabletics', 'Yitty'), 'FLGC', 'JFGC') AS legal_entity, -- FLGC only when store is Fabletics
       NULL                                                          AS transfer_in_id,
       NULL                                                          AS transfer_in_id_type,
       NULL                                                          AS transfer_out_id,
       NULL                                                          AS transfer_out_id_type,
       NULL                                                          AS store_credit_reason,
       'Gift Certificate Convert To Store Credit'                    AS new_transaction_type,
       'Convert To Store Credit'                                     AS credit_transfer_transaction_type,
       gc.datetime_added                                             AS datetime_transacted,
       o.datetime_added                                              AS order_issueddatetime,
       0                                                             AS amount
FROM lake_consolidated_view.ultra_merchant.gift_certificate_store_credit gcsc
         JOIN lake_consolidated_view.ultra_merchant.store_credit sc ON gcsc.store_credit_id = sc.store_credit_id
         JOIN lake_consolidated_view.ultra_merchant.statuscode s ON sc.statuscode = s.statuscode
         JOIN lake_consolidated_view.ultra_merchant.gift_certificate gc
              ON gcsc.gift_certificate_id = gc.gift_certificate_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.credit_transfer_transaction ctt
                   ON gcsc.store_credit_id = ctt.store_credit_id
         LEFT JOIN lake_consolidated_view.ultra_merchant."ORDER" o
                   ON gc.order_id = o.order_id AND o.processing_statuscode NOT IN (2335, 6215, 3158, 2205, 3438, 3358)
         LEFT JOIN lake_consolidated_view.ultra_merchant_history.membership m
                   ON o.customer_id = m.customer_id
                       AND o.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
         LEFT JOIN edw_prod.data_model.dim_store ds ON ds.store_id = o.store_id
WHERE ctt.store_credit_id IS NULL;

------------------------------------------------------------------------
-- NEW : Handling the JFB US converted tokens that were sent to giftco, these get transferred out as variable credit

CREATE OR REPLACE TEMPORARY TABLE _membership_token AS
SELECT mt.membership_token_id                                                            AS original_membership_token_id,
       gc.merchant_id,
       'JFGC'                                                                            AS legal_entity,
       iff(transaction_type = 'GENERATE', mt.membership_token_id, NULL)                  AS transfer_in_id,
       iff(transaction_type = 'GENERATE', 'membership_token_id', NULL)                   AS transfer_in_id_type,
       iff(transaction_type = 'REDEEM', ctt.store_credit_id, NULL)                       AS transfer_out_id,
       iff(transaction_type = 'REDEEM', 'store_credit_id', NULL)                         AS transfer_out_id_type,
       CASE
           WHEN transaction_type = 'GENERATE' THEN 'Transfer To Giftco'
           WHEN transaction_type = 'REDEEM' THEN 'Transfer To Ecom As Store Credit' END  AS new_transaction_type,
       credit_transfer_transaction_type,
       ctt.datetime_transacted,
       iff(new_transaction_type ILIKE '%Transfer To Ecom%', ctt.amount * -1, ctt.amount) AS amount
FROM (SELECT ct.*,
             c.label  AS credit_transfer_transaction_type,
             c.object AS credit_transaction_object
      FROM _credit_transfer_transaction ct
               JOIN lake_consolidated_view.ultra_merchant.credit_transfer_transaction_type c
                    ON c.credit_transfer_transaction_type_id = ct.credit_transfer_transaction_type_id
      WHERE statuscode = 4461) ctt
         JOIN
     (SELECT *
      FROM lake_view.jfgc.gift_card_transaction
      WHERE statuscode = 3001 -- Success
     ) gc ON CAST(ctt.foreign_transaction_id AS TEXT) = CAST(gc.gift_card_transaction_id AS TEXT)
         JOIN lake_view.jfgc.gift_card jgc ON jgc.gift_card_id = gc.gift_card_id
         JOIN lake_consolidated_view.ultra_merchant.membership_token mt
              ON CAST(mt.meta_original_membership_token_id AS TEXT) = jgc.reference_number
WHERE credit_transfer_transaction_type_id IN (170, 270);

------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.fin012_giftco_rollforward
    (membership_store_brand VARCHAR(155),
     store_brand VARCHAR(155),
     store_country VARCHAR(155),
     store_region VARCHAR(155),
     purchase_location VARCHAR(155),
     event_store_id INT,
     membership_store_id INT,
     original_credit_id INT,
     merchant_id INT,
     legal_entity VARCHAR(155),
     store_credit_id INT,
     transfer_in_id INT,
     transfer_in_id_type VARCHAR(155),
     transfer_out_id INT,
     transfer_out_id_type VARCHAR(155),
     original_issued_month DATE,
     original_credit_tender VARCHAR(155),
     original_credit_type VARCHAR(155),
     original_credit_reason VARCHAR(155),
     new_credit_type VARCHAR(155),
     new_credit_reason VARCHAR(155),
     credit_activity_type VARCHAR(155),
     activity_month DATE,
     activity_amount DECIMAL(20, 4),
     giftco_type VARCHAR(155),
     data_refresh_datetime DATETIME
        ) AS
SELECT iff(mst.store_brand = 'Fabletics', 'Fabletics Womens', mst.store_brand) AS membership_store_brand,
       iff(st.store_brand = 'Fabletics', 'Fabletics Womens', st.store_brand)   AS store_brand,
       st.store_country,
       st.store_region,
       iff(st.store_type = 'Retail', 'Retail', 'Online')                       AS purchase_location,
       st.store_id                                                             AS event_store_id,
       m.store_id                                                              AS membership_store_id,
       original_store_credit_id                                                AS original_credit_id,
       gc.merchant_id,
       gc.legal_entity                                                         AS legal_entity,
       iff(transfer_out_id_type = 'store_credit_id', transfer_out_id, NULL)    AS store_credit_id,
       transfer_in_id,
       transfer_in_id_type,
       transfer_out_id,
       transfer_out_id_type,
       date_trunc(MONTH, dc.credit_issued_hq_datetime)::date                   AS original_issued_month, dc.original_credit_tender,
       dc.original_credit_type,
       dc.original_credit_reason,
       dc.credit_type                                                          AS new_credit_type,
       dc.credit_reason                                                        AS new_credit_reason,
       new_transaction_type                                                    AS credit_activity_type,
       date_trunc(MONTH, datetime_transacted)::date                            AS activity_month, gc.amount AS activity_amount,
       'Store Credit to Giftco'                                                AS giftco_type,
       CURRENT_TIMESTAMP()                                                     AS data_refresh_datetime
FROM _store_credit gc
         JOIN reporting_base_prod.shared.dim_credit dc ON dc.credit_id = gc.original_store_credit_id
            AND dc.credit_type ILIKE '%credit%'
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dc.store_id
         JOIN lake_consolidated_view.ultra_merchant_history.membership m ON dc.customer_id = m.customer_id
            AND dc.credit_issued_local_datetime BETWEEN m.effective_start_datetime AND m.effective_end_datetime
         LEFT JOIN edw_prod.data_model.dim_store mst ON mst.store_id = m.store_id

UNION

SELECT CASE
           WHEN mst.store_id = 241 THEN 'Yitty'
           WHEN mst.store_id = 52 THEN 'Fabletics Womens'
           ELSE iff(st.store_brand = 'Fabletics', 'Fabletics Womens', st.store_brand) END AS membership_store_brand,
       iff(st.store_brand = 'Fabletics', 'Fabletics Womens', st.store_brand)              AS store_brand,
       st.store_country                                                                   AS store_country,
       st.store_region                                                                    AS store_region,
       iff(st.store_type = 'Retail', 'Retail', 'Online')                                  AS purchase_location,
       gc.event_store_id                                                                  AS event_store_id,
       gc.membership_store_id                                                             AS membership_store_id,
       original_gift_certificate_id                                                       AS original_credit_id,
       gc.merchant_id,
       gc.legal_entity,
       iff(transfer_out_id_type = 'store_credit_id', transfer_out_id, NULL)               AS store_credit_id,
       transfer_in_id,
       transfer_in_id_type,
       transfer_out_id,
       transfer_out_id_type,
       date_trunc(MONTH, iff(membership_store_id = 241, gc.order_issueddatetime,
            iff(gctt.label in ('Retail Card - Purchased'), gct.datetime_modified,
                gct.datetime_added))::DATE)                                               AS original_issued_month,
       iff(gct.order_id IS NOT NULL, 'Cash', 'NonCash')                                   AS original_credit_tender,
       'Gift Certificate'                                                                 AS original_credit_type,
       gctt.label                                                                         AS original_credit_reason,
       original_credit_type                                                               AS new_credit_type,
       original_credit_reason                                                             AS new_credit_reason,
       new_transaction_type                                                               AS credit_activity_type,
       date_trunc(MONTH, datetime_transacted)::date                                       AS activity_month, gc.amount AS activity_amount,
       'Gift certificate to Giftco'                                                       AS giftco_type,
       CURRENT_TIMESTAMP()                                                                AS data_refresh_datetime
FROM _gift_certificate gc
         JOIN lake_consolidated_view.ultra_merchant.gift_certificate gct
              ON gct.gift_certificate_id = gc.original_gift_certificate_id
         JOIN lake_consolidated_view.ultra_merchant.gift_certificate_type gctt
              ON gctt.gift_certificate_type_id = gct.gift_certificate_type_id
         JOIN edw_prod.data_model.dim_store st
              ON st.store_id = gc.event_store_id
         LEFT JOIN edw_prod.data_model.dim_store mst
              ON mst.store_id = gc.membership_store_id

UNION

SELECT CASE
           WHEN mst.store_id = 241 THEN 'Yitty'
           WHEN mst.store_id = 52 THEN 'Fabletics Womens'
           ELSE iff(st.store_brand = 'Fabletics', 'Fabletics Womens', st.store_brand) END AS membership_store_brand,
       iff(st.store_brand = 'Fabletics', 'Fabletics Womens', st.store_brand)              AS store_brand,
       st.store_country                                                                   AS store_country,
       st.store_region                                                                    AS store_region,
       iff(st.store_type = 'Retail', 'Retail', 'Online')                                  AS purchase_location,
       gce.event_store_id                                                                 AS event_store_id,
       gce.membership_store_id                                                            AS membership_store_id,
       original_gift_certificate_id                                                       AS original_credit_id,
       gce.merchant_id,
       gce.legal_entity,
       gce.store_credit_id,
       transfer_in_id,
       transfer_in_id_type,
       transfer_out_id,
       transfer_out_id_type,
       date_trunc(MONTH, iff(membership_store_id = 241, gce.order_issueddatetime,
            iff(gctt.label in ('Retail Card - Purchased'), gct.datetime_modified,
                gct.datetime_added))::DATE)                                               AS original_issued_month,
       iff(gct.order_id IS NOT NULL, 'Cash', 'NonCash')                                   AS original_credit_tender,
       'Gift Certificate'                                                                 AS original_credit_type,
       gctt.label                                                                         AS original_credit_reason,
       original_credit_type                                                               AS new_credit_type,
       original_credit_reason                                                             AS new_credit_reason,
       new_transaction_type                                                               AS credit_activity_type,
       date_trunc(MONTH, datetime_transacted)::date                                       AS activity_month, gce.amount AS activity_amount,
       'Gift Certificate to Ecom'                                                         AS giftco_type,
       CURRENT_TIMESTAMP()                                                                AS data_refresh_datetime
FROM _gift_certificate_to_ecom gce
         JOIN lake_consolidated_view.ultra_merchant.gift_certificate gct
              ON gct.gift_certificate_id = gce.original_gift_certificate_id
         JOIN lake_consolidated_view.ultra_merchant.gift_certificate_type gctt
              ON gctt.gift_certificate_type_id = gct.gift_certificate_type_id
         JOIN edw_prod.data_model.dim_store st
              ON st.store_id = gce.event_store_id
         LEFT JOIN edw_prod.data_model.dim_store mst
              ON mst.store_id = gce.membership_store_id

UNION

SELECT iff(mst.store_brand = 'Fabletics', 'Fabletics Womens', mst.store_brand) AS membership_store_brand,
       iff(st.store_brand = 'Fabletics', 'Fabletics Womens', st.store_brand)   AS store_brand,
       st.store_country,
       st.store_region,
       iff(st.store_type = 'Retail', 'Retail', 'Online')                       AS purchase_location,
       st.store_id                                                             AS event_store_id,
       m.store_id                                                              AS membership_store_id,
       original_membership_token_id                                            AS original_credit_id,
       gc.merchant_id,
       gc.legal_entity                                                         AS legal_entity,
       iff(transfer_out_id_type = 'store_credit_id', transfer_out_id, NULL)    AS store_credit_id,
       transfer_in_id,
       transfer_in_id_type,
       transfer_out_id,
       transfer_out_id_type,
       (date_trunc(MONTH, dc.credit_issued_hq_datetime)::DATE)                 AS original_issued_month,
       dc.original_credit_tender,
       dc.original_credit_type,
       dc.original_credit_reason,
       'Variable Credit'                                                       AS new_credit_type,
       'Gift Card Redemption (Expired Token)'                                  AS new_credit_reason,
       new_transaction_type                                                    AS credit_activity_type,
       (date_trunc(MONTH, datetime_transacted)::DATE)                          AS activity_month,
       gc.amount                                                               AS activity_amount,
       'Token to Giftco'                                                       AS giftco_type,
       CURRENT_TIMESTAMP()                                                     AS data_refresh_datetime
FROM _membership_token gc
         JOIN reporting_base_prod.shared.dim_credit dc ON dc.credit_id = gc.original_membership_token_id
            AND dc.credit_type ILIKE '%Token%'
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dc.store_id
         JOIN lake_consolidated_view.ultra_merchant_history.membership m ON dc.customer_id = m.customer_id
            AND dc.credit_issued_local_datetime BETWEEN m.effective_start_datetime AND m.effective_end_datetime
         LEFT JOIN edw_prod.data_model.dim_store mst ON mst.store_id = m.store_id;

ALTER TABLE reporting_base_prod.shared.fin012_giftco_rollforward
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO reporting_base_prod.shared.fin012_giftco_rollforward_snapshot
SELECT membership_store_brand,
       store_brand,
       store_country,
       store_region,
       purchase_location,
       original_credit_id,
       merchant_id,
       legal_entity,
       store_credit_id,
       transfer_in_id,
       transfer_in_id_type,
       transfer_out_id,
       transfer_out_id_type,
       original_issued_month,
       original_credit_tender,
       original_credit_type,
       original_credit_reason,
       new_credit_type,
       new_credit_reason,
       credit_activity_type,
       activity_month,
       activity_amount,
       giftco_type,
       data_refresh_datetime snapshot_datetime
FROM reporting_base_prod.shared.fin012_giftco_rollforward;

DELETE
FROM reporting_base_prod.shared.fin012_giftco_rollforward_snapshot
WHERE snapshot_datetime < DATEADD(MONTH, -12, getdate());
