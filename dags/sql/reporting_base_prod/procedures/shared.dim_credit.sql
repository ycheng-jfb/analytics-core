------------------------------------------------------------------------
-- in order_credit, whenever the amount is less than 0, the store_credit_id is actually an issuance
-- this happens when someone redeems a fixed credit and we find that we cannot fulfill the order for an individaul item
-- since the credit is unbreakable, we cannot unwind the original credit, so we will issue a variable credit to make up the amount
-- in these cases, the store_credit_id is actual the new credit issued
CREATE OR REPLACE TEMPORARY TABLE _refund_partial_shipment AS
SELECT DISTINCT store_credit_id
FROM lake_consolidated.ultra_merchant.order_credit
WHERE amount < 0
  AND hvr_is_deleted = 0;

-- We want to tie the correct store_id to the credit_id at the time the credit was issued.  YITTY launch breaks this and now we have to use lake_history tables
CREATE OR REPLACE TEMP TABLE _ultra_merchant_membership AS
SELECT sc.store_credit_id AS credit_id, m.store_id, m.membership_id, m.customer_id, 'store_credit_id'::varchar(30) as source
FROM lake_consolidated.ultra_merchant.store_credit AS sc
    JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON sc.customer_id = m.customer_id
        AND sc.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
GROUP BY sc.store_credit_id, m.store_id, m.membership_id, m.customer_id;

--inserting tokens
INSERT INTO _ultra_merchant_membership
(
    credit_id,
    store_id,
    membership_id,
    customer_id,
    source
)
SELECT mt.membership_token_id AS credit_id, m.store_id, m.membership_id, m.customer_id, 'token' as source
FROM lake_consolidated.ultra_merchant.membership_token AS mt
    JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.membership_id = mt.membership_id
        AND mt.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
GROUP BY mt.membership_token_id, m.store_id, m.membership_id, m.customer_id;

-- inserting bounceback endowments
INSERT INTO _ultra_merchant_membership
(
    credit_id,
    store_id,
    membership_id,
    customer_id,
    source
)
SELECT gc.gift_certificate_id AS credit_id, m.store_id, m.membership_id, m.customer_id, 'bounceback' as source
FROM lake_consolidated.ultra_merchant.gift_certificate AS gc
    JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
        ON be.object_id = gc.gift_certificate_id
            AND be.object = 'gift_certificate'
    JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = be.customer_id
        AND be.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
GROUP BY gc.gift_certificate_id, m.store_id, m.membership_id, m.customer_id;

-- Retail & SXF Gift Cards
INSERT INTO _ultra_merchant_membership
(
    credit_id,
    store_id,
    membership_id,
    customer_id,
    source
)
SELECT gc.gift_certificate_id AS credit_id, m.store_id, m.membership_id, m.customer_id, 'giftcard' as source
FROM lake_consolidated.ultra_merchant.gift_certificate AS gc
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON gc.order_id = o.order_id
    LEFT JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = COALESCE(gc.customer_id, o.customer_id)
        AND COALESCE(gc.datetime_added, o.datetime_added) BETWEEN m.effective_start_datetime AND m.effective_end_datetime
WHERE gc.order_id IS NOT NULL
GROUP BY gc.gift_certificate_id, m.store_id, m.membership_id, m.customer_id;

--lake_history customer for store_credits
CREATE OR REPLACE TEMP TABLE _ultra_merchant_customer AS
SELECT sc.store_credit_id AS credit_id, c.store_id, c.customer_id, 'store_credit_id'::VARCHAR(30) as source
FROM lake_consolidated.ultra_merchant.store_credit AS sc
    JOIN lake_consolidated.ultra_merchant_history.customer AS c
        ON c.customer_id = sc.customer_id
WHERE sc.datetime_added BETWEEN c.effective_start_datetime AND c.effective_end_datetime
GROUP BY sc.store_credit_id, c.store_id, c.customer_id;

--inserting tokens
INSERT INTO _ultra_merchant_customer
(
    credit_id,
    store_id,
    customer_id,
    source
)
SELECT mt.membership_token_id AS credit_id, c.store_id, c.customer_id, 'token' as source
FROM lake_consolidated.ultra_merchant.membership_token AS mt
    JOIN lake_consolidated.ultra_merchant.membership m
        ON mt.membership_id = m.membership_id
    JOIN lake_consolidated.ultra_merchant_history.customer AS c
        ON c.customer_id = m.customer_id
WHERE mt.datetime_added BETWEEN c.effective_start_datetime AND c.effective_end_datetime
GROUP BY mt.membership_token_id, c.store_id, c.customer_id;

--inserting gift certificates
INSERT INTO _ultra_merchant_customer
(credit_id,
 store_id,
 customer_id,
 source)
SELECT gc.gift_certificate_id, c.store_id, c.customer_id, 'giftcard' AS source
FROM lake_consolidated.ultra_merchant.gift_certificate AS gc
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON gc.order_id = o.order_id
         JOIN lake_consolidated.ultra_merchant_history.customer AS c
              ON c.customer_id = COALESCE(gc.customer_id, o.order_id)
WHERE COALESCE(gc.datetime_added, o.datetime_added) BETWEEN c.effective_start_datetime AND c.effective_end_datetime
  AND gc.order_id IS NOT NULL
GROUP BY gc.gift_certificate_id, c.store_id, c.customer_id
;
------------------------------------------------------------------------
-- Trying to understand the flow of the credits themselves

CREATE OR REPLACE TEMPORARY TABLE _dim_credit AS
SELECT DISTINCT sc.store_credit_id               AS credit_id,
       sc.meta_original_store_credit_id AS meta_original_credit_id,
       'store_credit_id'                AS source_credit_id_type,
       CASE
           WHEN st.store_type = 'Retail' AND st.store_brand = 'Fabletics' THEN 52
           WHEN st.store_type = 'Retail' AND st.store_brand = 'Savage X' THEN 121
           WHEN st.store_id = 41 THEN 26
           ELSE st.store_id END         AS store_id,
       c.customer_id,
       IFF(msc.store_credit_id IS NOT NULL, 'Fixed Credit', 'Variable Credit') AS credit_type,
       IFF(scr.cash = 1, 'Cash', 'NonCash')                                    AS credit_tender,
       CASE
           WHEN st.store_id = 55 AND sc.amount = 9.95 AND scr.cash = 1 THEN 'ShoeDazzle Classic Credit'
           WHEN st.store_id = 55 AND scr.label = 'Membership Credit' AND sc.datetime_added < '2014-04-01' AND
                msc.store_credit_id IS NULL THEN 'Legacy Credit'
           WHEN st.store_id = 46 AND scr.label = 'Membership Credit' AND sc.datetime_added < '2013-06-01' AND
                msc.store_credit_id IS NULL THEN 'Legacy Credit'
           WHEN msc.store_credit_id IS NULL AND scr.label = 'Membership Credit' THEN 'Other Membership Credit'
           ELSE scr.label END                                                  AS credit_reason,
       scr.label                                                               AS source_credit_reason,
       CASE
           WHEN msc.order_id IS NOT NULL THEN 'Billing Order'
           WHEN rsc.store_credit_id IS NOT NULL OR ref.payment_transaction_id IS NOT NULL THEN 'Refund'
           WHEN gcs.store_credit_id IS NOT NULL THEN 'Gift Certificate'
           WHEN sccc.original_store_credit_id IS NOT NULL THEN 'Fixed To Variable Conversion'
           WHEN mcl.converted_store_credit_id IS NOT NULL THEN 'Token To Credit Conversion'
           WHEN rps.store_credit_id IS NOT NULL THEN 'Refund Partial Shipment'
           WHEN tt2.store_credit_id IS NOT NULL THEN 'Giftco Roundtrip'
           WHEN scr.label = 'Converted Membership Token - Promotional Credit' THEN 'Expired Token to Credit'
           WHEN st.store_id = 46 AND sc.datetime_added < '2014-01-01' THEN 'FabKids Pre Migration'
           WHEN st.store_id = 55 AND sc.datetime_added < '2014-03-15' THEN 'ShoeDazzle Pre Migration'
           WHEN st.store_id = 55 AND sc.amount = 9.95 AND scr.cash = 1 THEN 'ShoeDazzle Classic Credit'
           WHEN scr.cash = 0
               THEN 'Manager Discretion' -- map any of the rest of the noncash credits we dont know to Manager Discretion
           ELSE 'Unknown' END                                                  AS credit_issuance_reason,
       msc.order_id                                                            AS credit_order_id,
       NULL                                                                    AS original_credit_id,
       sc.amount                                                               AS credit_amount,
       sc.datetime_added::TIMESTAMP_TZ                                         AS credit_issued_hq_datetime,
       CONVERT_TIMEZONE(st.store_time_zone, sc.datetime_added)::TIMESTAMP_TZ   AS credit_issued_local_datetime,
       sc.administrator_id
FROM lake_consolidated.ultra_merchant.store_credit sc
         JOIN lake_consolidated.ultra_merchant.statuscode s
              ON s.statuscode = sc.statuscode
         JOIN _ultra_merchant_customer c
              ON c.credit_id = sc.store_credit_id
              AND c.source = 'store_credit_id'
         LEFT JOIN _ultra_merchant_membership ms
                   ON ms.credit_id = sc.store_credit_id
                   AND ms.source = 'store_credit_id'
         JOIN edw_prod.data_model.dim_store st
              ON st.store_id = COALESCE(ms.store_id, c.store_id)
         JOIN lake_consolidated.ultra_merchant.store_credit_reason scr
              ON scr.store_credit_reason_id = sc.store_credit_reason_id
         LEFT JOIN lake_consolidated.ultra_merchant.membership_store_credit msc
                   ON sc.store_credit_id = msc.store_credit_id
         LEFT JOIN lake_consolidated.ultra_merchant.refund_store_credit rsc -- join to see if the refund credit was issued from a refund
                   ON rsc.store_credit_id = sc.store_credit_id
         LEFT JOIN lake_consolidated.ultra_merchant.refund ref -- join to see if the refund credit was issued from a refund
                   ON ref.payment_transaction_id = sc.store_credit_id
                       AND ref.payment_method = 'store_credit'
         LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit gcs -- join to see if the credit originated from a gift card
                   ON gcs.store_credit_id = sc.store_credit_id
         LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate gc
                   ON gc.gift_certificate_id = gcs.gift_certificate_id
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_conversion sccc
                   ON sccc.converted_store_credit_id = sc.store_credit_id
         LEFT JOIN _refund_partial_shipment rps
                   ON sc.store_credit_id = rps.store_credit_id
         LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt2
                   ON tt2.store_credit_id = sc.store_credit_id
                       AND tt2.credit_transfer_transaction_type_id IN (200, 210, 220, 250, 270)
         LEFT JOIN lake_consolidated.ultra_merchant.membership_token_credit_conversion mcl
            ON sc.store_credit_id = mcl.converted_store_credit_id

UNION ALL

SELECT DISTINCT mt.membership_token_id               AS credit_id,
       mt.meta_original_membership_token_id AS meta_original_credit_id,
       'Token'                              AS source_credit_id_type,
       CASE
           WHEN st.store_type = 'Retail' AND st.store_brand = 'Fabletics' THEN 52
           WHEN st.store_type = 'Retail' AND st.store_brand = 'Savage X' THEN 121
           WHEN st.store_id = 41 THEN 26
           ELSE st.store_id END             AS store_id,
       m.customer_id,
       'Token'                              AS credit_type,
       IFF(mtr.cash = 1, 'Cash', 'NonCash') AS credit_tender,
       mtr.label                                                             AS credit_reason,
       mtr.label                                                             AS source_credit_reason,
       CASE
           WHEN mt.order_id IS NOT NULL THEN 'Billing Order'
           WHEN mtr.label IN ('Converted Membership Credit', 'Refund - Converted Credit')
               THEN 'Credit To Token Conversion'
           ELSE 'Unknown' END                                                AS credit_issuance_reason,
       mt.order_id                                                           AS credit_order_id,
       NULL                                                                  AS original_credit_id,
       mt.purchase_price                                                     AS credit_amount,
       mt.datetime_added::TIMESTAMP_TZ                                       AS credit_issued_hq_datetime,
       CONVERT_TIMEZONE(st.store_time_zone, mt.datetime_added)::TIMESTAMP_TZ AS credit_issued_local_datetime,
       mt.administrator_id
FROM lake_consolidated.ultra_merchant.membership_token mt
         JOIN lake_consolidated.ultra_merchant.membership_token_reason mtr
              ON mtr.membership_token_reason_id = mt.membership_token_reason_id
         JOIN _ultra_merchant_membership m
              ON m.credit_id = mt.membership_token_id
              AND m.source = 'token'
         JOIN edw_prod.data_model.dim_store st
              ON st.store_id = m.store_id
         JOIN lake_consolidated.ultra_merchant.statuscode sc
              ON sc.statuscode = mt.statuscode

UNION ALL

SELECT DISTINCT
    gc.gift_certificate_id                                                  AS credit_id,
    gc.meta_original_gift_certificate_id                                    AS meta_original_credit_id,
    'gift_certificate_id'                                                   AS source_credit_id_type,
    CASE
        WHEN st.store_type = 'Retail' AND st.store_brand = 'Fabletics' THEN 52
        WHEN st.store_type = 'Retail' AND st.store_brand = 'Savage X' THEN 121
        WHEN st.store_id = 41 THEN 26
        ELSE st.store_id END                                                AS store_id,
    be.customer_id,
    'Giftcard'                                                               AS credit_type,
    'NonCash'                                                                AS cerdit_tender,
    'Bounceback Endowment'                                                   AS credit_reason,
    'Bounceback Endowment'                                                   AS source_credit_reason,
    'Bounceback Endowment'                                                   AS credit_issuance_reason,
    be.order_id                                                              AS credit_order_id,
    NULL                                                                     AS original_credit_id,
    gc.amount                                                                AS credit_amount,
    gc.datetime_added::TIMESTAMP_TZ                                          AS credit_issued_hq_datetime,
    CONVERT_TIMEZONE(st.store_time_zone, gc.datetime_added)::TIMESTAMP_TZ    AS credit_issued_local_datetime,
    be.administrator_id
FROM lake_consolidated.ultra_merchant.bounceback_endowment AS be
    JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
        ON gc.gift_certificate_id = be.object_id
        AND be.object = 'gift_certificate'
    JOIN _ultra_merchant_membership AS m
        ON m.credit_id = gc.gift_certificate_id
        AND m.source = 'bounceback'
    JOIN edw_prod.data_model.dim_store st
        ON st.store_id = m.store_id

UNION ALL

SELECT DISTINCT
    gc.gift_certificate_id                                                   AS credit_id,
    gc.meta_original_gift_certificate_id                                     AS meta_original_credit_id,
    'gift_certificate_id'                                                    AS source_credit_id_type,
    COALESCE(m.store_id, st.store_id) as store_id,
    COALESCE(m.customer_id, c.customer_id, gc.customer_id, o.customer_id, -1) AS customer_id,
    'Giftcard'                                                               AS credit_type,
    'Cash'                                                                AS credit_tender,
    'Gift Card - Paid'                                                   AS credit_reason,
    'Gift Card - Paid'                                                     AS source_credit_reason,
    'Gift Card - Paid'                                                     AS credit_issuance_reason,
    gc.order_id                                                              AS credit_order_id,
    NULL                                                                     AS original_credit_id,
    gc.amount                                                                AS credit_amount,
    o.datetime_added::TIMESTAMP_TZ                                          AS credit_issued_hq_datetime,
    CONVERT_TIMEZONE(st.store_time_zone, o.datetime_added)::TIMESTAMP_TZ    AS credit_issued_local_datetime,
    NULL AS administrator_id
FROM lake_consolidated.ultra_merchant.gift_certificate AS gc
    LEFT JOIN _ultra_merchant_membership AS m
        ON m.credit_id = gc.gift_certificate_id
        AND m.source = 'giftcard'
    LEFT JOIN _ultra_merchant_customer AS c
        ON c.credit_id = gc.gift_certificate_id
            AND c.source = 'giftcard'
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON gc.order_id = o.order_id
    JOIN edw_prod.stg.dim_store AS st
        ON st.store_id = COALESCE(m.store_id, o.store_id)
WHERE gc.gift_certificate_type_id <> 9;

UPDATE _dim_credit
SET credit_type = 'Variable Credit'
WHERE credit_reason = 'Refund - Partial Shipment'
  AND credit_type = 'Fixed Credit';


CREATE OR REPLACE TEMPORARY TABLE _dim_credit_key AS
SELECT ROW_NUMBER() OVER (ORDER BY credit_issued_hq_datetime) AS credit_key,
       dc.*,
       CAST('No State Expected' AS TEXT)                      AS credit_state,
       CAST('No State Expected' AS TEXT)                      AS credit_state_logic_source,
       CAST(st.store_country AS TEXT)                         AS country_code
FROM _dim_credit dc
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dc.store_id;

-- delete flse credits that were accidentaly issued & immediately cancelled
-- accounting wanted them to look like they never even were issued because it causes issues with our credit activity rates
DELETE
FROM _dim_credit_key
WHERE credit_id IN (SELECT store_credit_id FROM edw_prod.reference.credit_ids_with_issue)
  AND credit_type ILIKE '%credit%';


-- get the state of each credit for US brands
CREATE OR REPLACE TEMPORARY TABLE _credit_with_state AS
SELECT dc.credit_key,
       dc.customer_id,
       dc.credit_id,
       dc.credit_type,
       COALESCE(NULLIF(a1.state, ''),NULLIF(a2.state, ''), NULLIF(a3.state, ''), 'Unknown') AS credit_state,
       COALESCE(a1.country_code, a2.country_code, 'US')  AS country_code,
       CAST(CASE
                WHEN NULLIF(a1.state, '') IS NOT NULL THEN 'Billing Address'
                WHEN NULLIF(a2.state, '') IS NOT NULL THEN 'Shipping Address'
                WHEN NULLIF(a3.state, '') IS NOT NULL THEN 'Customer Default'
                ELSE 'Unknown' END AS TEXT)              AS credit_state_logic_source
FROM _dim_credit_key dc -- all credit activity
         JOIN edw_prod.data_model.dim_store ds ON ds.store_id = dc.store_id
         LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o ON o.order_id = dc.credit_order_id
         LEFT JOIN lake_consolidated.ultra_merchant.address a1 ON a1.address_id = o.billing_address_id
         LEFT JOIN lake_consolidated.ultra_merchant.address a2 ON a2.address_id = o.shipping_address_id
         LEFT JOIN lake_consolidated.ultra_merchant.customer cd
                   ON cd.customer_id = dc.customer_id -- original credit customer default address (usually same customer_id)
         LEFT JOIN lake_consolidated.ultra_merchant.address a3 ON a3.address_id = cd.default_address_id
WHERE ds.store_country = 'US';


-- we want to prioritize a state from a shipping or a billing address first, even above the customer default state
-- so, find the first credit_id that had a valid state. (state != 'Unkown' and credit_state_source_logic not in ('Customer Default','Unknown'))
CREATE OR REPLACE TEMPORARY TABLE _first_state AS
SELECT cs.customer_id,
       cs.credit_state,
       cs.country_code
FROM _credit_with_state cs
         JOIN (
    SELECT customer_id,
           MIN(credit_id) AS credit_id
    FROM _credit_with_state
    WHERE credit_state != 'Unknown'
      AND credit_state_logic_source NOT IN ('Customer Default', 'Unknown')
      AND credit_type ILIKE '%credit%'
    GROUP BY customer_id) cs2
              ON cs.customer_id = cs2.customer_id
                  AND cs.credit_id = cs2.credit_id;

-- update the credit state & credit_state_logic_source to be the clean state from a billing or shipping address
-- credit_state_logic_source = 'First State Logic' so we can identify how these states got updated
UPDATE _credit_with_state
SET country_code              = fs.country_code,
    credit_state              = fs.credit_state,
    credit_state_logic_source = 'First State Logic'
FROM _first_state fs
WHERE fs.customer_id = _credit_with_state.customer_id
  AND _credit_with_state.credit_state_logic_source IN ('Unknown', 'Customer Default');

-- update the final with
UPDATE _dim_credit_key
SET credit_state              = CASE
                                    WHEN lower(cs.country_code) = 'au' THEN 'AUS'
                                    WHEN lower(cs.country_code) = 'ca' THEN 'CAN'
                                    ELSE COALESCE(cs.credit_state, 'Unknown') END,
    credit_state_logic_source = cs.credit_state_logic_source
FROM _credit_with_state cs
WHERE cs.credit_key = _dim_credit_key.credit_key;



------------------------------------------------------------------------
/*
 The below gets the original store credit ids for credits that were Sent to Giftco & Transferred Back to Ecom
    We are attempting to get the Original Credit ID for any credits where store_Credit_reason = 'Gift Card Redemption (Expired Credit)'
    Some credits are sent to giftco, transferred back to ecom, sent to giftco a second time and transferred back again, we need to tie all the way back to the original credits

    We exclude any credits that were first converted to variable and then transferred, that logic is accounted for in the next block
 */


CREATE OR REPLACE TEMPORARY TABLE _giftco_original_credit AS
SELECT c.credit_key,
       COALESCE(sc2.store_credit_id, sc.store_credit_id) AS original_store_credit_id
FROM _dim_credit_key c
         JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt ON tt.store_credit_id = c.credit_id
         JOIN lake_consolidated.ultra_merchant.store_credit sc
              ON sc.store_credit_id::STRING = tt.source_reference_number -- original_store_credit_id
    -- get the credits that went a second time
    LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt2 ON tt2.store_credit_id = sc.store_credit_id
    AND tt2.credit_transfer_transaction_type_id IN (200, 210, 220)
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit sc2 ON sc2.store_credit_id = tt2.source_reference_number::STRING
WHERE c.credit_type ILIKE '%credit%'                            -- make sure it's credits
  AND tt.credit_transfer_transaction_type_id IN (200, 210, 220) -- originated as a credit & created as a credit
  AND tt.source_reference_number IS NOT NULL
  AND COALESCE(sc2.store_credit_id, sc.store_credit_id) -- make sure that the converted credit was NOT a credit that was converted to variable
    NOT IN
      (SELECT DISTINCT converted_store_credit_id FROM lake_consolidated.ultra_merchant.store_credit_conversion scc);



/************************************************************************************
 The below gets the original store credit ids for credits that were
        1. Converted from Fixed to Variable
        2. Sent to Giftco (Also captures the original if the credit was sent to giftco twice)
*************************************************************************************/

CREATE OR REPLACE TEMPORARY TABLE _conv_to_variable_plus_giftco_original_credit AS
SELECT before_variable_conversion.credit_key,
       scc2.original_store_credit_id AS original_store_credit_id
FROM (SELECT c.credit_key,
             COALESCE(sc2.store_credit_id, sc.store_credit_id) AS original_store_credit_id
      FROM _dim_credit_key c
               JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt ON tt.store_credit_id = c.credit_id
               JOIN lake_consolidated.ultra_merchant.store_credit sc
                    ON sc.store_credit_id::STRING = tt.source_reference_number -- original_store_credit_id
               LEFT JOIN lake_consolidated.ultra_merchant.membership_store_credit msc
                         ON msc.store_credit_id = sc.store_credit_id
               LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason scr
                         ON scr.store_credit_reason_id = sc.store_credit_reason_id
               LEFT JOIN lake_consolidated.ultra_merchant.statuscode s ON s.statuscode = sc.statuscode
          -- get the credits that went a second time
               LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt2
                         ON tt2.store_credit_id = sc.store_credit_id
                             AND tt2.credit_transfer_transaction_type_id IN (200, 210, 220)
               LEFT JOIN lake_consolidated.ultra_merchant.store_credit sc2 ON sc2.store_credit_id = tt2.source_reference_number::STRING
               LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason scr2
                         ON scr2.store_credit_reason_id = sc2.store_credit_reason_id
               LEFT JOIN lake_consolidated.ultra_merchant.membership_store_credit msc2
                         ON msc2.store_credit_id = sc2.store_credit_id
               LEFT JOIN lake_consolidated.ultra_merchant.statuscode s2 ON s2.statuscode = sc2.statuscode
      WHERE c.credit_type ILIKE '%credit%'                            -- make sure it's credits
        AND tt.credit_transfer_transaction_type_id IN (200, 210, 220) -- originated as a credit & created as a credit
        AND tt.source_reference_number IS NOT NULL
        AND COALESCE(sc2.store_credit_id, sc.store_credit_id) -- make sure that the converted credit was a credit that was converted to variable
          IN (SELECT DISTINCT converted_store_credit_id FROM lake_consolidated.ultra_merchant.store_credit_conversion scc)
     ) before_variable_conversion
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_conversion scc2
                   ON scc2.converted_store_credit_id = before_variable_conversion.original_store_credit_id;


CREATE OR REPLACE TEMP TABLE _giftco_store_credit_original_token AS
SELECT dc.credit_key,
       dc.credit_id,
       ctt.source_reference_number as original_token_id
FROM _dim_credit_key dc
JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
    on dc.credit_id = ctt.store_credit_id
WHERE ctt.credit_transfer_transaction_type_id = 270;

CREATE OR REPLACE TEMPORARY TABLE _giftco_store_credit_original_giftcertificate_ctt AS
SELECT dc.credit_key,
       dc.credit_id,
       ctt.source_reference_number AS original_gift_certificate_id
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
              ON dc.credit_id = ctt.store_credit_id
WHERE ctt.credit_transfer_transaction_type_id IN (250, 260)
AND dc.source_credit_id_type = 'store_credit_id';

CREATE OR REPLACE TEMPORARY TABLE _giftco_store_credit_original_giftcertificate_gsct AS
SELECT dc.credit_key,
       dc.credit_id,
       gcst.gift_certificate_id AS original_gift_certificate_id
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit gcst
              ON dc.credit_id = gcst.store_credit_id
WHERE dc.source_credit_id_type = 'store_credit_id'
    AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
                 WHERE dc.credit_id = ctt.store_credit_id
                   AND gcst.gift_certificate_id = ctt.source_reference_number);

CREATE OR REPLACE TEMPORARY TABLE _credit_origination AS
SELECT sc.credit_key,
       dcf.credit_key                               AS original_credit_key,
       dcf.credit_tender                            AS original_credit_tender,
       dcf.credit_type                              AS original_credit_type,
       dcf.credit_reason                            AS original_credit_reason,
       dcf.credit_issued_local_datetime             AS original_credit_issued_local_datetime,
       dcf.credit_state,
       dcf.credit_state_logic_source,
       'Converted To Variable And Giftco Roundtrip' AS original_credit_match_reason
FROM _conv_to_variable_plus_giftco_original_credit sc
         JOIN _dim_credit_key dcf ON sc.original_store_credit_id = dcf.credit_id AND
                                     dcf.credit_type ILIKE '%credit%'

UNION

SELECT gc.credit_key,
       dcf.credit_key                   AS original_credit_key,
       dcf.credit_tender                AS original_credit_tender,
       dcf.credit_type                  AS original_credit_type,
       dcf.credit_reason                AS original_credit_reason,
       dcf.credit_issued_local_datetime AS original_credit_issued_local_datetime,
       dcf.credit_state,
       dcf.credit_state_logic_source,
       'Giftco Roundtrip'               AS original_credit_match_reason
FROM _giftco_original_credit gc
         JOIN _dim_credit_key dcf ON gc.original_store_credit_id = dcf.credit_id AND
                                     dcf.credit_type ILIKE '%credit%'

UNION

SELECT -- only fixed credits get converted to variable
       dc.credit_key,
       dck.credit_key                   AS original_credit_key,
       dck.credit_tender                AS original_credit_tender,
       dck.credit_type                  AS original_credit_type,
       dck.credit_reason                AS original_credit_reason,
       dck.credit_issued_local_datetime AS original_credit_issued_local_datetime,
       dck.credit_state,
       dck.credit_state_logic_source,
       'Converted To Variable'          AS original_credit_match_reason
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.store_credit_conversion scc
              ON scc.converted_store_credit_id = dc.credit_id
         JOIN _dim_credit_key dck
              ON dck.credit_id = scc.original_store_credit_id
                  AND dck.credit_type ILIKE '%credit%'
         LEFT JOIN lake_consolidated.ultra_merchant.membership_store_credit msc
                   ON msc.store_credit_id = scc.original_store_credit_id
WHERE scc.store_credit_conversion_type_id = 1
  AND dc.credit_type ILIKE '%credit%'-- conversion to variable

UNION

SELECT -- only fixed credits were converted to tokens
       dc.credit_key,
       dck.credit_key                   AS original_credit_key,
       dck.credit_tender                AS original_credit_tender,
       dck.credit_type                  AS original_credit_type,
       dck.credit_reason                AS original_credit_reason,
       dck.credit_issued_local_datetime AS original_credit_issued_local_datetime,
       dck.credit_state,
       dck.credit_state_logic_source,
       'Converted To Token'             AS original_credit_match_reason
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion scc
              ON scc.converted_membership_token_id = dc.credit_id
         JOIN _dim_credit_key dck ON dck.credit_id = scc.original_store_credit_id AND dck.credit_type ILIKE '%credit%'
WHERE scc.store_credit_conversion_type_id = 2
  AND dc.credit_type = 'Token'
-- conversion to token

UNION

SELECT -- only variable credits to token
       dc.credit_key,
       dck2.credit_key                      original_credit_key,
       dck2.credit_tender                AS original_credit_tender,
       dck2.credit_type                  AS original_credit_type,
       dck2.credit_reason                AS original_credit_reason,
       dck2.credit_issued_local_datetime AS original_credit_issued_local_datetime,
       dck2.credit_state,
       dck2.credit_state_logic_source,
       'Converted To Variable To Token'  AS original_credit_match_reason
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion sctc
              ON sctc.converted_membership_token_id = dc.credit_id
         JOIN _dim_credit_key dck ON dck.credit_id = sctc.original_store_credit_id
         JOIN lake_consolidated.ultra_merchant.store_credit_conversion scc
              ON scc.converted_store_credit_id = dck.credit_id
         JOIN _dim_credit_key dck2
              ON dck2.credit_id = scc.original_store_credit_id
                  AND dck2.credit_type ILIKE '%credit%'
WHERE sctc.store_credit_conversion_type_id = 3
  AND dc.credit_type = 'Token'
  AND scc.store_credit_conversion_type_id = 1
  AND dck.credit_type ILIKE '%credit%'

UNION

SELECT -- only fixed credits were converted to tokens
       dc.credit_key,
       dck.credit_key                            AS original_credit_key,
       dck.credit_tender                         AS original_credit_tender,
       dck.credit_type                           AS original_credit_type,
       dck.credit_reason                         AS original_credit_reason,
       dck.credit_issued_local_datetime          AS original_credit_issued_local_datetime,
       dck.credit_state,
       dck.credit_state_logic_source,
       'Converted To Token And Giftco Roundtrip' AS original_credit_match_reason
FROM _giftco_store_credit_original_token dc
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion scc
              ON scc.converted_membership_token_id = dc.original_token_id
         JOIN _dim_credit_key dck ON dck.credit_id = scc.original_store_credit_id AND dck.credit_type ILIKE '%credit%'
WHERE scc.store_credit_conversion_type_id = 2

UNION ALL

SELECT -- only fixed credits were converted to tokens
       dc.credit_key,
       dck.credit_key                   AS original_credit_key,
       dck.credit_tender                AS original_credit_tender,
       dck.credit_type                  AS original_credit_type,
       dck.credit_reason                AS original_credit_reason,
       dck.credit_issued_local_datetime AS original_credit_issued_local_datetime,
       dck.credit_state,
       dck.credit_state_logic_source,
       'Converted To Credit'             AS original_credit_match_reason
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.membership_token_credit_conversion scc
              ON scc.converted_store_credit_id = dc.credit_id
         JOIN _dim_credit_key dck ON dck.credit_id = scc.original_membership_token_id AND dck.credit_type ILIKE '%token%'
WHERE scc.store_credit_conversion_type_id = 4
  AND dc.credit_type = 'Fixed Credit'

UNION ALL

SELECT DISTINCT dc.credit_key,
                dcf.credit_key                       AS original_credit_key,
                dcf.credit_tender                    AS original_credit_tender,
                dcf.credit_type                      AS original_credit_type,
                dcf.credit_reason                    AS original_credit_reason,
                dcf.credit_issued_local_datetime     AS original_credit_issued_local_datetime,
                dcf.credit_state,
                dcf.credit_state_logic_source,
                'Gift Card Converted to Store Credit'          AS original_credit_match_reason
FROM _giftco_store_credit_original_giftcertificate_ctt dc
    JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
              ON dc.credit_id = ctt.store_credit_id
         JOIN _dim_credit_key dcf ON dcf.credit_id = dc.original_gift_certificate_id
    AND dcf.source_credit_id_type = 'gift_certificate_id'

UNION ALL

SELECT DISTINCT dc.credit_key,
                dcf.credit_key                       AS original_credit_key,
                dcf.credit_tender                    AS original_credit_tender,
                dcf.credit_type                      AS original_credit_type,
                dcf.credit_reason                    AS original_credit_reason,
                dcf.credit_issued_local_datetime     AS original_credit_issued_local_datetime,
                dcf.credit_state,
                dcf.credit_state_logic_source,
                'Gift Card Converted to Store Credit'          AS original_credit_match_reason
FROM _giftco_store_credit_original_giftcertificate_gsct dc
    JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit gcst
              ON dc.credit_id = gcst.store_credit_id
         JOIN _dim_credit_key dcf ON dcf.credit_id = dc.original_gift_certificate_id
    AND dcf.source_credit_id_type = 'gift_certificate_id'
;


------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _dim_credit_final AS
SELECT dc.credit_key,
       dc.credit_id,
       dc.meta_original_credit_id,
       dc.source_credit_id_type,
       dc.store_id,
       dc.customer_id,
       credit_type,
       credit_tender,
       credit_reason,
       dc.credit_issuance_reason,
       credit_order_id,
       credit_amount,
       credit_issued_hq_datetime,
       credit_issued_local_datetime,
       administrator_id,
       COALESCE(ca.original_credit_key, dc.credit_key)               AS original_credit_key,
       COALESCE(ca.original_credit_match_reason, 'Already Original') AS original_credit_match_reason,
       COALESCE(ca.original_credit_tender, dc.credit_tender)         AS original_credit_tender,
       COALESCE(ca.original_credit_type, dc.credit_type)             AS original_credit_type,
       COALESCE(ca.original_credit_reason, dc.credit_reason)         AS original_credit_reason,
       COALESCE(ca.credit_state, dc.credit_state)                    AS credit_state,
       CASE
           WHEN ca.credit_state_logic_source IN ('Billing Address', 'Shipping Address')
               AND ca.credit_state_logic_source != dc.credit_state_logic_source
               THEN 'Original Credit ' || ca.credit_state_logic_source
           ELSE dc.credit_state_logic_source END                     AS credit_state_logic_source,
       CAST('Unknown' AS TEXT)                                       AS credit_report_mapping,
       CAST('Unknown' AS TEXT)                                       AS credit_report_sub_mapping,
       COALESCE(ca.original_credit_issued_local_datetime,
                dc.credit_issued_local_datetime)                     AS original_credit_issued_local_datetime,
    FALSE AS is_cancelled_before_token_conversion
FROM _dim_credit_key dc
         LEFT JOIN _credit_origination ca ON ca.credit_key = dc.credit_key;

DELETE FROM _dim_credit_final AS dcf
USING edw_prod.reference.test_customer AS tc
WHERE tc.customer_id = dcf.customer_id
AND dcf.original_credit_type = 'Giftcard'
AND dcf.original_credit_tender = 'Cash'
AND dcf.original_credit_reason = 'Gift Card - Paid';

/* getting credit value at the time of original issuance */
CREATE OR REPLACE TEMP TABLE _dim_credit__equivalent_value AS
SELECT DISTINCT dcf.credit_key,
                dcf.customer_id,
                dcf.store_id,
                dcf.original_credit_issued_local_datetime,
                m.price AS equivalent_credit_value_local_amount
FROM _dim_credit_final AS dcf
         LEFT JOIN lake_consolidated.ultra_merchant_history.membership AS m
                   ON m.customer_id = dcf.customer_id
                       AND IFF(m.store_id = 41, 26, m.store_id) = dcf.store_id
                       AND CONVERT_TIMEZONE('America/Los_Angeles',
                                            dcf.original_credit_issued_local_datetime) BETWEEN m.effective_start_datetime AND m.effective_end_datetime;

CREATE OR REPLACE TEMP TABLE _dim_credit__null_equivalent_value AS
SELECT DISTINCT dcf.credit_key,
                m.price                                                      AS equivalent_credit_value_local_amount,
                ABS(DATEDIFF('millisecond',
                             CONVERT_TIMEZONE('America/Los_Angeles', dcf.original_credit_issued_local_datetime),
                             m.effective_start_datetime))                    AS date_diff,
                RANK() OVER (PARTITION BY credit_key ORDER BY date_diff) AS closest_membership_date
FROM _dim_credit__equivalent_value AS dcf
         JOIN lake_consolidated.ultra_merchant_history.membership AS m
              ON m.customer_id = dcf.customer_id AND
                 IFF(m.store_id = 41, 26, m.store_id) = dcf.store_id
WHERE dcf.equivalent_credit_value_local_amount IS NULL
    QUALIFY closest_membership_date = 1;

UPDATE _dim_credit__equivalent_value ev
SET ev.equivalent_credit_value_local_amount = nev.equivalent_credit_value_local_amount
FROM _dim_credit__null_equivalent_value nev
WHERE ev.credit_key = nev.credit_key;

-- there are 28 credit keys with multiple credit origination keys. The below code prioritizes credits with same store id
-- and removes the ones with different store ids
CREATE OR REPLACE TEMP TABLE _deleted_credits AS
WITH _duplicate_parent_child_keys AS (
    SELECT credit_key, store_id, original_credit_key
    FROM _dim_credit_final
    WHERE credit_key IN (
        SELECT credit_key
        FROM _dim_credit_final
        GROUP BY credit_key
        HAVING count(*) > 1)
)
SELECT dpck.credit_key, dpck.original_credit_key, dpck.store_id
FROM _duplicate_parent_child_keys dpck
         JOIN _dim_credit_final dcf
                    ON dpck.original_credit_key = dcf.credit_key AND dpck.store_id <> dcf.store_id;

DELETE
FROM _dim_credit_final f
    USING _deleted_credits dc
WHERE f.credit_key = dc.credit_key
  AND f.original_credit_key = dc.original_credit_key
  AND f.store_id = dc.store_id;

-- 29 credits that look like they were expired credits originally causing un-necessary combos
UPDATE _dim_credit_final
SET credit_reason          = 'Gift Card Redemption (Gift Certificate)',
    original_credit_reason = 'Gift Card Redemption (Gift Certificate)'
WHERE credit_reason = 'Gift Card Redemption (Expired Credit)'
  AND original_credit_key = credit_key;

/* 573 credits were wrongfully converted to token. They were supposed to be canceled, but source did not cancel them and they got converted during EMP migration (DA-28668) */
UPDATE _dim_credit_final AS t
SET t.is_cancelled_before_token_conversion = TRUE
FROM edw_prod.reference.credit_cancellation_datetime AS s
WHERE s.credit_id = t.credit_id
    AND s.source_credit_id_type = t.source_credit_id_type
    AND s.credit_issuance_reason = 'Credit To Token Conversion';

/****************************************************************************
  For reporting purposes, we use the combination of:
    ORIGINAL_CREDIT_TENDER
    ORIGINAL_CREDIT_TYPE
    ORIGINAL_CREDIT_REASON
    ORIGINAL_CREDIT_MATCH_REASON

  to update the CREDIT_REPORT_MAPPING and CREDIT_REPORT_SUB_MAPPING

 ****************************************************************************/

-- update the CREDIT_REPORT_MAPPING AND CREDIT_REPORT_SUB_MAPPING fields:
UPDATE _dim_credit_final
SET credit_report_mapping     = r.report_mapping,
    credit_report_sub_mapping = r.report_sub_mapping
FROM edw_prod.reference.credit_report_mapping r
WHERE r.original_credit_type = _dim_credit_final.original_credit_type
  AND _dim_credit_final.original_credit_reason = r.original_credit_reason
  AND _dim_credit_final.original_credit_tender = r.original_credit_tender
  AND _dim_credit_final.original_credit_match_reason = r.original_credit_match_reason;

CREATE OR REPLACE TEMPORARY TABLE _giftco_transfer_key AS
SELECT dcf.original_credit_key
FROM lake_consolidated_view.ultra_merchant.membership_token_transaction mtt
         JOIN _dim_credit_final dcf
              ON mtt.membership_token_id = dcf.credit_id
WHERE mtt.membership_token_transaction_type_id = 50
AND credit_type='Token';

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
FROM _dim_credit_final dcf
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN edw_prod.stg.dim_customer dc
                   ON dcf.customer_id = dc.customer_id
         LEFT JOIN _giftco_transfer_key gtk
                   ON dcf.original_credit_key = gtk.original_credit_key;

CREATE OR REPLACE TEMPORARY TABLE _deferred_recognition_label_credit AS
SELECT dcf.credit_key,
       dcf.credit_state,
       dcf.credit_issued_hq_datetime,
       st.store_country,
       CASE
           WHEN st.store_country != 'US' THEN 'Recognize'
           WHEN dcf.credit_state IN ('AUS', 'CAN') THEN 'Recognize'
           WHEN LOWER(dcf.credit_state) = 'ky' AND dcf.credit_issued_hq_datetime >= '2015-07-14' THEN 'Recognize'
           WHEN sm.giftco_recognition IS NOT NULL THEN sm.giftco_recognition
           ELSE 'Cant Recognize' END AS deferred_recognition_label_credit
FROM _dim_credit_final dcf
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN edw_prod.reference.credit_state_mapping sm ON LOWER(sm.state_abbreviation) = LOWER(dcf.credit_state);

CREATE OR REPLACE TEMP TABLE _updated_credit_state AS
SELECT dcf.credit_key,
       COALESCE(z1.zip, z2.zip, z3.zip)                                                       AS updated_zip,
       UPPER(COALESCE(z1.state, z2.state, z3.state, a1.state, a2.state, a3.state, 'Unknown')) AS updated_state
FROM _dim_credit_final dcf
     JOIN edw_prod.data_model.dim_store st ON dcf.store_id = st.store_id
     LEFT JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON dcf.credit_order_id = o.order_id
     LEFT JOIN lake_consolidated_view.ultra_merchant.address a1 ON a1.address_id = o.billing_address_id
     LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state z1 ON SUBSTR(a1.zip, 1, 5) = z1.zip
     LEFT JOIN lake_consolidated_view.ultra_merchant.address a2 ON a2.address_id = o.shipping_address_id
     LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state z2 ON SUBSTR(a2.zip, 1, 5) = z2.zip
     LEFT JOIN lake_consolidated_view.ultra_merchant.customer cd ON cd.customer_id = dcf.customer_id
     LEFT JOIN lake_consolidated_view.ultra_merchant.address a3 ON a3.address_id = cd.default_address_id
     LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state z3 ON SUBSTR(a3.zip, 1, 5) = z3.zip
     LEFT JOIN _giftco_transfer_status gts
           ON dcf.credit_key = gts.credit_key
     LEFT JOIN _deferred_recognition_label_credit drlc
           ON dcf.credit_key = drlc.credit_key
WHERE st.store_country = 'US'
  AND UPPER(dcf.credit_state) NOT IN (SELECT DISTINCT state_abbreviation FROM edw_prod.reference.credit_state_mapping)
  AND deferred_recognition_label_credit = 'Cant Recognize'
  AND giftco_transfer_status <> 'Membership to Giftco'
  AND original_credit_tender = 'Cash'
  AND ((original_credit_type = 'Fixed Credit' AND original_credit_reason = 'Membership Credit') OR
       (original_credit_type = 'Token' AND original_credit_reason = 'Token Billing'));

CREATE OR REPLACE TEMP TABLE _updated_deferred_recognition_label_credit AS
SELECT d.credit_key,
       d.credit_state,
       d.credit_issued_hq_datetime,
       d.store_country,
       CASE
           WHEN updated_state IN ('NOV')
               THEN 'NS'
           WHEN updated_state IN ('NEW')
               THEN 'NB'
           WHEN updated_state IN ('--')
               THEN 'CA'
           WHEN updated_state IN ('SAS')
               THEN 'SK'
           WHEN updated_state IN ('YUK')
               THEN 'YT'
           WHEN updated_state IN ('ONT')
               THEN 'OT'
           WHEN updated_state IN ('PRI')
               THEN 'PE'
           WHEN updated_state IN ('ALB')
               THEN 'AB'
           WHEN updated_state IN ('BRI')
               THEN 'BC'
           WHEN updated_state IN ('QUE')
               THEN 'QC'
           ELSE updated_state
           END                                      AS updated_state,
       COALESCE(giftco_recognition, d.deferred_recognition_label_credit) AS deferred_recognition_label_credit
FROM _deferred_recognition_label_credit d
     LEFT JOIN _updated_credit_state ucs
           ON d.credit_key = ucs.credit_key
     LEFT JOIN edw_prod.reference.credit_state_mapping csm
           ON UPPER(TRIM(ucs.updated_state)) = csm.state_abbreviation;

UPDATE _updated_deferred_recognition_label_credit
SET deferred_recognition_label_credit =
        CASE
            WHEN updated_state IN ('No State Expected', 'CAN', 'AUS', 'AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'OT',
                                   'NU', 'ON', 'PE', 'QC', 'SK', 'YT', 'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT',
                                   'NT', 'NSW', 'CA', 'WES', 'SOU')
                THEN 'Recognize'
            WHEN LOWER(credit_state) = 'ky' AND credit_issued_hq_datetime >= '2015-07-14' THEN 'Recognize'
            WHEN store_country != 'US' THEN 'Recognize'
            ELSE deferred_recognition_label_credit END
WHERE credit_key IN (SELECT credit_key FROM _updated_credit_state);

/****************************************************************************
  Calculate the to USD exchange rate & vat rate (based on credit issuance date)
 ****************************************************************************/

-- putting exchange rates to convert source currency to USD by day into a table
CREATE OR REPLACE TEMPORARY TABLE _exchange_rate AS
SELECT src_currency,
       dd.full_date,
       er.exchange_rate
FROM edw_prod.reference.currency_exchange_rate er
         JOIN edw_prod.data_model.dim_date dd ON er.effective_start_datetime <= dd.full_date
    AND er.effective_end_datetime > dd.full_date
WHERE dest_currency = 'USD'
  AND dd.full_date <= CURRENT_DATE();

-- putting vat rates into a temp table
CREATE OR REPLACE TEMPORARY TABLE _vat_rate AS
SELECT REPLACE(country_code, 'GB', 'UK') AS country_code,
       dd.full_date,
       er.rate
FROM edw_prod.reference.vat_rate_history er
         JOIN edw_prod.data_model.dim_date dd ON er.start_date <= dd.full_date
    AND er.expires_date >= dd.full_date
WHERE dd.full_date <= CURRENT_DATE() + 7;

------------------------------------------------------------------------
-- insert into final DIM CREDIT TABLE

CREATE OR REPLACE TRANSIENT TABLE shared.dim_credit AS
SELECT dcf.credit_key,
       dcf.store_id,
       dcf.credit_id,
       dcf.meta_original_credit_id,
       dcf.source_credit_id_type,
       dcf.administrator_id,
       dcf.customer_id,
       dcf.credit_report_mapping,
       dcf.credit_report_sub_mapping,
       dcf.credit_type,
       dcf.credit_tender,
       dcf.credit_reason,
       dcf.credit_issuance_reason,
       dcf.credit_order_id,
       dcf.credit_issued_hq_datetime,
       dcf.credit_issued_local_datetime,
       dcf.original_credit_key,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_issued_local_datetime,
       dcf.original_credit_reason,
       dcf.original_credit_match_reason,
       udrlc.deferred_recognition_label_credit,
       oudrlc.deferred_recognition_label_credit      AS original_deferred_recognition_label_credit,
       'Recognize'                                   AS deferred_recognition_label_token,
       dcf.credit_state,
       dcf.credit_state_logic_source,
       IFNULL(exch.exchange_rate, 1)                 AS credit_issued_usd_conversion_rate,
       dcf.credit_amount                             AS credit_issued_local_gross_vat_amount,
       IFNULL(IFF(dcf.credit_type = 'Token', 1 ,ROUND(dcf.credit_amount / NULLIF(eqv.equivalent_credit_value_local_amount, 0), 4)), 1)    AS credit_issued_equivalent_count,
       dcf.credit_amount / (1 + IFNULL(vrh.rate, 0)) AS credit_issued_local_amount,
       dcf.is_cancelled_before_token_conversion,
       gts.giftco_transfer_status,
       c.first_gender AS original_gender,
       CURRENT_TIMESTAMP()                           AS meta_create_datetime,
       CURRENT_TIMESTAMP()                           AS meta_update_datetime
FROM _dim_credit_final dcf
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
         LEFT JOIN _giftco_transfer_status gts
                   ON dcf.credit_key = gts.credit_key
         LEFT JOIN _updated_deferred_recognition_label_credit udrlc
                   ON dcf.credit_key = udrlc.credit_key
         LEFT JOIN _updated_deferred_recognition_label_credit oudrlc
                   ON dcf.original_credit_key = oudrlc.credit_key
         LEFT JOIN _dim_credit__equivalent_value AS eqv
                   ON eqv.credit_key = dcf.credit_key
         LEFT JOIN edw_prod.stg.dim_customer AS c
                   ON c.customer_id = dcf.customer_id
         LEFT JOIN _exchange_rate exch
                   ON exch.src_currency = st.store_currency -- add in exchange rate to convert to USD
                       AND CAST(dcf.credit_issued_hq_datetime AS DATE) = full_date
         LEFT JOIN _vat_rate vrh -- join so you can get the vat rate
                   ON vrh.country_code = st.store_country
                       AND vrh.full_date = CAST(dcf.credit_issued_hq_datetime AS DATE);

ALTER TABLE shared.dim_credit SET DATA_RETENTION_TIME_IN_DAYS = 0;
