------------------------------------------------------------------------
------------------------------------------------------------------------
/*
-- get all orders from fact_order that has any sort of credit redemptions in lake_consolidated.ULTRA_MERCHANT.ORDER_CREDIT

-- there are 3 types of credits: Credits, Tokens & Gift Certificates
    -- ORDER_CREDIT has one per order w/ credit redemption per credit redeemed.
    -- If its a credit, STORE_CREDIT_ID will be populated
    -- If the redemption is a token, MEMBERSHIP_TOKEN_ID will be populated
    -- If the redemption is a gift certificate directly redeemed, GIFT_CERTIFICATE_ID will be populated
 */

CREATE OR REPLACE TEMPORARY TABLE _credits AS
SELECT fo.order_id,
       edw_prod.stg.udf_unconcat_brand(fo.order_id) AS meta_original_order_id,
       fo.customer_id,
       fo.store_id,
       fo.order_local_datetime,
       COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id) AS credit_id,
       CASE
           WHEN oc.store_credit_id IS NOT NULL THEN 'Credit'
           WHEN oc.membership_token_id IS NOT NULL THEN 'Token'
           WHEN oc.gift_certificate_id IS NOT NULL THEN 'Gift Certificate'
           END                                                                      AS credit_type,
       oc.amount                                                                    AS redeemed_gross_vat_local_amount,
       oc.amount / (1 + COALESCE(fo.effective_vat_rate, 0))                         AS redeemed_local_amount,
       fo.order_status_key,
       fo.master_order_id
FROM edw_prod.data_model.fact_order fo
         LEFT JOIN lake_consolidated.ultra_merchant.order_credit oc ON oc.order_id = fo.order_id
WHERE oc.hvr_is_deleted = 0  -- excluding deletes
  AND credit_id IS NOT NULL;
-- either credit, gift_certificate_id or membership_token_id is not null (actually has a redemption)

INSERT INTO _credits
SELECT c.master_order_id,
       edw_prod.stg.udf_unconcat_brand(c.master_order_id) AS meta_original_order_id,
       c.customer_id,
       c.store_id,
       c.order_local_datetime,
       c.credit_id,
       c.credit_type,
       c.redeemed_gross_vat_local_amount,
       c.redeemed_local_amount,
       -1 AS order_status_key,
       NULL AS master_order_id
FROM  _credits AS c
WHERE c.master_order_id IS NOT NULL
  AND c.order_status_key = 8
AND NOT EXISTS (
        SELECT c2.order_id
        FROM _credits AS c2
        WHERE c2.order_id = c.master_order_id
        );

CREATE OR REPLACE TEMP TABLE _test_cash_giftcard AS
SELECT c.credit_id, c.credit_type
FROM _credits AS c
    JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
        ON gc.gift_certificate_id = c.credit_id
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = gc.order_id
    JOIN edw_prod.reference.test_customer AS tc
        ON tc.customer_id = COALESCE(gc.customer_id, o.customer_id)
WHERE c.credit_type = 'Gift Certificate'
AND (gc.order_id IS NOT NULL OR gc.gift_certificate_type_id = 8);

INSERT INTO _test_cash_giftcard (credit_id, credit_type)
SELECT
    c.credit_id, c.credit_type
FROM _credits AS c
    JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcsc
        ON gcsc.store_credit_id = c.credit_id
    JOIN edw_prod.reference.test_customer AS tc
        ON tc.customer_id = c.customer_id
WHERE c.credit_type = 'Credit'
AND NOT EXISTS (
    SELECT 1
    FROM _test_cash_giftcard AS tcg
    WHERE tcg.credit_id = c.credit_id
    AND tcg.credit_type = c.credit_type
);

-- deleting credits originating from a cash giftcard
DELETE FROM _credits AS c
USING _test_cash_giftcard AS tcg
WHERE tcg.credit_id = c.credit_id
AND c.credit_type = tcg.credit_type;

-- use our dim_credit dimensions to get credit specific dimensions that we will then map to the credit categories for reporting
CREATE OR REPLACE TEMPORARY TABLE _credit_ids AS
SELECT DISTINCT dcf.credit_id,
                CASE
                    WHEN dcf.source_credit_id_type ILIKE '%credit%' THEN 'Credit'
                    WHEN dcf.source_credit_id_type = 'gift_certificate_id' THEN 'Gift Certificate'
                    ELSE dcf.credit_type
                END AS new_credit_type,
                dcf.original_credit_type,
                dcf.original_credit_reason,
                dcf.original_credit_tender,
                dcf.original_credit_match_reason,
                dcf.credit_issued_local_amount,
                dcf.original_credit_issued_local_datetime AS credit_issued_local_datetime,
                credit_report_mapping,
                credit_report_sub_mapping
FROM shared.dim_credit dcf
UNION ALL
SELECT DISTINCT gc.gift_certificate_id,
                'Gift Certificate'                               AS new_credit_type,
                'Gift Certificate'                               AS original_credit_type,
                'Gift Certificate'                               AS credit_reason,
                IFF(gc.order_id IS NOT NULL OR NVL(gc.gift_certificate_type_id,0) = 8, 'Cash', 'NonCash')  AS credit_tender,
                'Already Original'                               AS original_credit_match_reason,
                gc.amount,
                gc.datetime_added::TIMESTAMP_TZ                  AS credit_issued_local_datetime, -- should convert to local
                CASE
                    WHEN credit_tender = 'Cash' THEN 'Other Cash Credit'
                    ELSE 'NonCash Credit' END                    AS credit_report_mapping,
                CASE
                    WHEN credit_tender = 'Cash' THEN 'Other Cash Credit - Direct Gift Card'
                    ELSE 'NonCash Credit - Direct Gift Card' END AS credit_report_sub_mapping
FROM lake_consolidated.ultra_merchant.gift_certificate gc
WHERE gc.gift_certificate_id NOT IN (SELECT credit_id FROM shared.dim_credit WHERE source_credit_id_type = 'gift_certificate_id');


CREATE OR REPLACE TEMP TABLE _credit__redeemed_equivalent_count AS
SELECT DISTINCT
    c.order_id,
    c.customer_id,
    ci.credit_id,
    ci.credit_issued_local_datetime,
    c.redeemed_gross_vat_local_amount,
    IFF(ci.new_credit_type = 'Token', 1, c.redeemed_gross_vat_local_amount / m.price) AS billed_cash_credit_redeemed_equivalent_count
FROM _credits AS c
    JOIN _credit_ids ci
    ON c.credit_id = ci.credit_id AND
       c.credit_type = ci.new_credit_type AND
       ci.credit_report_mapping = 'Billed Credit'
    LEFT JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = c.customer_id
        AND ci.credit_issued_local_datetime BETWEEN m.effective_start_datetime AND m.effective_end_datetime;


CREATE OR REPLACE TEMP TABLE _credit__redeemed_equivalent_count_null AS
SELECT DISTINCT
    c.order_id,
    c.credit_id,
    c.customer_id,
    c.redeemed_gross_vat_local_amount / m.price AS n_billed_cash_credit_redeemed_equivalent_count,
    ABS(DATEDIFF('millisecond',c.credit_issued_local_datetime,m.effective_start_datetime)) AS date_diff,
    RANK() OVER(PARTITION BY c.credit_id ORDER BY date_diff) AS closest_membership_date
FROM _credit__redeemed_equivalent_count AS c
    JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = c.customer_id
WHERE c.billed_cash_credit_redeemed_equivalent_count IS NULL
QUALIFY closest_membership_date = 1;

UPDATE _credit__redeemed_equivalent_count ec
SET ec.billed_cash_credit_redeemed_equivalent_count = ecn.n_billed_cash_credit_redeemed_equivalent_count
FROM _credit__redeemed_equivalent_count_null ecn
WHERE ec.order_id = ecn.order_id
  AND ec.credit_id = ecn.credit_id;

CREATE OR REPLACE TEMP TABLE _credit__redeemed_equivalent_count_agg AS
SELECT order_id,
       SUM(IFNULL(billed_cash_credit_redeemed_equivalent_count, 1)) AS billed_cash_credit_redeemed_equivalent_count
FROM _credit__redeemed_equivalent_count
GROUP BY order_id;

-- create final order table with combos
CREATE OR REPLACE TRANSIENT TABLE shared.fact_order_credit AS
SELECT c.order_id,
       c.meta_original_order_id,

       SUM(c.redeemed_local_amount) AS                                            credit_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'NonCash Credit', c.redeemed_local_amount,
               0)) AS                                                             noncash_credit_redeemed_local_amount,
       SUM(IFF(frm.original_credit_tender = 'Cash', redeemed_local_amount, 0)) AS cash_credit_redeemed_local_amount,

       SUM(IFF(frm.credit_report_mapping = 'Billed Credit', c.redeemed_local_amount,
               0)) AS                                                             billed_cash_credit_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Refund Credit', c.redeemed_local_amount,
               0)) AS                                                             refund_cash_credit_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit', c.redeemed_local_amount,
               0)) AS                                                             other_cash_credit_redeemed_local_amount,

       SUM(IFF(frm.credit_report_mapping = 'Billed Credit' AND
               DATE_TRUNC(MONTH, credit_issued_local_datetime)::DATE = DATE_TRUNC(MONTH, c.order_local_datetime)::DATE,
               c.redeemed_local_amount,
               0)) AS                                                             billed_cash_credit_redeemed_same_month_local_amount,
       IFNULL(eqv.billed_cash_credit_redeemed_equivalent_count,0) AS              billed_cash_credit_redeemed_equivalent_count,

       SUM(IFF(frm.credit_report_mapping = 'Billed Credit' AND frm.credit_report_sub_mapping = 'Billed Credit - Credit',
               c.redeemed_local_amount,
               0)) AS                                                             billed_cash_credit_sub_credit_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Billed Credit' AND
               frm.credit_report_sub_mapping = 'Billed Credit - Credit Converted To Token', c.redeemed_local_amount,
               0)) AS                                                             billed_cash_credit_sub_converted_to_token_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Billed Credit' AND
               frm.credit_report_sub_mapping = 'Billed Credit - Credit Converted To Variable', c.redeemed_local_amount,
               0)) AS                                                             billed_cash_credit_sub_converted_to_variable_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Billed Credit' AND
               frm.credit_report_sub_mapping = 'Billed Credit - Credit Giftco Roundtrip', c.redeemed_local_amount,
               0)) AS                                                             billed_cash_credit_sub_gifco_roundtrip_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Billed Credit' AND frm.credit_report_sub_mapping = 'Billed Credit - Token',
               c.redeemed_local_amount,
               0)) AS                                                             billed_cash_credit_sub_token_redeemed_local_amount,

       SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit' AND
               frm.credit_report_sub_mapping = 'Other Cash Credit - Direct Gift Card', c.redeemed_local_amount,
               0)) AS                                                             other_cash_credit_sub_direct_gift_card_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit' AND
               frm.credit_report_sub_mapping = 'Other Cash Credit - Gift Card', c.redeemed_local_amount,
               0)) AS                                                             other_cash_credit_sub_gift_card_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit' AND
               frm.credit_report_sub_mapping = 'Other Cash Credit - Legacy Credit', c.redeemed_local_amount,
               0)) AS                                                             other_cash_credit_sub_legacy_credit_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit' AND
               frm.credit_report_sub_mapping = 'Other Cash Credit - Other Credit', c.redeemed_local_amount,
               0)) AS                                                             other_cash_credit_sub_other_credit_redeemed_local_amount,

       SUM(IFF(frm.credit_report_mapping = 'Refund Credit' AND frm.credit_report_sub_mapping = 'Refund Credit - Credit',
               c.redeemed_local_amount,
               0)) AS                                                             refund_cash_credit_sub_credit_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'Refund Credit' AND frm.credit_report_sub_mapping = 'Refund Credit - Token',
               c.redeemed_local_amount,
               0)) AS                                                             refund_cash_credit_sub_token_redeemed_local_amount,

       SUM(IFF(frm.credit_report_mapping = 'NonCash Credit' AND
               frm.credit_report_sub_mapping = 'NonCash Credit - Credit', c.redeemed_local_amount,
               0)) AS                                                             noncash_credit_sub_credit_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'NonCash Credit' AND
               frm.credit_report_sub_mapping = 'NonCash Credit - Direct Gift Card', c.redeemed_local_amount,
               0)) AS                                                             noncash_credit_sub_direct_gift_card_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'NonCash Credit' AND
               frm.credit_report_sub_mapping = 'NonCash Credit - Gift Card', c.redeemed_local_amount,
               0)) AS                                                             noncash_credit_sub_gift_card_redeemed_local_amount,
       SUM(IFF(frm.credit_report_mapping = 'NonCash Credit' AND
               frm.credit_report_sub_mapping = 'NonCash Credit - Token', c.redeemed_local_amount,
               0)) AS                                                             noncash_credit_sub_token_redeemed_local_amount,

       CURRENT_TIMESTAMP() AS                                                     meta_create_datetime,
       CURRENT_TIMESTAMP() AS                                                     meta_update_datetime
FROM edw_prod.data_model.fact_order fo
         LEFT JOIN _credits c ON c.order_id = fo.order_id
         LEFT JOIN _credit_ids frm ON c.credit_id = frm.credit_id
    AND frm.new_credit_type = c.credit_type
         JOIN edw_prod.data_model.dim_store st ON st.store_id = c.store_id
         LEFT JOIN _credit__redeemed_equivalent_count_agg AS eqv ON eqv.order_id = fo.order_id
GROUP BY c.order_id, c.meta_original_order_id, IFNULL(eqv.billed_cash_credit_redeemed_equivalent_count,0);

ALTER TABLE shared.fact_order_credit SET DATA_RETENTION_TIME_IN_DAYS = 0;
