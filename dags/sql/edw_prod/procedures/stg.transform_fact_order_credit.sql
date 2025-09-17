/*
-- get all orders from fact_order that has any sort of credit redemptions in lake_consolidated.ultra_merchant.ORDER_CREDIT
-- there are 3 types of credits: Credits, Tokens & Gift Certificates
    -- ORDER_CREDIT has one per order w/ credit redemption per credit redeemed.
    -- If its a credit, STORE_CREDIT_ID will be populated
    -- If the redemption is a token, MEMBERSHIP_TOKEN_ID will be populated
    -- If the redemption is a gift certificate directly redeemed, GIFT_CERTIFICATE_ID will be populated
 */

SET target_table = 'stg.fact_order_credit';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table,NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

SET wm_edw_stg_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
SET wm_lake_ultra_merchant_order_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_credit'));
SET wm_lake_ultra_merchant_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit'));
SET wm_edw_stg_dim_credit = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_credit'));
SET wm_lake_ultra_merchant_gift_certificate = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_certificate'));
SET wm_lake_history_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.membership'));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

CREATE OR REPLACE TEMP TABLE _fact_order_credit__order_base (order_id INT) CLUSTER BY (order_id);
INSERT INTO _fact_order_credit__order_base (order_id)
-- Full Refresh
SELECT DISTINCT oc.order_id
FROM stg.fact_order AS o
    JOIN lake_consolidated.ultra_merchant.order_credit AS oc
        ON oc.order_id = o.order_id
WHERE $is_full_refresh = TRUE
UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.order_id
FROM (
    SELECT o.order_id
    FROM stg.fact_order AS o
        JOIN lake_consolidated.ultra_merchant.order_credit AS oc
            ON oc.order_id = o.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit AS sc
            ON sc.store_credit_id = oc.store_credit_id
    WHERE o.meta_update_datetime > $wm_edw_stg_fact_order
        OR oc.meta_update_datetime > $wm_lake_ultra_merchant_order_credit
        OR sc.meta_update_datetime > $wm_lake_ultra_merchant_store_credit
    UNION ALL
    SELECT oc.order_id
    FROM lake_consolidated.ultra_merchant.order_credit AS oc
        JOIN stg.dim_credit AS dc
            ON dc.credit_id = COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id)
    WHERE dc.meta_update_datetime > $wm_edw_stg_dim_credit
    UNION ALL
    SELECT oc.order_id
    FROM lake_consolidated.ultra_merchant.order_credit AS oc
        JOIN lake_consolidated.ultra_merchant.gift_certificate gc
            ON gc.gift_certificate_id = COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id)
    WHERE gc.meta_update_datetime > $wm_lake_ultra_merchant_gift_certificate
    UNION ALL
    SELECT order_id
    FROM lake_consolidated.ultra_merchant.order_credit
    WHERE COALESCE(store_credit_id, membership_token_id, gift_certificate_id) IS NULL AND
          meta_update_datetime > $wm_lake_ultra_merchant_order_credit
    UNION ALL
    SELECT fo.order_id
         FROM stg.fact_order AS fo
                JOIN lake_consolidated.ultra_merchant_history.membership AS m
                    ON fo.customer_id = m.customer_id
    WHERE m.meta_update_datetime > $wm_lake_history_ultra_merchant_membership
    UNION ALL /* previously errored rows */
    SELECT order_id
    FROM excp.fact_order_credit
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh;

/* making sure we refresh the child and master for dropship splits */
INSERT INTO _fact_order_credit__order_base (order_id)
SELECT DISTINCT fo.master_order_id AS order_id
FROM _fact_order_credit__order_base AS base
    JOIN stg.fact_order AS fo
        ON fo.order_id = base.order_id
WHERE fo.order_status_key = 8 -- dropship child
    AND fo.master_order_id IS NOT NULL
    AND $is_full_refresh = FALSE
    AND NOT EXISTS (
        SELECT ob.order_id
        FROM _fact_order_credit__order_base AS ob
        WHERE ob.order_id = fo.master_order_id
    );


INSERT INTO _fact_order_credit__order_base (order_id)
SELECT DISTINCT fo.order_id
FROM _fact_order_credit__order_base AS base
    JOIN stg.fact_order AS fo
        ON fo.master_order_id = base.order_id
WHERE fo.order_status_key = 8 -- dropship child
    AND $is_full_refresh = FALSE
    AND NOT EXISTS (
        SELECT ob.order_id
        FROM _fact_order_credit__order_base AS ob
        WHERE ob.order_id = fo.order_id
    );


-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', current_warehouse()) FROM _fact_order_credit__order_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

CREATE OR REPLACE TEMP TABLE _fact_order_credit__dropship_splits AS
SELECT
    fo.order_id, fo.master_order_id
FROM _fact_order_credit__order_base AS ob
    JOIN stg.fact_order AS fo
        ON fo.order_id = ob.order_id
WHERE fo.order_status_key = 8; -- dropship child

-- either credit, gift_certificate_id or membership_token_id is not null (actually has a redemption)
CREATE OR REPLACE TEMP TABLE _fact_order_credit__credits AS
SELECT
    fo.order_id,
    fo.meta_original_order_id,
    fo.customer_id,
    fo.store_id,
    fo.order_local_datetime,
    COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id) AS credit_id,
    CASE
        WHEN oc.store_credit_id IS NOT NULL THEN 'Credit'
        WHEN oc.membership_token_id IS NOT NULL THEN 'Token'
        WHEN oc.gift_certificate_id IS NOT NULL THEN 'Gift Certificate'
        END AS credit_type,
    oc.amount AS redeemed_gross_vat_local_amount,
    oc.amount / (1 + COALESCE(fo.effective_vat_rate, 0)) AS redeemed_local_amount,
    fo.is_test_customer,
    dosc.is_test_order
FROM _fact_order_credit__order_base AS base
    JOIN stg.fact_order AS fo
        ON fo.order_id = base.order_id
    JOIN stg.dim_order_sales_channel AS dosc
        ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    LEFT JOIN lake_consolidated.ultra_merchant.order_credit AS oc
        ON oc.order_id = fo.order_id
WHERE COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id) IS NOT NULL
  AND oc.hvr_is_deleted = FALSE
  AND fo.is_deleted = FALSE;
-- select * from _fact_order_credit__credits

INSERT INTO _fact_order_credit__credits
SELECT ds.master_order_id,
       stg.udf_unconcat_brand(ds.master_order_id) AS meta_original_order_id,
       c.customer_id,
       c.store_id,
       c.order_local_datetime,
       c.credit_id,
       c.credit_type,
       c.redeemed_gross_vat_local_amount,
       c.redeemed_local_amount,
       c.is_test_customer,
       c.is_test_order
FROM _fact_order_credit__dropship_splits AS ds
    JOIN _fact_order_credit__credits AS c
        ON c.order_id = ds.order_id
WHERE ds.master_order_id IS NOT NULL
AND NOT EXISTS (
        SELECT c.order_id
        FROM _fact_order_credit__credits AS c
        WHERE c.order_id = ds.master_order_id
        );

-- use dim_credit dimension to get credit specific dimensions that we will then map to the credit categories for reporting
CREATE OR REPLACE TEMP TABLE _fact_order_credit__credit_id AS
SELECT DISTINCT
    dcf.credit_id,
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
    dcf.original_credit_issued_local_datetime as credit_issued_local_datetime,
    credit_report_mapping,
    credit_report_sub_mapping
FROM stg.dim_credit AS dcf
    JOIN _fact_order_credit__credits AS c
        ON c.credit_id = dcf.credit_id
        AND new_credit_type = c.credit_type
UNION ALL
SELECT DISTINCT
    gc.gift_certificate_id,
    'Gift Certificate' AS new_credit_type,
    'Gift Certificate' AS original_credit_type,
    'Gift Certificate' AS credit_reason,
    IFF(gc.order_id IS NOT NULL OR NVL(gc.gift_certificate_type_id,0) = 8, 'Cash', 'NonCash') AS credit_tender,
    'Already Original' AS original_credit_match_reason,
    gc.amount,
    gc.datetime_added::TIMESTAMP_TZ AS credit_issued_local_datetime, /* should convert to local */
    IFF(credit_tender = 'Cash', 'Other Cash Credit', 'NonCash Credit') AS credit_report_mapping,
    IFF(credit_tender = 'Cash', 'Other Cash Credit - Direct Gift Card', 'NonCash Credit - Direct Gift Card') AS credit_report_sub_mapping
FROM lake_consolidated.ultra_merchant.gift_certificate AS gc
    JOIN _fact_order_credit__credits AS c
        ON c.credit_id = gc.gift_certificate_id
WHERE gc.gift_certificate_id NOT IN (SELECT credit_id FROM stg.dim_credit WHERE source_credit_id_type = 'gift_certificate_id');
-- SELECT * FROM _fact_order_credit__credit_id

CREATE OR REPLACE TEMP TABLE _fact_order_credit__redeemed_equivalent_count AS
SELECT DISTINCT
    c.order_id,
    c.customer_id,
    ci.credit_id,
    ci.credit_issued_local_datetime,
    c.redeemed_gross_vat_local_amount,
    IFF(c.credit_type = 'Token', 1, c.redeemed_gross_vat_local_amount / m.price) AS billed_cash_credit_redeemed_equivalent_count
FROM _fact_order_credit__credits AS c
    JOIN _fact_order_credit__credit_id ci
    ON c.credit_id = ci.credit_id AND
       c.credit_type = ci.new_credit_type AND
       ci.credit_report_mapping = 'Billed Credit'
    LEFT JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = c.customer_id
        AND ci.credit_issued_local_datetime BETWEEN m.effective_start_datetime AND m.effective_end_datetime;
-- SELECT * FROM _fact_order_credit__redeemed_equivalent_count;

CREATE OR REPLACE TEMP TABLE _fact_order_credit__redeemed_equivalent_count_null AS
SELECT DISTINCT
    c.order_id,
    c.credit_id,
    c.customer_id,
    c.redeemed_gross_vat_local_amount / m.price AS n_billed_cash_credit_redeemed_equivalent_count,
    ABS(DATEDIFF('millisecond',c.credit_issued_local_datetime,m.effective_start_datetime)) AS date_diff,
    RANK() OVER(PARTITION BY c.credit_id ORDER BY date_diff) AS closest_membership_date
FROM _fact_order_credit__redeemed_equivalent_count AS c
    JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = c.customer_id
WHERE c.billed_cash_credit_redeemed_equivalent_count IS NULL
QUALIFY closest_membership_date = 1;

UPDATE _fact_order_credit__redeemed_equivalent_count ec
SET ec.billed_cash_credit_redeemed_equivalent_count = ecn.n_billed_cash_credit_redeemed_equivalent_count
FROM _fact_order_credit__redeemed_equivalent_count_null ecn
WHERE ec.order_id = ecn.order_id
  AND ec.credit_id = ecn.credit_id;

CREATE OR REPLACE TEMP TABLE _fact_order_credit__redeemed_equivalent_count_agg AS
SELECT order_id,
       SUM(IFNULL(billed_cash_credit_redeemed_equivalent_count, 1)) AS billed_cash_credit_redeemed_equivalent_count
FROM _fact_order_credit__redeemed_equivalent_count
GROUP BY order_id;

CREATE OR REPLACE TEMP TABLE _fact_order_credit__stg AS
SELECT
    c.order_id,
    c.meta_original_order_id,
    c.is_test_customer,
    c.is_test_order,
    IFNULL(rec.billed_cash_credit_redeemed_equivalent_count, 0) AS billed_cash_credit_redeemed_equivalent_count, -- this metric is already aggregated above
    SUM(c.redeemed_local_amount) AS credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'NonCash Credit',
        c.redeemed_local_amount, 0)) AS noncash_credit_redeemed_local_amount,
    SUM(IFF(frm.original_credit_tender = 'Cash',
        c.redeemed_local_amount, 0)) AS cash_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Billed Credit',
        c.redeemed_local_amount, 0)) AS billed_cash_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Refund Credit',
        c.redeemed_local_amount, 0)) AS refund_cash_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit',
        c.redeemed_local_amount, 0)) AS other_cash_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Billed Credit'
        AND DATE_TRUNC(MONTH, frm.credit_issued_local_datetime)::DATE = DATE_TRUNC(MONTH, c.order_local_datetime)::DATE,
        c.redeemed_local_amount, 0)) AS billed_cash_credit_redeemed_same_month_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Billed Credit'
        AND frm.credit_report_sub_mapping = 'Billed Credit - Credit',
        c.redeemed_local_amount, 0)) AS billed_cash_credit_sub_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Billed Credit'
        AND frm.credit_report_sub_mapping = 'Billed Credit - Credit Converted To Token',
        c.redeemed_local_amount, 0)) AS billed_cash_credit_sub_converted_to_token_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Billed Credit'
        AND frm.credit_report_sub_mapping = 'Billed Credit - Credit Converted To Variable',
        c.redeemed_local_amount, 0)) AS billed_cash_credit_sub_converted_to_variable_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Billed Credit'
        AND frm.credit_report_sub_mapping = 'Billed Credit - Credit Giftco Roundtrip',
        c.redeemed_local_amount, 0)) AS billed_cash_credit_sub_giftco_roundtrip_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Billed Credit'
        AND frm.credit_report_sub_mapping = 'Billed Credit - Token',
        c.redeemed_local_amount, 0)) AS billed_cash_credit_sub_token_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit'
        AND frm.credit_report_sub_mapping = 'Other Cash Credit - Direct Gift Card',
        c.redeemed_local_amount, 0)) AS other_cash_credit_sub_direct_gift_card_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit'
        AND frm.credit_report_sub_mapping = 'Other Cash Credit - Gift Card',
        c.redeemed_local_amount, 0)) AS other_cash_credit_sub_gift_card_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit'
        AND frm.credit_report_sub_mapping = 'Other Cash Credit - Legacy Credit',
        c.redeemed_local_amount, 0)) AS other_cash_credit_sub_legacy_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Other Cash Credit'
        AND frm.credit_report_sub_mapping = 'Other Cash Credit - Other Credit',
        c.redeemed_local_amount, 0)) AS other_cash_credit_sub_other_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Refund Credit'
        AND frm.credit_report_sub_mapping = 'Refund Credit - Credit', c.redeemed_local_amount,
            0)) AS refund_cash_credit_sub_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'Refund Credit' AND frm.credit_report_sub_mapping = 'Refund Credit - Token',
            c.redeemed_local_amount, 0)) AS refund_cash_credit_sub_token_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'NonCash Credit'
        AND frm.credit_report_sub_mapping = 'NonCash Credit - Credit',
        c.redeemed_local_amount, 0)) AS noncash_credit_sub_credit_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'NonCash Credit'
        AND frm.credit_report_sub_mapping = 'NonCash Credit - Direct Gift Card',
        c.redeemed_local_amount, 0)) AS noncash_credit_sub_direct_gift_card_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'NonCash Credit'
        AND frm.credit_report_sub_mapping = 'NonCash Credit - Gift Card',
        c.redeemed_local_amount, 0)) AS noncash_credit_sub_gift_card_redeemed_local_amount,
    SUM(IFF(frm.credit_report_mapping = 'NonCash Credit'
        AND frm.credit_report_sub_mapping = 'NonCash Credit - Token',
        c.redeemed_local_amount, 0)) AS noncash_credit_sub_token_redeemed_local_amount,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_order_credit__credits AS c
    LEFT JOIN _fact_order_credit__credit_id AS frm
        ON frm.credit_id = c.credit_id
        AND frm.new_credit_type = c.credit_type
    LEFT JOIN _fact_order_credit__redeemed_equivalent_count_agg rec
        ON c.order_id = rec.order_id
    LEFT JOIN data_model.dim_store AS st
        ON st.store_id = c.store_id
GROUP BY c.order_id, c.meta_original_order_id, c.is_test_customer, c.is_test_order, rec.billed_cash_credit_redeemed_equivalent_count;
-- SELECT * FROM _fact_order_credit__stg

INSERT INTO stg.fact_order_credit_stg (
    order_id,
    meta_original_order_id,
    credit_redeemed_local_amount,
    noncash_credit_redeemed_local_amount,
    cash_credit_redeemed_local_amount,
    billed_cash_credit_redeemed_local_amount,
    refund_cash_credit_redeemed_local_amount,
    other_cash_credit_redeemed_local_amount,
    billed_cash_credit_redeemed_same_month_local_amount,
    billed_cash_credit_redeemed_equivalent_count,
    billed_cash_credit_sub_credit_redeemed_local_amount,
    billed_cash_credit_sub_converted_to_token_redeemed_local_amount,
    billed_cash_credit_sub_converted_to_variable_redeemed_local_amount,
    billed_cash_credit_sub_giftco_roundtrip_redeemed_local_amount,
    billed_cash_credit_sub_token_redeemed_local_amount,
    other_cash_credit_sub_direct_gift_card_redeemed_local_amount,
    other_cash_credit_sub_gift_card_redeemed_local_amount,
    other_cash_credit_sub_legacy_credit_redeemed_local_amount,
    other_cash_credit_sub_other_credit_redeemed_local_amount,
    refund_cash_credit_sub_credit_redeemed_local_amount,
    refund_cash_credit_sub_token_redeemed_local_amount,
    noncash_credit_sub_credit_redeemed_local_amount,
    noncash_credit_sub_direct_gift_card_redeemed_local_amount,
    noncash_credit_sub_gift_card_redeemed_local_amount,
    noncash_credit_sub_token_redeemed_local_amount,
    is_test_customer,
    is_test_order,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    order_id,
    meta_original_order_id,
    credit_redeemed_local_amount,
    noncash_credit_redeemed_local_amount,
    cash_credit_redeemed_local_amount,
    billed_cash_credit_redeemed_local_amount,
    refund_cash_credit_redeemed_local_amount,
    other_cash_credit_redeemed_local_amount,
    billed_cash_credit_redeemed_same_month_local_amount,
    billed_cash_credit_redeemed_equivalent_count,
    billed_cash_credit_sub_credit_redeemed_local_amount,
    billed_cash_credit_sub_converted_to_token_redeemed_local_amount,
    billed_cash_credit_sub_converted_to_variable_redeemed_local_amount,
    billed_cash_credit_sub_giftco_roundtrip_redeemed_local_amount,
    billed_cash_credit_sub_token_redeemed_local_amount,
    other_cash_credit_sub_direct_gift_card_redeemed_local_amount,
    other_cash_credit_sub_gift_card_redeemed_local_amount,
    other_cash_credit_sub_legacy_credit_redeemed_local_amount,
    other_cash_credit_sub_other_credit_redeemed_local_amount,
    refund_cash_credit_sub_credit_redeemed_local_amount,
    refund_cash_credit_sub_token_redeemed_local_amount,
    noncash_credit_sub_credit_redeemed_local_amount,
    noncash_credit_sub_direct_gift_card_redeemed_local_amount,
    noncash_credit_sub_gift_card_redeemed_local_amount,
    noncash_credit_sub_token_redeemed_local_amount,
    is_test_customer,
    is_test_order,
    FALSE,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_order_credit__stg
ORDER BY
    order_id;

/* Handle Deletions. This can't be done in with the MERGE because sometimes an Order ID might have deleted and
non-deleted store credit records in um.store_credit.
For Dropship Splits:
   1) if all child orders are deleted, then we will also flag the master as deleted
   2) master orders don't have a credit, token or gift_cert id populated in order_credit, we need to make sure masters aren't deleted because of this */

 CREATE OR REPLACE TEMP TABLE _fact_order_credit__deleted_master_orders AS
SELECT a.order_id
FROM (
    SELECT ds.master_order_id AS order_id,
             COUNT(ds.order_id) AS child_order_count,
             SUM(IFF(COALESCE(fo.order_id, oc.order_id) IS NOT NULL, 1, 0)) AS deleted_child_order_count
      FROM _fact_order_credit__dropship_splits AS ds
               LEFT JOIN stg.fact_order AS fo
                         ON fo.order_id = ds.order_id
                             AND fo.is_deleted = TRUE
               LEFT JOIN lake_consolidated.ultra_merchant.order_credit AS oc
                         ON oc.order_id = ds.order_id
                             AND oc.hvr_is_deleted = TRUE
      GROUP BY 1
      ) AS a
WHERE EQUAL_NULL(a.child_order_count, deleted_child_order_count);


CREATE OR REPLACE TEMP TABLE _fact_order_credit__delete AS
SELECT base.order_id,
       SUM(CASE
        WHEN m.master_order_id IS NULL AND COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id) IS NULL THEN 0
        WHEN oc.hvr_is_deleted = TRUE THEN 0
        WHEN fo.is_deleted = TRUE THEN 0
        WHEN dmo.order_id IS NOT NULL THEN 0
        ELSE 1 END ) AS Rn
FROM _fact_order_credit__order_base AS base
         JOIN lake_consolidated.ultra_merchant.order_credit AS oc
                   ON oc.order_id = base.order_id
         JOIN stg.fact_order AS fo
                    ON fo.order_id = base.order_id
        LEFT JOIN (SELECT DISTINCT master_order_id FROM _fact_order_credit__dropship_splits WHERE master_order_id IS NOT NULL) AS m
                    ON m.master_order_id = base.order_id
        LEFT JOIN _fact_order_credit__deleted_master_orders dmo ON base.order_id = dmo.order_id
GROUP BY base.order_id
HAVING Rn = 0;

UPDATE stg.fact_order_credit foc
SET foc.is_deleted = TRUE,
    foc.meta_update_datetime = $execution_start_time
FROM _fact_order_credit__delete focd
WHERE  foc.order_id = focd.order_id AND
      foc.is_deleted = FALSE;
