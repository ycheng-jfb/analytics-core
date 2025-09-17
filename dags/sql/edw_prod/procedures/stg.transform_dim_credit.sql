SET target_table = 'stg.dim_credit';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

-- Full Refresh on every Sunday 9:15 run
SET is_full_refresh = (SELECT IFF(DATE_PART('weekday', CURRENT_DATE) = 0 AND HOUR(CURRENT_TIME) = 9, TRUE, $is_full_refresh));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_view_ultra_merchant_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit'));
SET wm_lake_view_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer'));
SET wm_lake_history_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.customer'));
SET wm_lake_history_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.membership'));
SET wm_lake_view_ultra_merchant_store_credit_reason = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit_reason'));
SET wm_lake_view_ultra_merchant_membership_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_store_credit'));
SET wm_lake_view_ultra_merchant_refund_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.refund_store_credit'));
SET wm_lake_view_ultra_merchant_refund = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.refund'));
SET wm_lake_view_ultra_merchant_gift_certificate_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_certificate_store_credit'));
SET wm_lake_view_ultra_merchant_gift_certificate = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_certificate'));
SET wm_lake_view_ultra_merchant_store_credit_conversion = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit_conversion'));
SET wm_lake_view_ultra_merchant_credit_transfer_transaction = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.credit_transfer_transaction'));
SET wm_lake_view_ultra_merchant_membership_token = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token'));
SET wm_lake_view_ultra_merchant_membership_token_reason = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token_reason'));
SET wm_lake_view_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_lake_view_ultra_merchant_order_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_credit'));
SET wm_lake_view_ultra_merchant_address = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.address'));
SET wm_lake_view_ultra_merchant_membership_billing = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_billing'));
SET wm_lake_view_ultra_merchant_bounceback_endowment = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.bounceback_endowment'));
SET wm_edw_reference_test_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.test_customer'));
SET wm_lake_view_ultra_merchant_membership_token_credit_conversion = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token_credit_conversion'));

/*
SELECT
    $wm_self,
    $wm_lake_view_ultra_merchant_store_credit,
    $wm_lake_view_ultra_merchant_customer,
    $wm_lake_history_ultra_merchant_customer,
    $wm_lake_history_ultra_merchant_membership,
    $wm_lake_view_ultra_merchant_store_credit_reason,
    $wm_lake_view_ultra_merchant_membership_store_credit,
    $wm_lake_view_ultra_merchant_refund_store_credit,
    $wm_lake_view_ultra_merchant_refund,
    $wm_lake_view_ultra_merchant_gift_certificate_store_credit,
    $wm_lake_view_ultra_merchant_gift_certificate,
    $wm_lake_view_ultra_merchant_store_credit_conversion,
    $wm_lake_view_ultra_merchant_credit_transfer_transaction,
    $wm_lake_view_ultra_merchant_membership_token,
    $wm_lake_view_ultra_merchant_membership_token_reason,
    $wm_lake_view_ultra_merchant_order,
    $wm_lake_view_ultra_merchant_order_credit,
    $wm_lake_view_ultra_merchant_address,
    $wm_lake_view_ultra_merchant_membership_billing,
    $wm_edw_reference_test_customer,
    $wm_lake_view_ultra_merchant_membership_token_credit_conversion;
*/

CREATE OR REPLACE TEMP TABLE _dim_credit__credit_base (credit_id INT);

-- Full Refresh
INSERT INTO _dim_credit__credit_base (credit_id)
SELECT DISTINCT f.credit_id
FROM (SELECT store_credit_id AS credit_id
      FROM lake_consolidated.ultra_merchant.store_credit
      UNION ALL
      SELECT membership_token_id AS credit_id
      FROM lake_consolidated.ultra_merchant.membership_token
      UNION ALL
      SELECT gc.gift_certificate_id AS credit_id
      FROM lake_consolidated.ultra_merchant.gift_certificate AS gc
      WHERE gc.gift_certificate_type_id = 9
         OR gc.order_id IS NOT NULL) AS f
WHERE $is_full_refresh = TRUE
ORDER BY f.credit_id;

-- Incremental Refresh
INSERT INTO _dim_credit__credit_base (credit_id)
SELECT DISTINCT incr.credit_id
FROM (
    -- Self-check for manual updates
    SELECT credit_id
    FROM stg.dim_credit
    WHERE meta_update_datetime > $wm_self

    UNION ALL
    -- Check for dependency table updates
    SELECT sc.store_credit_id AS credit_id
    FROM lake_consolidated.ultra_merchant.store_credit AS sc
        LEFT JOIN lake_consolidated.ultra_merchant.customer AS c
            ON c.customer_id = sc.customer_id
        LEFT JOIN (
            SELECT
                ch.customer_id,
                MAX(ch.meta_update_datetime) AS meta_update_datetime
            FROM lake_consolidated.ultra_merchant_history.customer AS ch
            WHERE ch.meta_update_datetime > $wm_lake_history_ultra_merchant_customer
            GROUP BY ch.customer_id
            ) AS ch
            ON ch.customer_id = sc.customer_id
        LEFT JOIN (
            SELECT
                ms.customer_id,
                MAX(ms.meta_update_datetime) AS meta_update_datetime
            FROM lake_consolidated.ultra_merchant_history.membership AS ms
            WHERE ms.meta_update_datetime > $wm_lake_history_ultra_merchant_membership
            GROUP BY ms.customer_id
            ) AS ms
            ON ms.customer_id = sc.customer_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
            ON scr.store_credit_reason_id = sc.store_credit_reason_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_store_credit AS msc
            ON sc.store_credit_id = msc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.refund_store_credit AS rsc
            ON rsc.store_credit_id = sc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.refund AS ref
            ON ref.payment_transaction_id = sc.store_credit_id
            AND ref.payment_method = 'store_credit'
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcs
            ON gcs.store_credit_id = sc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
            ON gc.gift_certificate_id = gcs.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_conversion AS sccc
            ON sccc.converted_store_credit_id = sc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction AS tt2
            ON tt2.store_credit_id = sc.store_credit_id
            AND tt2.credit_transfer_transaction_type_id IN (200, 210, 220, 250, 270)
        LEFT JOIN lake_consolidated.ultra_merchant.membership_billing AS mb
            ON mb.order_id = msc.order_id
            AND mb.membership_billing_source_id IN (5,6)
        LEFT JOIN lake_consolidated.ultra_merchant.order_credit AS oc
            ON oc.store_credit_id = sc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token_credit_conversion mtcc
            ON mtcc.converted_store_credit_id = sc.store_credit_id
    WHERE (sc.meta_update_datetime > $wm_lake_view_ultra_merchant_store_credit
        OR ch.meta_update_datetime > $wm_lake_history_ultra_merchant_customer
        OR c.meta_update_datetime > $wm_lake_view_ultra_merchant_customer
        OR ms.meta_update_datetime > $wm_lake_history_ultra_merchant_membership
        OR scr.meta_update_datetime > $wm_lake_view_ultra_merchant_store_credit_reason
        OR msc.meta_update_datetime > $wm_lake_view_ultra_merchant_membership_store_credit
        OR rsc.meta_update_datetime > $wm_lake_view_ultra_merchant_refund_store_credit
        OR ref.meta_update_datetime > $wm_lake_view_ultra_merchant_refund
        OR gcs.meta_update_datetime > $wm_lake_view_ultra_merchant_gift_certificate_store_credit
        OR gc.meta_update_datetime > $wm_lake_view_ultra_merchant_gift_certificate
        OR sccc.meta_update_datetime > $wm_lake_view_ultra_merchant_store_credit_conversion
        OR tt2.meta_update_datetime > $wm_lake_view_ultra_merchant_credit_transfer_transaction
        OR mb.meta_update_datetime > $wm_lake_view_ultra_merchant_membership_billing
        OR oc.meta_update_datetime > $wm_lake_view_ultra_merchant_order_credit
        OR mtcc.meta_update_datetime > $wm_lake_view_ultra_merchant_membership_token_credit_conversion)

    UNION ALL

    SELECT mt.membership_token_id AS credit_id
    FROM lake_consolidated.ultra_merchant.membership_token AS mt
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token_reason AS mtr
            ON mtr.membership_token_reason_id = mt.membership_token_reason_id
        LEFT JOIN (
            SELECT
                m.membership_id,
                MAX(m.meta_update_datetime) AS meta_update_datetime
            FROM lake_consolidated.ultra_merchant_history.membership AS m
            WHERE m.meta_update_datetime > $wm_lake_history_ultra_merchant_membership
            GROUP BY m.membership_id
            ) AS m
            ON m.membership_id = mt.membership_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_billing AS mb
            ON mb.order_id = mt.order_id
            AND mb.membership_billing_source_id IN (5,6)
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token_credit_conversion mtccl
            ON mtccl.original_membership_token_id = mt.membership_token_id
    WHERE (mt.meta_update_datetime > $wm_lake_view_ultra_merchant_membership_token
        OR mtr.meta_update_datetime > $wm_lake_view_ultra_merchant_membership_token_reason
        OR m.meta_update_datetime > $wm_lake_history_ultra_merchant_membership
        OR mb.meta_update_datetime > $wm_lake_view_ultra_merchant_membership_billing
        OR mtccl.meta_update_datetime > $wm_lake_view_ultra_merchant_membership_token_credit_conversion)

    UNION ALL

    SELECT dc.credit_id
    FROM stg.dim_credit AS dc
        LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.order_id = dc.credit_order_id
        LEFT JOIN lake_consolidated.ultra_merchant.address AS a1
            ON a1.address_id = o.billing_address_id
        LEFT JOIN lake_consolidated.ultra_merchant.address AS a2
            ON a2.address_id = o.shipping_address_id
        LEFT JOIN lake_consolidated.ultra_merchant.customer AS cd
            ON cd.customer_id = o.customer_id -- original credit customer default address (usually same customer_id)
        LEFT JOIN (
            SELECT
                ch.customer_id,
                MAX(ch.meta_update_datetime) AS meta_update_datetime
            FROM lake_consolidated.ultra_merchant_history.customer AS ch
            WHERE ch.meta_update_datetime > $wm_lake_history_ultra_merchant_customer
            GROUP BY ch.customer_id
            ) AS ch
            ON ch.customer_id = cd.customer_id
        LEFT JOIN lake_consolidated.ultra_merchant.address AS a3
            ON a3.address_id = cd.default_address_id
    WHERE (o.meta_update_datetime > $wm_lake_view_ultra_merchant_order
        OR a1.meta_update_datetime > $wm_lake_view_ultra_merchant_address
        OR a2.meta_update_datetime > $wm_lake_view_ultra_merchant_address
        OR cd.meta_update_datetime > $wm_lake_view_ultra_merchant_customer
        OR ch.meta_update_datetime > $wm_lake_history_ultra_merchant_customer
        OR a3.meta_update_datetime > $wm_lake_view_ultra_merchant_address)

    UNION ALL

    SELECT gc.gift_certificate_id AS credit_id
    FROM lake_consolidated.ultra_merchant.gift_certificate AS gc
             LEFT JOIN lake_consolidated.ultra_merchant.bounceback_endowment be
                       ON gc.gift_certificate_id = be.object_id
                           AND be.object = 'gift_certificate'
             LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
                       ON gc.gift_certificate_id = ctt.source_reference_number
             LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o
                       ON gc.order_id = o.order_id
             LEFT JOIN (SELECT m.customer_id,
                               MAX(m.meta_update_datetime) AS meta_update_datetime
                        FROM lake_consolidated.ultra_merchant_history.membership m
                        WHERE m.meta_update_datetime > $wm_lake_history_ultra_merchant_membership
                        GROUP BY m.customer_id) ms
                       ON COALESCE(gc.customer_id, o.customer_id) = ms.customer_id
             LEFT JOIN (SELECT c.customer_id,
                               MAX(c.meta_update_datetime) AS meta_update_datetime
                        FROM lake_consolidated.ultra_merchant_history.customer c
                        WHERE c.meta_update_datetime > $wm_lake_history_ultra_merchant_customer
                        GROUP BY c.customer_id) ch
                       ON COALESCE(gc.customer_id, o.customer_id) = ch.customer_id
    WHERE (gc.gift_certificate_type_id = 9 OR gc.order_id IS NOT NULL)
      AND (gc.meta_update_datetime > $wm_lake_view_ultra_merchant_gift_certificate
        OR be.meta_update_datetime > $wm_lake_view_ultra_merchant_bounceback_endowment
        OR ctt.meta_update_datetime > $wm_lake_view_ultra_merchant_credit_transfer_transaction
        OR o.meta_update_datetime > $wm_lake_view_ultra_merchant_order
        OR ms.meta_update_datetime > $wm_lake_history_ultra_merchant_membership
        OR ch.meta_update_datetime > $wm_lake_history_ultra_merchant_customer
        )

    UNION ALL

    SELECT dc.credit_id
    FROM stg.dim_credit dc
        JOIN reference.test_customer tc
            ON dc.customer_id = tc.customer_id
    WHERE tc.meta_update_datetime > $wm_edw_reference_test_customer

    UNION ALL

    -- Previously errored rows
    SELECT credit_id
    FROM excp.dim_credit
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.credit_id;


SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', CURRENT_WAREHOUSE()) FROM _dim_credit__credit_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

-- Some of the credit ids have parent-child relationship. We identify these relationships in the _credit_origination part of the code. We asssociate
-- pairs by using original credit key field. If the above watermark process captures a child credit record, we should process its parent as well, just to be safe.
CREATE OR REPLACE TEMP TABLE _credit_base__original_credits AS
SELECT original_credit_id
FROM (
SELECT COALESCE(sc2.store_credit_id, sc.store_credit_id) AS original_credit_id
FROM _dim_credit__credit_base dccb
         JOIN lake_consolidated.ultra_merchant.store_credit c
              ON dccb.credit_id = c.store_credit_id
         JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt ON tt.store_credit_id = c.store_credit_id
         JOIN lake_consolidated.ultra_merchant.store_credit sc
              ON sc.store_credit_id::STRING = tt.source_reference_number -- original_store_credit_id
    -- get the credits that went a second time
         LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt2 ON tt2.store_credit_id = sc.store_credit_id
    AND tt2.credit_transfer_transaction_type_id IN (200, 210, 220)
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit sc2 ON sc2.store_credit_id = tt2.source_reference_number::STRING
  WHERE tt.credit_transfer_transaction_type_id IN (200, 210, 220) -- originated as a credit & created as a credit
  AND tt.source_reference_number IS NOT NULL
  AND COALESCE(sc2.store_credit_id, sc.store_credit_id) -- make sure that the converted credit was NOT a credit that was converted to variable
    NOT IN (SELECT DISTINCT converted_store_credit_id FROM lake_consolidated.ultra_merchant.store_credit_conversion scc)

UNION ALL

SELECT scc2.original_store_credit_id AS original_credit_id
FROM (SELECT COALESCE(sc2.store_credit_id, sc.store_credit_id) AS original_store_credit_id
      FROM _dim_credit__credit_base c
               JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt ON tt.store_credit_id = c.credit_id
               JOIN lake_consolidated.ultra_merchant.store_credit sc
                    ON sc.store_credit_id::STRING = tt.source_reference_number -- original_store_credit_id
          -- get the credits that went a second time
               LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt2
                         ON tt2.store_credit_id = sc.store_credit_id
                             AND tt2.credit_transfer_transaction_type_id IN (200, 210, 220)
               LEFT JOIN lake_consolidated.ultra_merchant.store_credit sc2 ON sc2.store_credit_id = tt2.source_reference_number::STRING
      WHERE tt.credit_transfer_transaction_type_id IN (200, 210, 220) -- originated as a credit & created as a credit
        AND tt.source_reference_number IS NOT NULL
        AND COALESCE(sc2.store_credit_id, sc.store_credit_id) -- make sure that the converted credit was a credit that was converted to variable
          IN (SELECT DISTINCT converted_store_credit_id FROM lake_consolidated.ultra_merchant.store_credit_conversion scc)
     ) before_variable_conversion
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_conversion scc2
                   ON scc2.converted_store_credit_id = before_variable_conversion.original_store_credit_id

UNION ALL

-- only fixed credits get converted to variable
SELECT scc.original_store_credit_id AS original_credit_id
FROM _dim_credit__credit_base dc
         JOIN lake_consolidated.ultra_merchant.store_credit_conversion scc
              ON scc.converted_store_credit_id = dc.credit_id
WHERE scc.store_credit_conversion_type_id = 1


UNION ALL

-- only fixed credits were converted to tokens
SELECT scc.original_store_credit_id AS original_credit_id
FROM _dim_credit__credit_base dc
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion scc
              ON scc.converted_membership_token_id = dc.credit_id
WHERE scc.store_credit_conversion_type_id = 2


UNION ALL

-- fluk credits got converted from fixed to variable to token in may-2022
-- getting original fixed credit_id
SELECT scc.original_store_credit_id AS original_credit_id
FROM _dim_credit__credit_base dc
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion sctc
              ON sctc.converted_membership_token_id = dc.credit_id
         JOIN lake_consolidated.ultra_merchant.store_credit_conversion scc
              ON scc.converted_store_credit_id = sctc.original_store_credit_id
WHERE sctc.store_credit_conversion_type_id = 3 -- variable to token
AND scc.store_credit_conversion_type_id = 1 -- fixed to variable

UNION ALL
-- getting variable credit_id
SELECT sctc.original_store_credit_id AS original_credit_id
FROM _dim_credit__credit_base dc
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion sctc
              ON sctc.converted_membership_token_id = dc.credit_id
WHERE sctc.store_credit_conversion_type_id = 3 -- variable to token

UNION ALL

SELECT DISTINCT scc.original_store_credit_id AS original_credit_id
FROM _dim_credit__credit_base dc
    JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
    on dc.credit_id = ctt.store_credit_id and ctt.credit_transfer_transaction_type_id = 270
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion scc
            ON scc.converted_membership_token_id = dc.credit_id
WHERE scc.store_credit_conversion_type_id = 2

UNION ALL
-- credits converted to tokens and back to credits
SELECT DISTINCT mtccl.original_store_credit_id
FROM _dim_credit__credit_base dc
         JOIN lake_consolidated.ultra_merchant.membership_token_credit_conversion mtccl
              ON mtccl.converted_store_credit_id = dc.credit_id
WHERE store_credit_conversion_type_id = 4
  AND original_store_credit_id IS NOT NULL

UNION ALL
-- gift certificates converted to store_credit_id
SELECT COALESCE(gcsc.gift_certificate_id, ctt.source_reference_number) AS original_store_credit
FROM _dim_credit__credit_base dc
         LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit gcsc
                   ON gcsc.store_credit_id = dc.credit_id
         LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
                   ON COALESCE(ctt.store_credit_id, ctt.gift_certificate_id) = dc.credit_id
                    AND ctt.credit_transfer_transaction_type_id IN (250, 260)
WHERE COALESCE(gcsc.gift_certificate_id, ctt.source_reference_number) IS NOT NULL
) WHERE NOT $is_full_refresh;


INSERT INTO _dim_credit__credit_base (credit_id)
SELECT DISTINCT original_credit_id
FROM _credit_base__original_credits
WHERE original_credit_id NOT IN (SELECT credit_id FROM _dim_credit__credit_base);

/*
Similar to above logic, if we process a parent credit, we should find and process its potential child credit as well. Reverse engineering the code above is
unnecessarily complex. However, we can still capture potential child credits with the below script. In reality, there can be 3 different scenarios:
1) parent credit doesn't have child. Then we don't need to worry.
2) parent credit has a child, but we never processed it before. In this case, it should be captured in the first part of the watermark process(standard across all of our transform scripts)
3) parent credit has a child, we processed it already. In this case, it is already in stg.dim_credit. We will just extract child's credit_id and add it to the base table so it will be processed with its parent.
*/

CREATE OR REPLACE TEMP TABLE _credit_base__child_credits AS
SELECT dc2.credit_id AS child_credit_id
FROM stg.dim_credit dc
         JOIN _dim_credit__credit_base dccb
              ON dc.credit_id = dccb.credit_id
         JOIN stg.dim_credit dc2
              ON dc2.original_credit_key = dc.credit_key
                     AND dc2.original_credit_key <> dc2.credit_key
WHERE NOT $is_full_refresh;


INSERT INTO _dim_credit__credit_base
SELECT DISTINCT child_credit_id FROM _credit_base__child_credits
WHERE child_credit_id NOT IN (SELECT credit_id FROM _dim_credit__credit_base);
/*
 the below snippet is added to address the below scenario:
                     credit 1            credit 2
 parent credit id    14(store 52)    |  50(store 26)
 child credit id    10(store 52)    |  14(store 26)

 In a case when we have credit id 10 in the base table, we would get 14 from the _credit_base__original_credits.
 So we would process 10, 14(store 52) and 14(store 26). However, the base table would miss 50. That would force the
 script to treat 14(store 26) as original instead of a child. Normally, we could address this issue with a recursion
 when  we populate the base table, however, it is pretty difficult to achieve this in Snowflake(need to pass a table to
 a javascript stored proc). Therefore, I have added the below snippet where we would use dim_credit itself
 to get the parent credits
 */

INSERT INTO _dim_credit__credit_base (credit_id)
SELECT DISTINCT dco.credit_id FROM stg.dim_credit dc
JOIN _dim_credit__credit_base base
ON dc.credit_id = base.credit_id
JOIN stg.dim_credit dco
ON dc.original_credit_key = dco.credit_key
WHERE NOT $is_full_refresh AND dco.credit_id NOT IN (SELECT credit_id FROM _dim_credit__credit_base);

-- in order_credit, whenever the amount is less than 0, the store_credit_id is actually an issuance
-- this happens when someone redeems a fixed credit and we find that we cannot fulfill the order for an individual item
-- since the credit is unbreakable, we cannot unwind the original credit, so we will issue a variable credit to make up the amount
-- in these cases, the store_credit_id is actual the new credit issued

CREATE OR REPLACE TEMPORARY TABLE _refund_partial_shipment AS
SELECT DISTINCT oc.store_credit_id
FROM lake_consolidated.ultra_merchant.order_credit oc
WHERE oc.amount < 0;


-- We want to tie the correct store_id to the credit_id at the time the credit was issued.  YITTY launch breaks this and now we have to use lake_history tables
CREATE OR REPLACE TEMP TABLE _ultra_merchant_membership AS
SELECT cb.credit_id, m.store_id, m.membership_id, m.customer_id, 'store_credit_id' AS source
FROM _dim_credit__credit_base AS cb
         JOIN lake_consolidated.ultra_merchant.store_credit AS sc
              ON cb.credit_id = sc.store_credit_id
         JOIN lake_consolidated.ultra_merchant_history.membership AS m
              ON sc.customer_id = m.customer_id
                  AND sc.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
GROUP BY cb.credit_id, m.store_id, m.membership_id, m.customer_id

UNION

SELECT cb.credit_id, m.store_id, m.membership_id, m.customer_id, 'token' AS source
FROM _dim_credit__credit_base AS cb
         JOIN lake_consolidated.ultra_merchant.membership_token AS mt
              ON cb.credit_id = mt.membership_token_id
         JOIN lake_consolidated.ultra_merchant_history.membership AS m
              ON m.membership_id = mt.membership_id
                  AND mt.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
GROUP BY cb.credit_id, m.store_id, m.membership_id, m.customer_id

UNION

SELECT cb.credit_id, m.store_id, m.membership_id, m.customer_id, 'bounceback' AS source
FROM _dim_credit__credit_base AS cb
         JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
              ON gc.gift_certificate_id = cb.credit_id
         JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
              ON be.object_id = gc.gift_certificate_id
                  AND be.object = 'gift_certificate'
         JOIN lake_consolidated.ultra_merchant_history.membership AS m
              ON m.customer_id = be.customer_id
                  AND be.datetime_added BETWEEN m.effective_start_datetime AND m.effective_end_datetime
UNION
SELECT cb.credit_id, m.store_id, m.membership_id, m.customer_id, 'giftcard' AS source
FROM _dim_credit__credit_base AS cb
         JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
              ON gc.gift_certificate_id = cb.credit_id
         LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o
                   ON gc.order_id = o.order_id
         LEFT JOIN lake_consolidated.ultra_merchant_history.membership AS m
                   ON m.customer_id = COALESCE(gc.customer_id, o.customer_id)
                       AND
                      COALESCE(gc.datetime_added, o.datetime_added) BETWEEN m.effective_start_datetime AND m.effective_end_datetime;


CREATE OR REPLACE TEMP TABLE _ultra_merchant_customer AS
SELECT cb.credit_id, c.store_id, c.customer_id, 'store_credit_id' as source
FROM _dim_credit__credit_base AS cb
    JOIN lake_consolidated.ultra_merchant.store_credit AS sc
        ON sc.store_credit_id = cb.credit_id
    JOIN lake_consolidated.ultra_merchant_history.customer AS c
        ON c.customer_id = sc.customer_id
WHERE sc.datetime_added BETWEEN c.effective_start_datetime AND c.effective_end_datetime
GROUP BY cb.credit_id, c.store_id, c.customer_id

UNION ALL

SELECT cb.credit_id, c.store_id, c.customer_id, 'token' as source
FROM _dim_credit__credit_base AS cb
    JOIN lake_consolidated.ultra_merchant.membership_token AS mt
        ON cb.credit_id = mt.membership_token_id
    JOIN lake_consolidated.ultra_merchant.membership m
        ON mt.membership_id = m.membership_id
    JOIN lake_consolidated.ultra_merchant_history.customer AS c
        ON c.customer_id = m.customer_id
WHERE mt.datetime_added BETWEEN c.effective_start_datetime AND c.effective_end_datetime
GROUP BY cb.credit_id, c.store_id, c.customer_id

UNION ALL

SELECT cb.credit_id, c.store_id, c.customer_id, 'giftcard' as source
FROM _dim_credit__credit_base AS cb
         JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
              ON gc.gift_certificate_id = cb.credit_id
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON gc.order_id = o.order_id
         LEFT JOIN lake_consolidated.ultra_merchant_history.customer AS c
                   ON c.customer_id = COALESCE(gc.customer_id, o.customer_id)
                       AND COALESCE(gc.datetime_added, o.datetime_added) BETWEEN c.effective_start_datetime AND c.effective_end_datetime
;

-- Trying to understand the flow of the credits themselves
CREATE OR REPLACE TEMPORARY TABLE _dim_credit AS
SELECT DISTINCT sc.store_credit_id                                                      AS credit_id,
                sc.meta_original_store_credit_id                                        AS meta_original_credit_id,
                'store_credit_id'                                                       AS source_credit_id_type,
                CASE
                    WHEN st.store_type = 'Retail' AND st.store_brand = 'Fabletics' THEN 52
                    WHEN st.store_type = 'Retail' AND st.store_brand = 'Savage X' THEN 121
                    WHEN st.store_id = 41 THEN 26
                    ELSE st.store_id END                                                AS store_id,
                sc.customer_id,
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
                    WHEN rsc.store_credit_id IS NOT NULL
                        OR ref.payment_transaction_id IS NOT NULL THEN 'Refund'
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
                sc.administrator_id,
                CONVERT_TIMEZONE(st.store_time_zone,sc.date_expires)::TIMESTAMP_TZ      AS credit_expiration_local_datetime
FROM _dim_credit__credit_base cb
         JOIN lake_consolidated.ultra_merchant.store_credit sc
              ON cb.credit_id = sc.store_credit_id
         JOIN lake_consolidated.ultra_merchant.statuscode s
              ON s.statuscode = sc.statuscode
         JOIN _ultra_merchant_customer c
              ON c.credit_id = cb.credit_id
                  AND c.source = 'store_credit_id'
         LEFT JOIN _ultra_merchant_membership ms
                   ON ms.credit_id = cb.credit_id
                       AND ms.source = 'store_credit_id'
         JOIN data_model.dim_store st
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

SELECT DISTINCT mt.membership_token_id                                                AS credit_id,
                mt.meta_original_membership_token_id                                  AS meta_original_credit_id,
                'Token'                                                               AS source_credit_id_type,
                CASE
                    WHEN st.store_type = 'Retail' AND st.store_brand = 'Fabletics' THEN 52
                    WHEN st.store_type = 'Retail' AND st.store_brand = 'Savage X' THEN 121
                    WHEN st.store_id = 41 THEN 26
                    ELSE st.store_id END                                              AS store_id,
                m.customer_id,
                'Token'                                                               AS credit_type,
                IFF(mtr.cash = 1, 'Cash', 'NonCash')                                  AS credit_tender,
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
                mt.administrator_id,
                CONVERT_TIMEZONE(st.store_time_zone, mt.date_expires)::TIMESTAMP_TZ   AS credit_expiration_local_datetime
FROM _dim_credit__credit_base cb
         JOIN lake_consolidated.ultra_merchant.membership_token mt
              ON cb.credit_id = mt.membership_token_id
         JOIN lake_consolidated.ultra_merchant.membership_token_reason mtr
              ON mtr.membership_token_reason_id = mt.membership_token_reason_id
         JOIN _ultra_merchant_membership m
              ON m.credit_id = cb.credit_id
                  AND m.source = 'token'
         JOIN data_model.dim_store st
              ON st.store_id = m.store_id
         JOIN lake_consolidated.ultra_merchant.statuscode sc
              ON sc.statuscode = mt.statuscode

UNION ALL

SELECT
    gc.gift_certificate_id                                                   AS credit_id,
    gc.meta_original_gift_certificate_id                                     AS meta_original_credit_id,
    'gift_certificate_id'                                                    AS source_credit_id_type,
    m.store_id,
    be.customer_id,
    'Giftcard'                                                               AS credit_type,
    'NonCash'                                                                AS credit_tender,
    'Bounceback Endowment'                                                   AS credit_reason,
    'Bounceback Endowment'                                                   AS source_credit_reason,
    'Bounceback Endowment'                                                   AS credit_issuance_reason,
    be.order_id                                                              AS credit_order_id,
    NULL                                                                     AS original_credit_id,
    gc.amount                                                                AS credit_amount,
    gc.datetime_added::TIMESTAMP_TZ                                          AS credit_issued_hq_datetime,
    CONVERT_TIMEZONE(st.store_time_zone, gc.datetime_added)::TIMESTAMP_TZ    AS credit_issued_local_datetime,
    be.administrator_id,
    CONVERT_TIMEZONE(st.store_time_zone, gc.date_expires)::TIMESTAMP_TZ      AS credit_expiration_local_datetime
FROM _dim_credit__credit_base AS base
    JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
        ON gc.gift_certificate_id = base.credit_id
    JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
        ON be.object_id = gc.gift_certificate_id
        AND be.object = 'gift_certificate'
    LEFT JOIN _ultra_merchant_membership AS m
        ON m.credit_id = base.credit_id
        AND m.source = 'bounceback'
    JOIN stg.dim_store AS st
        ON st.store_id = m.store_id

UNION ALL

SELECT gc.gift_certificate_id                                                    AS credit_id,
       gc.meta_original_gift_certificate_id                                      AS meta_original_credit_id,
       'gift_certificate_id'                                                     AS source_credit_id_type,
       COALESCE(m.store_id, st.store_id)                                         AS store_id,
       COALESCE(m.customer_id, c.customer_id, gc.customer_id, o.customer_id, -1) AS customer_id,
       'Giftcard'                                                                AS credit_type,
       'Cash'                                                                    AS credit_tender,
       'Gift Card - Paid'                                                        AS credit_reason,
       'Gift Card - Paid'                                                        AS source_credit_reason,
       'Gift Card - Paid'                                                        AS credit_issuance_reason,
       gc.order_id                                                               AS credit_order_id,
       NULL                                                                      AS original_credit_id,
       gc.amount                                                                 AS credit_amount,
       o.datetime_added::TIMESTAMP_TZ                                            AS credit_issued_hq_datetime,
       CONVERT_TIMEZONE(st.store_time_zone, o.datetime_added)::TIMESTAMP_TZ      AS credit_issued_local_datetime,
       NULL                                                                      AS administrator_id,
       CONVERT_TIMEZONE(st.store_time_zone, gc.date_expires)::TIMESTAMP_TZ       AS credit_expiration_local_datetime
FROM _dim_credit__credit_base AS base
         JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
              ON gc.gift_certificate_id = base.credit_id
         LEFT JOIN _ultra_merchant_membership AS m
                   ON m.credit_id = base.credit_id
                       AND m.source = 'giftcard'
         LEFT JOIN _ultra_merchant_customer AS c
                   ON c.credit_id = base.credit_id
                       AND c.source = 'giftcard'
         JOIN lake_consolidated.ultra_merchant."ORDER" AS o
              ON gc.order_id = o.order_id
         JOIN stg.dim_store AS st
              ON st.store_id = COALESCE(m.store_id, o.store_id)
WHERE gc.gift_certificate_type_id <> 9;

-- instead of using a surrogate key, we need to create the keys in the script so we can pair child-parent credit ids and represent the relationship with the original_credit_key field
SET max_credit_key = (SELECT IFF($is_full_refresh,0,IFF(MAX(credit_key) IS NULL OR MAX(credit_key) = -1, 0, MAX(credit_key))) FROM stg.dim_credit);
-- exclude false credits that were accidentaly issued & immediately cancelled
-- accounting wanted them to look like they never even were issued because it causes issues with our credit activity rates
CREATE OR REPLACE TEMPORARY TABLE _dim_credit_key AS
SELECT $max_credit_key +
       ROW_NUMBER() OVER (ORDER BY credit_issued_hq_datetime, credit_id, dc.store_id) AS credit_key,
       credit_id,
       meta_original_credit_id,
       source_credit_id_type,
       dc.store_id,
       customer_id,
       CASE
           WHEN credit_reason = 'Refund - Partial Shipment'
               AND credit_type = 'Fixed Credit' THEN 'Variable Credit'
           ELSE credit_type END                                                                   AS credit_type,
       credit_tender,
       credit_reason,
       source_credit_reason,
       credit_issuance_reason,
       credit_order_id,
       original_credit_id,
       credit_amount,
       credit_issued_hq_datetime,
       credit_issued_local_datetime,
       administrator_id,
       credit_expiration_local_datetime,
       st.store_country                                                                           AS country_code
FROM _dim_credit dc
    JOIN data_model.dim_store st
        ON st.store_id = dc.store_id;
-- SELECT * FROM _dim_credit_key WHERE credit_id = 78445726;

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
    NOT IN (SELECT DISTINCT converted_store_credit_id FROM lake_consolidated.ultra_merchant.store_credit_conversion scc);


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
          -- get the credits that went a second time
               LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction tt2
                         ON tt2.store_credit_id = sc.store_credit_id
                             AND tt2.credit_transfer_transaction_type_id IN (200, 210, 220)
               LEFT JOIN lake_consolidated.ultra_merchant.store_credit sc2 ON sc2.store_credit_id = tt2.source_reference_number::STRING
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
       ctt.source_reference_number AS original_token_id
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
SELECT DISTINCT sc.credit_key,
                dcf.credit_key                               AS original_credit_key,
                dcf.credit_tender                            AS original_credit_tender,
                dcf.credit_type                              AS original_credit_type,
                dcf.credit_reason                            AS original_credit_reason,
                dcf.credit_issued_local_datetime             AS original_credit_issued_local_datetime,
                dcf.credit_expiration_local_datetime         AS credit_expiration_local_datetime,
                dcf.country_code                             AS original_credit_country_code,
                'Converted To Variable And Giftco Roundtrip' AS original_credit_match_reason
FROM _conv_to_variable_plus_giftco_original_credit sc
         JOIN _dim_credit_key dcf ON sc.original_store_credit_id = dcf.credit_id AND
                                     dcf.credit_type ILIKE '%credit%'

UNION ALL

SELECT DISTINCT gc.credit_key,
                dcf.credit_key                       AS original_credit_key,
                dcf.credit_tender                    AS original_credit_tender,
                dcf.credit_type                      AS original_credit_type,
                dcf.credit_reason                    AS original_credit_reason,
                dcf.credit_issued_local_datetime     AS original_credit_issued_local_datetime,
                dcf.credit_expiration_local_datetime AS credit_expiration_local_datetime,
                dcf.country_code                     AS original_country_code,
                'Giftco Roundtrip'                   AS original_credit_match_reason
FROM _giftco_original_credit gc
         JOIN _dim_credit_key dcf ON gc.original_store_credit_id = dcf.credit_id AND
                                     dcf.credit_type ILIKE '%credit%'

UNION ALL

-- only fixed credits get converted to variable
SELECT DISTINCT dc.credit_key,
                dcf.credit_key                       AS original_credit_key,
                dcf.credit_tender                    AS original_credit_tender,
                dcf.credit_type                      AS original_credit_type,
                dcf.credit_reason                    AS original_credit_reason,
                dcf.credit_issued_local_datetime     AS original_credit_issued_local_datetime,
                dcf.credit_expiration_local_datetime AS credit_expiration_local_datetime,
                dcf.country_code                     AS original_credit_country_code,
                'Converted To Variable'              AS original_credit_match_reason
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.store_credit_conversion scc
              ON scc.converted_store_credit_id = dc.credit_id
         JOIN _dim_credit_key dcf
              ON dcf.credit_id = scc.original_store_credit_id
                  AND dcf.credit_type ILIKE '%credit%'
         LEFT JOIN lake_consolidated.ultra_merchant.membership_store_credit msc
                   ON msc.store_credit_id = scc.original_store_credit_id
WHERE scc.store_credit_conversion_type_id = 1
  AND dc.credit_type ILIKE '%credit%'-- conversion to variable

UNION ALL

-- only fixed credits were converted to tokens
SELECT DISTINCT dc.credit_key,
                dcf.credit_key                       AS original_credit_key,
                dcf.credit_tender                    AS original_credit_tender,
                dcf.credit_type                      AS original_credit_type,
                dcf.credit_reason                    AS original_credit_reason,
                dcf.credit_issued_local_datetime     AS original_credit_issued_local_datetime,
                dcf.credit_expiration_local_datetime AS credit_expiration_local_datetime,
                dcf.country_code                     AS original_credit_country_code,
                'Converted To Token'                 AS original_credit_match_reason
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion scc
              ON scc.converted_membership_token_id = dc.credit_id
         JOIN _dim_credit_key dcf
              ON dcf.credit_id = scc.original_store_credit_id AND dcf.credit_type ILIKE '%credit%'
WHERE scc.store_credit_conversion_type_id = 2
  AND dc.credit_type = 'Token' -- conversion to token

UNION ALL

-- In May 2022, FLUK credits were converted to variable then token
SELECT DISTINCT dc.credit_key,
                dck2.credit_key                       AS original_credit_key,
                dck2.credit_tender                    AS original_credit_tender,
                dck2.credit_type                      AS original_credit_type,
                dck2.credit_reason                    AS original_credit_reason,
                dck2.credit_issued_local_datetime     AS original_credit_issued_local_datetime,
                dck2.credit_expiration_local_datetime AS credit_expiration_local_datetime,
                dck2.country_code                     AS original_credit_country_code,
                'Converted To Variable To Token'      AS original_credit_match_reason
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

UNION ALL

SELECT DISTINCT dc.credit_key,
                dcf.credit_key                            AS original_credit_key,
                dcf.credit_tender                         AS original_credit_tender,
                dcf.credit_type                           AS original_credit_type,
                dcf.credit_reason                         AS original_credit_reason,
                dcf.credit_issued_local_datetime          AS original_credit_issued_local_datetime,
                dcf.credit_expiration_local_datetime      AS credit_expiration_local_datetime,
                dcf.country_code                          AS original_credit_country_code,
                'Converted To Token And Giftco Roundtrip' AS original_credit_match_reason
FROM _giftco_store_credit_original_token dc
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion scc
              ON scc.converted_membership_token_id = dc.original_token_id
         JOIN _dim_credit_key dcf ON dcf.credit_id = scc.original_store_credit_id AND dcf.credit_type ILIKE '%credit%'
WHERE scc.store_credit_conversion_type_id = 2

UNION ALL

-- Tokens converted to Fixed Credit
SELECT DISTINCT dc.credit_key,
                dcf.credit_key                       AS original_credit_key,
                dcf.credit_tender                    AS original_credit_tender,
                dcf.credit_type                      AS original_credit_type,
                dcf.credit_reason                    AS original_credit_reason,
                dcf.credit_issued_local_datetime     AS original_credit_issued_local_datetime,
                dcf.credit_expiration_local_datetime AS credit_expiration_local_datetime,
                dcf.country_code                     AS original_credit_country_code,
                'Converted To Credit'                 AS original_credit_match_reason
FROM _dim_credit_key dc
         JOIN lake_consolidated.ultra_merchant.membership_token_credit_conversion mttl
              ON mttl.converted_store_credit_id = dc.credit_id
         JOIN _dim_credit_key dcf
              ON dcf.credit_id = mttl.original_membership_token_id AND dcf.credit_type ILIKE '%token%'
WHERE mttl.store_credit_conversion_type_id = 4
  AND dc.credit_type = 'Fixed Credit' -- conversion to Fixed Credit

UNION ALL

SELECT DISTINCT dc.credit_key,
                dcf.credit_key                        AS original_credit_key,
                dcf.credit_tender                     AS original_credit_tender,
                dcf.credit_type                       AS original_credit_type,
                dcf.credit_reason                     AS original_credit_reason,
                dcf.credit_issued_local_datetime      AS original_credit_issued_local_datetime,
                dcf.credit_expiration_local_datetime  AS credit_expiration_local_datetime,
                dcf.country_code                      AS original_credit_country_code,
                'Gift Card Converted to Store Credit' AS original_credit_match_reason
FROM _giftco_store_credit_original_giftcertificate_ctt dc
         JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
              ON dc.credit_id = ctt.store_credit_id
         JOIN _dim_credit_key dcf ON dcf.credit_id = dc.original_gift_certificate_id
    AND dcf.source_credit_id_type = 'gift_certificate_id'

UNION ALL

SELECT DISTINCT dc.credit_key,
                dcf.credit_key                        AS original_credit_key,
                dcf.credit_tender                     AS original_credit_tender,
                dcf.credit_type                       AS original_credit_type,
                dcf.credit_reason                     AS original_credit_reason,
                dcf.credit_issued_local_datetime      AS original_credit_issued_local_datetime,
                dcf.credit_expiration_local_datetime  AS credit_expiration_local_datetime,
                dcf.country_code                      AS original_credit_country_code,
                'Gift Card Converted to Store Credit' AS original_credit_match_reason
FROM _giftco_store_credit_original_giftcertificate_gsct dc
         JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit gcst
              ON dc.credit_id = gcst.store_credit_id
         JOIN _dim_credit_key dcf ON dcf.credit_id = dc.original_gift_certificate_id
    AND dcf.source_credit_id_type = 'gift_certificate_id';


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
       dc.credit_expiration_local_datetime,
       administrator_id,
       COALESCE(ca.original_credit_key, dc.credit_key)               AS original_credit_key,
       COALESCE(ca.original_credit_match_reason, 'Already Original') AS original_credit_match_reason,
       COALESCE(ca.original_credit_tender, dc.credit_tender)         AS original_credit_tender,
       COALESCE(ca.original_credit_type, dc.credit_type)             AS original_credit_type,
       COALESCE(ca.original_credit_reason, dc.credit_reason)         AS original_credit_reason,
       COALESCE(ca.original_credit_country_code, dc.country_code)    AS credit_country_code,
       CAST('Unknown' AS VARCHAR(100))                               AS credit_report_mapping,
       CAST('Unknown' AS VARCHAR(100))                               AS credit_report_sub_mapping,
       COALESCE(ca.original_credit_issued_local_datetime,
                dc.credit_issued_local_datetime)                     AS original_credit_issued_local_datetime,
       COALESCE(ca.credit_expiration_local_datetime,
                dc.credit_expiration_local_datetime)                 AS original_credit_expiration_local_datetime,
       FALSE                                                         AS is_cancelled_before_token_conversion
FROM _dim_credit_key dc
         LEFT JOIN _credit_origination ca ON ca.credit_key = dc.credit_key;

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
SELECT
DISTINCT
    dcf.credit_key,
    m.price AS equivalent_credit_value_local_amount,
    ABS(DATEDIFF('millisecond',CONVERT_TIMEZONE('America/Los_Angeles',dcf.original_credit_issued_local_datetime),m.effective_start_datetime)) AS date_diff,
    RANK() OVER(PARTITION BY credit_key ORDER BY date_diff) AS closest_membership_date
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

UPDATE _dim_credit_final
SET credit_report_mapping     = r.report_mapping,
    credit_report_sub_mapping = r.report_sub_mapping
FROM reference.credit_report_mapping r
WHERE r.original_credit_type = _dim_credit_final.original_credit_type
  AND _dim_credit_final.original_credit_reason = r.original_credit_reason
  AND _dim_credit_final.original_credit_tender = r.original_credit_tender
  AND _dim_credit_final.original_credit_match_reason = r.original_credit_match_reason;

/* 573 credits were wrongfully converted to token. They were supposed to be canceled, but source did not cancel them and they got converted during EMP migration (DA-28668) */
UPDATE _dim_credit_final AS t
SET t.is_cancelled_before_token_conversion = TRUE
FROM reference.credit_cancellation_datetime AS s
WHERE s.credit_id = t.credit_id
    AND s.source_credit_id_type = t.source_credit_id_type
    AND s.credit_issuance_reason = 'Credit To Token Conversion';

CREATE OR REPLACE TEMPORARY TABLE _dim_credit__bill_me_now AS
SELECT
    dcf.credit_key,
    IFF(mb.membership_billing_source_id = 5, TRUE, FALSE) AS is_bill_me_now_online,
    IFF(mb.membership_billing_source_id = 6, TRUE, FALSE) AS is_bill_me_now_gms
FROM _dim_credit_final AS dcf
JOIN lake_consolidated.ultra_merchant.membership_billing AS mb
    ON mb.order_id = dcf.credit_order_id
    AND mb.membership_billing_source_id IN (5,6);


-- putting exchange rates to convert source currency to USD by day into a table
CREATE OR REPLACE TEMPORARY TABLE _exchange_rate AS
SELECT src_currency,
       er.rate_date_pst,
       er.exchange_rate
FROM reference.currency_exchange_rate_by_date er
WHERE dest_currency = 'USD';

UPDATE stg.dim_credit dc
SET dc.credit_issued_usd_conversion_rate = exch.exchange_rate,
    dc.meta_update_datetime = current_timestamp
FROM stg.dim_store ds
    JOIN _exchange_rate exch
                   ON exch.src_currency = ds.store_currency
    WHERE exch.exchange_rate <> dc.credit_issued_usd_conversion_rate AND ds.store_id = dc.store_id
                       AND CAST(dc.credit_issued_hq_datetime AS DATE) = rate_date_pst;

-- putting vat rates into a temp table
CREATE OR REPLACE TEMPORARY TABLE _vat_rate AS
SELECT REPLACE(country_code, 'GB', 'UK') AS country_code,
       dd.full_date,
       er.rate
FROM reference.vat_rate_history er
         JOIN data_model.dim_date dd ON er.start_date <= dd.full_date
    AND er.expires_date >= dd.full_date
WHERE dd.full_date <= CURRENT_DATE() + 7;

UPDATE stg.dim_credit dc
SET dc.credit_issued_local_amount = credit_issued_local_gross_vat_amount/ (1 + IFNULL(vrh.rate, 0)),
    meta_update_datetime = current_timestamp
FROM stg.dim_store ds
         JOIN _vat_rate vrh
              ON vrh.country_code = ds.store_country
WHERE CAST(credit_issued_local_gross_vat_amount/ (1 + IFNULL(vrh.rate, 0)) AS NUMBER(19,4)) <> credit_issued_local_amount AND ds.store_id = dc.store_id
AND vrh.full_date = CAST(dc.credit_issued_hq_datetime AS DATE);

CREATE OR REPLACE TEMP TABLE _dim_credit__stg AS
SELECT dcf.credit_key,
       dcf.credit_id,
       dcf.meta_original_credit_id,
       dcf.store_id,
       dcf.customer_id,
       COALESCE(dcf.administrator_id,-1) AS administrator_id,
       dcf.credit_order_id,
       dcf.source_credit_id_type,
       dcf.credit_report_mapping,
       dcf.credit_report_sub_mapping,
       dcf.credit_type,
       dcf.credit_tender,
       dcf.credit_reason,
       dcf.credit_issuance_reason,
       dcf.credit_issued_hq_datetime,
       dcf.credit_issued_local_datetime,
       dcf.credit_expiration_local_datetime,
       dcf.original_credit_key,
       dcf.original_credit_tender,
       dcf.original_credit_type,
       dcf.original_credit_issued_local_datetime,
       dcf.original_credit_expiration_local_datetime,
       dcf.original_credit_reason,
       dcf.original_credit_match_reason,
       'Recognize'                                                                                 AS deferred_recognition_label_token,
       dcf.credit_country_code                                                                     AS credit_country_code,
       IFNULL(exch.exchange_rate, 1)                                                               AS credit_issued_usd_conversion_rate,
       dcf.credit_amount                                                                           AS credit_issued_local_gross_vat_amount,
       IFNULL(IFF(dcf.credit_type = 'Token', 1 ,ROUND(dcf.credit_amount / NULLIF(eq.equivalent_credit_value_local_amount, 0), 4)), 1)     AS credit_issued_equivalent_count,
       dcf.credit_amount / (1 + IFNULL(vrh.rate, 0))                                               AS credit_issued_local_amount,
       COALESCE(bmn.is_bill_me_now_online, FALSE)                                                  AS is_bill_me_now_online,
       COALESCE(bmn.is_bill_me_now_gms, FALSE)                                                     AS is_bill_me_now_gms,
       COALESCE(dcf.is_cancelled_before_token_conversion, FALSE)                                   AS is_cancelled_before_token_conversion,
       IFF(tc.customer_id IS NOT NULL, TRUE, FALSE)                                                AS is_test_customer,
       $execution_start_time                                                                       AS meta_create_datetime,
       $execution_start_time                                                                       AS meta_update_datetime
FROM _dim_credit_final dcf
         JOIN data_model.dim_store st
              ON st.store_id = dcf.store_id
         LEFT JOIN reference.test_customer tc
                   ON dcf.customer_id = tc.customer_id
         LEFT JOIN _dim_credit__equivalent_value AS eq
                   ON eq.credit_key = dcf.credit_key
         LEFT JOIN _exchange_rate exch
                   ON exch.src_currency = st.store_currency
                       AND CAST(dcf.credit_issued_hq_datetime AS DATE) = exch.rate_date_pst
         LEFT JOIN _vat_rate vrh
                   ON vrh.country_code = st.store_country
                       AND vrh.full_date = CAST(dcf.credit_issued_hq_datetime AS DATE)
         LEFT JOIN _dim_credit__bill_me_now AS bmn
                   ON bmn.credit_key = dcf.credit_key;
-- SELECT credit_id, store_id, credit_issued_hq_datetime, COUNT(1) FROM _dim_credit__stg GROUP BY 1, 2, 3 HAVING COUNT(1) > 1;
-- SELECT * FROM _dim_credit__stg WHERE credit_id = 78445726;
-- SELECT * FROM _dim_credit_final WHERE credit_id = 78445726;

------------------------------------------------------------------------
-- insert into the staging table

INSERT INTO stg.dim_credit_stg (
    credit_key,
    credit_id,
    meta_original_credit_id,
    store_id,
    customer_id,
    administrator_id,
    credit_order_id,
    source_credit_id_type,
    credit_report_mapping,
    credit_report_sub_mapping,
    credit_type,
    credit_tender,
    credit_reason,
    credit_issuance_reason,
    credit_issued_hq_datetime,
    credit_issued_local_datetime,
    credit_expiration_local_datetime,
    original_credit_key,
    original_credit_tender,
    original_credit_type,
    original_credit_issued_local_datetime,
    original_credit_expiration_local_datetime,
    original_credit_reason,
    original_credit_match_reason,
    deferred_recognition_label_token,
    credit_country_code,
    credit_issued_usd_conversion_rate,
    credit_issued_local_gross_vat_amount,
    credit_issued_equivalent_count,
    credit_issued_local_amount,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_cancelled_before_token_conversion,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    credit_key,
    credit_id,
    meta_original_credit_id,
    store_id,
    customer_id,
    administrator_id,
    credit_order_id,
    source_credit_id_type,
    credit_report_mapping,
    credit_report_sub_mapping,
    credit_type,
    credit_tender,
    credit_reason,
    credit_issuance_reason,
    credit_issued_hq_datetime,
    credit_issued_local_datetime,
    credit_expiration_local_datetime,
    original_credit_key,
    original_credit_tender,
    original_credit_type,
    original_credit_issued_local_datetime,
    original_credit_expiration_local_datetime,
    original_credit_reason,
    original_credit_match_reason,
    deferred_recognition_label_token,
    credit_country_code,
    credit_issued_usd_conversion_rate,
    credit_issued_local_gross_vat_amount,
    credit_issued_equivalent_count,
    credit_issued_local_amount,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_cancelled_before_token_conversion,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_credit__stg;

