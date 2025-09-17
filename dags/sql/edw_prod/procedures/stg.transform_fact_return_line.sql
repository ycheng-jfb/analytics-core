SET target_table = 'stg.fact_return_line';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

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
SET wm_lake_ultra_merchant_return = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.return'));
SET wm_lake_ultra_merchant_return_product = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.return_product'));
SET wm_lake_ultra_warehouse_lpn = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.lpn'));
SET wm_lake_ultra_warehouse_warehouse = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.warehouse'));
SET wm_lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_edw_stg_dim_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer'));
SET wm_edw_stg_fact_activation = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
SET wm_edw_stg_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
SET wm_edw_stg_fact_order_line = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_line'));
SET wm_lake_ultra_merchant_order_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_classification'));
SET wm_lake_ultra_merchant_order_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_detail'));
SET wm_lake_ultra_merchant_order_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line'));
SET wm_lake_ultra_merchant_order_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_credit'));
SET wm_lake_ultra_merchant_rma = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.rma'));
SET wm_lake_ultra_merchant_return_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.return_line'));
SET wm_lake_ultra_merchant_rma_product = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.rma_product'));
SET wm_lake_ultra_merchant_rma_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.rma_line'));
SET wm_lake_ultra_merchant_order_line_discount = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line_discount'));
SET wm_lake_ultra_merchant_retail_return_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.retail_return_detail'));
SET wm_lake_ultra_merchant_retail_return = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.retail_return'));
SET wm_lake_ultra_merchant_exchange = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.exchange'));
SET wm_lake_ultra_merchant_exchange_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.exchange_line'));
SET wm_lake_ultra_merchant_address = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.address'));
SET wm_lake_ultra_merchant_reship = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.reship'));
SET wm_lake_ultra_merchant_gift_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_order'));
SET wm_lake_ultra_merchant_order_line_split_map = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line_split_map'));
SET wm_lake_ultra_merchant_statuscode_modification_log = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.statuscode_modification_log'));
SET wm_edw_reference_finance_assumption = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.finance_assumption'));


CREATE OR REPLACE TEMP TABLE _fact_return_line__return_base (return_id INT, rma_id INT);

-- Full Refresh
INSERT INTO _fact_return_line__return_base (return_id, rma_id)
SELECT r.return_id, -1
FROM lake_consolidated.ultra_merchant.return AS r
WHERE $is_full_refresh = TRUE
ORDER BY r.return_id;

INSERT INTO _fact_return_line__return_base (rma_id, return_id)
SELECT rma.rma_id, -1
FROM lake_consolidated.ultra_merchant.rma AS rma
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = rma.order_id
WHERE $is_full_refresh = TRUE
    AND oc.order_type_id = 51
    AND rma.statuscode IN (4656,4670,4672) /* return is in transit or received */
    AND NOT EXISTS (
        SELECT 1
        FROM lake_consolidated.ultra_merchant.return AS r
        WHERE r.rma_id = rma.rma_id
    );

-- Incremental Refresh
INSERT INTO _fact_return_line__return_base (return_id, rma_id)
SELECT DISTINCT incr.return_id, -1
FROM (
    /* Self-check for manual updates */
    SELECT frl.return_id
    FROM stg.fact_return_line AS frl
    WHERE frl.meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        LEFT JOIN lake_consolidated.ultra_merchant.return_product AS rp
            ON rp.return_id = r.return_id
        LEFT JOIN lake.ultra_warehouse.lpn AS l
            ON l.lpn_code = rp.lpn_code
        LEFT JOIN lake.ultra_warehouse.warehouse AS w
            ON w.warehouse_id = l.warehouse_id
    WHERE r.meta_update_datetime > $wm_lake_ultra_merchant_return
        OR rp.meta_update_datetime > $wm_lake_ultra_merchant_return_product
        OR l.hvr_change_time > $wm_lake_ultra_warehouse_lpn
        OR w.hvr_change_time > $wm_lake_ultra_warehouse_warehouse
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.order_id = r.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.address a
            ON a.address_id = o.shipping_address_id
    WHERE o.meta_update_datetime > $wm_lake_ultra_merchant_order
        OR a.meta_update_datetime > $wm_lake_ultra_merchant_address
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        JOIN stg.fact_order AS o
            ON o.order_id = r.order_id
        LEFT JOIN stg.dim_customer AS dc
            ON dc.customer_id = o.customer_id
        LEFT JOIN stg.fact_activation AS fa
            ON fa.customer_id = o.customer_id
    WHERE o.meta_update_datetime > $wm_edw_stg_fact_order
        OR dc.meta_update_datetime > $wm_edw_stg_dim_customer
        OR fa.meta_update_datetime > $wm_edw_stg_fact_activation
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = r.order_id
        AND oc.order_type_id = 40
    WHERE oc.meta_update_datetime > $wm_lake_ultra_merchant_order_classification
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
    JOIN lake_consolidated.ultra_merchant.order_detail AS od
        ON od.order_id = r.order_id
        AND od.name IN ('retail_store_id','original_store_id')
    WHERE od.meta_update_datetime > $wm_lake_ultra_merchant_order_detail
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        LEFT JOIN lake_consolidated.ultra_merchant.order_line AS ol
            ON ol.order_id = r.order_id
    WHERE ol.meta_update_datetime > $wm_lake_ultra_merchant_order_line
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        LEFT JOIN stg.fact_order_line AS fol
            ON fol.order_id = r.order_id
    WHERE fol.meta_update_datetime > $wm_edw_stg_fact_order_line
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        LEFT JOIN lake_consolidated.ultra_merchant.order_credit AS oc
            ON oc.order_id = r.order_id
    WHERE oc.meta_update_datetime > $wm_lake_ultra_merchant_order_credit
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        LEFT JOIN lake_consolidated.ultra_merchant.rma AS rma
            ON rma.order_id = r.order_id
    WHERE rma.meta_update_datetime > $wm_lake_ultra_merchant_rma
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        LEFT JOIN lake_consolidated.ultra_merchant.return_line AS rl
            ON rl.return_id = r.return_id
        LEFT JOIN lake_consolidated.ultra_merchant.rma_product AS rmp
            ON rmp.order_line_id = rl.order_line_id
        LEFT JOIN lake_consolidated.ultra_merchant.order_line_discount AS old
            ON old.order_line_id = rl.order_line_id
    WHERE rl.meta_update_datetime > $wm_lake_ultra_merchant_return_line
        OR rmp.meta_update_datetime > $wm_lake_ultra_merchant_rma_product
        OR old.meta_update_datetime > $wm_lake_ultra_merchant_order_line_discount
    UNION ALL
    SELECT rrd.return_id
    FROM lake_consolidated.ultra_merchant.retail_return_detail rrd
    JOIN lake_consolidated.ultra_merchant.retail_return rr
        ON rrd.retail_return_id = rr.retail_return_id
     WHERE rrd.meta_update_datetime > $wm_lake_ultra_merchant_retail_return_detail
        OR rr.meta_update_datetime > $wm_lake_ultra_merchant_retail_return
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        JOIN lake_consolidated.ultra_merchant.exchange AS e
            ON e.original_order_id = r.order_id
            AND e.statuscode = 4589
        JOIN lake_consolidated.ultra_merchant.exchange_line AS el
            ON el.exchange_id = e.exchange_id
    WHERE e.meta_update_datetime > $wm_lake_ultra_merchant_exchange
    OR el.meta_update_datetime > $wm_lake_ultra_merchant_exchange_line
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
            ON gfto.order_id = r.order_id
    WHERE gfto.meta_update_datetime > $wm_lake_ultra_merchant_gift_order
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
        JOIN lake_consolidated.ultra_merchant.reship AS rsh
            ON r.order_id = rsh.reship_order_id
    WHERE rsh.meta_update_datetime > $wm_lake_ultra_merchant_reship
    UNION ALL
    SELECT r.return_id
    FROM lake_consolidated.ultra_merchant.return AS r
             JOIN lake_consolidated.ultra_merchant.order_line_split_map sm
                  ON r.order_id = sm.order_id
    WHERE sm.meta_update_datetime > $wm_lake_ultra_merchant_order_line_split_map
    UNION ALL
    SELECT return_id
    FROM stg.fact_return_line
    WHERE DATE_TRUNC(MONTH, return_completion_local_datetime::DATE) IN
          (SELECT DISTINCT financial_date FROM reference.finance_assumption
           WHERE meta_update_datetime > $wm_edw_reference_finance_assumption)
    UNION ALL
    /* Previously errored rows */
    SELECT return_id
    FROM excp.fact_return_line
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
        AND return_id <> -1
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.return_id;

/* adding in rma data for third party returns that don't have a return_id */
INSERT INTO _fact_return_line__return_base (rma_id, return_id)
SELECT DISTINCT rma_id, -1
FROM (SELECT rma.rma_id
      FROM lake_consolidated.ultra_merchant.rma AS rma
               JOIN lake_consolidated.ultra_merchant.order_classification AS oc
                    ON oc.order_id = rma.order_id
               JOIN lake_consolidated.ultra_merchant.rma_product AS rp
                    ON rp.rma_id = rma.rma_id
               JOIN lake_consolidated.ultra_merchant.rma_line AS rl
                    ON rl.rma_id = rma.rma_id
               LEFT JOIN lake_consolidated.ultra_merchant.statuscode_modification_log AS sml
                         ON sml.object_id = rma.meta_original_rma_id
                             AND sml.object = 'rma'
                             AND sml.to_statuscode IN (4656, 4670, 4672)
               LEFT JOIN lake_consolidated.ultra_merchant.return AS r
                        ON r.rma_id = rma.rma_id
      WHERE $is_full_refresh = FALSE
        AND r.return_id IS NULL
        AND rma.statuscode IN (4656, 4670, 4672)
        AND oc.order_type_id = 51
        AND (
              rma.meta_update_datetime > $wm_lake_ultra_merchant_rma
              OR rp.meta_update_datetime > $wm_lake_ultra_merchant_rma_product
              OR oc.meta_update_datetime > $wm_lake_ultra_merchant_order_classification
              OR sml.meta_update_datetime > $wm_lake_ultra_merchant_statuscode_modification_log
              OR rl.meta_update_datetime > $wm_lake_ultra_merchant_rma_line
          )

      UNION ALL

      SELECT rma_id
      FROM excp.fact_return_line
      WHERE return_id = -1
        AND meta_is_current_excp
        AND meta_data_quality = 'error');

/* cleaning up duplicate rma_product_ids assigned to a single order_line_id */
CREATE OR REPLACE TEMP TABLE _fact_return_line__rma_product_cleanup AS
SELECT
    base.rma_id,
    rp.rma_product_id,
    rp.meta_original_rma_product_id,
    rp.order_line_id,
    rp.product_id
FROM _fact_return_line__return_base AS base
    JOIN lake_consolidated.ultra_merchant.rma_product AS rp
        ON rp.rma_id = base.rma_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY rp.rma_id, rp.order_line_id ORDER BY rp.datetime_added) = 1;


/* cleaning up duplicate rma_line_ids assigned to a single order_line_id */
CREATE OR REPLACE TEMP TABLE _fact_return_line__rma_line_cleanup AS
SELECT
    base.rma_id,
    rl.rma_line_id,
    rl.order_line_id,
    rl.quantity
FROM _fact_return_line__return_base AS base
    JOIN lake_consolidated.ultra_merchant.rma_line AS rl
        ON rl.rma_id = base.rma_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY rl.rma_id, rl.order_line_id ORDER BY rl.datetime_added) = 1;



CREATE OR REPLACE TEMP TABLE _fact_return_line__order_base AS
SELECT DISTINCT order_id
FROM (SELECT r.order_id
      FROM _fact_return_line__return_base AS base
               JOIN lake_consolidated.ultra_merchant.return AS r
                    ON r.return_id = base.return_id
      UNION ALL
      SELECT rma.order_id
      FROM _fact_return_line__return_base AS base
               JOIN lake_consolidated.ultra_merchant.rma AS rma
                    ON base.rma_id = rma.rma_id)
ORDER BY order_id;
-- SELECT * FROM _fact_return_line__order_base;


CREATE OR REPLACE TEMP TABLE _fact_return_line__order_line_base AS
SELECT
    ol.order_id,
    ol.order_line_id,
    ol.product_id,
    ol.extended_price,
    ol.statuscode,
    ol.group_key,
    ol.quantity,
    pt.label AS product_type,
    COALESCE(TO_BOOLEAN(pt.is_free), FALSE) AS is_free
FROM _fact_return_line__order_base AS base
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_id = base.order_id
    LEFT JOIN lake_consolidated.ultra_merchant.product_type AS pt
        ON pt.product_type_id = ol.product_type_id
ORDER BY ol.order_id, ol.order_line_id;
-- SELECT * FROM _fact_return_line__order_line_base;

CREATE OR REPLACE TEMP TABLE _fact_return_line__order_total AS
SELECT
    base.order_id,
    SUM(base.extended_price) AS extended_price_total
FROM _fact_return_line__order_line_base AS base
WHERE LOWER(base.product_type) <> 'bundle'
    AND NOT base.is_free
GROUP BY base.order_id
ORDER BY base.order_id;
-- SELECT * FROM _fact_return_line__order_total;

CREATE OR REPLACE TEMP TABLE _fact_return_line__order_line_discount_base AS
SELECT
    base.order_line_id,
    base.extended_price,
    COALESCE(SUM(old.amount), 0) AS discount_gross_of_vat_local_amount
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_line_discount AS old
        ON old.order_line_id = base.order_line_id
WHERE o.datetime_added >= '2014-05-15'
GROUP BY
    base.order_line_id,
    base.extended_price
ORDER BY base.order_line_id;
-- SELECT * FROM _fact_return_line__order_line_discount_base;

CREATE OR REPLACE TEMP TABLE _fact_return_line__bundles AS
SELECT
    base.order_id,
    base.group_key,
    COALESCE(base.extended_price, 0) AS bundle_subtotal,
    oldb.discount_gross_of_vat_local_amount AS bundle_discount
FROM _fact_return_line__order_line_base AS base
    JOIN _fact_return_line__order_line_discount_base AS oldb
        ON oldb.order_line_id = base.order_line_id
WHERE LOWER(base.product_type) = 'bundle';
-- SELECT * FROM _fact_return_line__bundles;

CREATE OR REPLACE TEMP TABLE _fact_return_line__order_line_discount (order_line_id INT, discount_gross_of_vat_local_amount NUMBER(19, 4));

INSERT INTO _fact_return_line__order_line_discount
SELECT
    base.order_line_id,
    oldb.discount_gross_of_vat_local_amount + COALESCE(IFF(b.bundle_subtotal = 0, 0, b.bundle_discount * 1.0 * base.extended_price / b.bundle_subtotal), 0) AS discount_gross_of_vat_local_amount
FROM _fact_return_line__order_line_base AS base
    JOIN _fact_return_line__order_line_discount_base AS oldb
        ON oldb.order_line_id = base.order_line_id
    LEFT JOIN _fact_return_line__bundles AS b
        ON b.order_id = base.order_id
        AND b.group_key = base.group_key;

INSERT INTO _fact_return_line__order_line_discount
SELECT
    base.order_line_id,
    CASE
        WHEN base.extended_price = 0 THEN 0
        WHEN base.is_free THEN 0
        ELSE COALESCE((o.discount * (base.extended_price / ot.extended_price_total)), 0)
    END::NUMBER(19, 4) AS discount_gross_of_vat_local_amount
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    LEFT JOIN _fact_return_line__order_total AS ot
        ON ot.order_id = base.order_id
WHERE o.datetime_added < '2014-05-15'
    AND LOWER(base.product_type) <> 'bundle'; /* this product_type filter is not present in edw01 fact_order_line */
-- SELECT * FROM _fact_return_line__order_line_discount;

CREATE OR REPLACE TEMP TABLE _fact_return_line__order_agg AS
SELECT
    base.order_id,
    SUM(base.extended_price - old.discount_gross_of_vat_local_amount) AS order_total
/* calculate the order line values after applying any discounts */
FROM _fact_return_line__order_line_base AS base
    LEFT JOIN _fact_return_line__order_line_discount AS old
        ON old.order_line_id = base.order_line_id
WHERE LOWER(base.product_type) <> 'bundle' /* Filters out Bundle order lines */
    AND base.statuscode <> 2830 /* Filters out cancelled order lines */
    AND NOT base.is_free /* Filters out free items */
GROUP BY base.order_id;
-- SELECT * FROM _fact_return_line__order_agg;

CREATE OR REPLACE TEMP TABLE _fact_return_line__return_agg AS
SELECT
    rl.return_id,
    -1::INT AS rma_id,
    SUM(base.extended_price - old.discount_gross_of_vat_local_amount) AS return_total
/* calculate the order line values after applying any discounts */
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.order_line_id = base.order_line_id
    LEFT JOIN _fact_return_line__order_line_discount AS old
        ON old.order_line_id = base.order_line_id
GROUP BY rl.return_id;
-- SELECT * FROM _fact_return_line__return_agg;

INSERT INTO _fact_return_line__return_agg (return_id, rma_id, return_total)
SELECT
    -1 AS return_id,
    rp.rma_id,
    SUM(base.extended_price - old.discount_gross_of_vat_local_amount) AS return_total
FROM _fact_return_line__order_line_base AS base
    JOIN _fact_return_line__rma_product_cleanup AS rp
        ON rp.order_line_id = base.order_line_id
    LEFT JOIN _fact_return_line__order_line_discount AS old
        ON old.order_line_id = base.order_line_id
GROUP BY rp.rma_id;

CREATE OR REPLACE TEMP TABLE _fact_return_line__return_line_ratio AS
SELECT
    rl.return_line_id,
    -1::INT AS rma_product_id,
    IFF(ra.return_total = 0, 0, ((base.extended_price - old.discount_gross_of_vat_local_amount) / ra.return_total))::NUMBER(18, 6) AS return_line_ratio_of_total
/* calculate the order line values after applying any discounts */
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.order_line_id = base.order_line_id
    JOIN _fact_return_line__return_agg AS ra
        ON ra.return_id = rl.return_id
    LEFT JOIN _fact_return_line__order_line_discount AS old
        ON old.order_line_id = base.order_line_id;
-- SELECT * FROM _fact_return_line__return_line_ratio;

INSERT INTO _fact_return_line__return_line_ratio (return_line_id, rma_product_id, return_line_ratio_of_total)
SELECT
    -1 AS return_line_id,
    rp.rma_product_id,
    IFF(ra.return_total = 0, 0, ((base.extended_price - old.discount_gross_of_vat_local_amount) / ra.return_total))::NUMBER(18, 6) AS return_line_ratio_of_total
/* calculate the order line values after applying any discounts */
FROM _fact_return_line__order_line_base AS base
    JOIN _fact_return_line__rma_product_cleanup AS rp
        ON rp.order_line_id = base.order_line_id
    JOIN _fact_return_line__return_agg AS ra
        ON ra.rma_id = rp.rma_id
    LEFT JOIN _fact_return_line__order_line_discount AS old
        ON old.order_line_id = base.order_line_id;

CREATE OR REPLACE TEMP TABLE _fact_return_line__order_details AS
SELECT
    base.order_id,
    base.order_line_id,
    COALESCE(oa.order_total,0) AS order_total,
    COALESCE(old.discount_gross_of_vat_local_amount,0) AS discount_gross_of_vat_local_amount,
    CASE
        WHEN COALESCE(oa.order_total, 0) = 0 THEN 0
        WHEN base.statuscode = 2830 OR base.is_free THEN 0 /* Cancelled and Free Order lines */
        ELSE ((base.extended_price - old.discount_gross_of_vat_local_amount) / oa.order_total)
    END::NUMBER(18, 6) AS order_line_ratio_of_total
FROM _fact_return_line__order_line_base AS base
    JOIN _fact_return_line__order_agg AS oa
        ON oa.order_id = base.order_id
    LEFT JOIN _fact_return_line__order_line_discount AS old
        ON old.order_line_id = base.order_line_id
WHERE LOWER(base.product_type) <> 'bundle';
-- SELECT * FROM _fact_return_line__order_details;

CREATE OR REPLACE TEMP TABLE _fact_return_line__cash_non_cash AS
SELECT
    oc.order_credit_id,
    oc.order_id,
    IFF(COALESCE(TO_BOOLEAN(scr.cash), FALSE), oc.amount, 0) AS cash_credit_gross_of_vat_local_amount,
    IFF(NOT COALESCE(TO_BOOLEAN(scr.cash), FALSE), oc.amount, 0) non_cash_credit_gross_of_vat_local_amount
FROM _fact_return_line__order_base AS base
    JOIN lake_consolidated.ultra_merchant.order_credit AS oc
        ON oc.order_id = base.order_id
        AND oc.hvr_is_deleted = 0
    LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit AS gcsc
        ON gcsc.gift_certificate_id = oc.gift_certificate_id
    LEFT JOIN lake_consolidated.ultra_merchant.store_credit AS sc
        ON sc.store_credit_id = COALESCE(oc.store_credit_id, gcsc.store_credit_id)
    LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
        ON scr.store_credit_reason_id = sc.store_credit_reason_id;
-- SELECT * FROM _fact_return_line__cash_non_cash;

CREATE OR REPLACE TEMP TABLE _fact_return_line__cash_non_cash_totals AS
SELECT
    cnc.order_id,
    COALESCE(SUM(cnc.cash_credit_gross_of_vat_local_amount), 0) AS cash_credit_gross_of_vat_local_amount,
    COALESCE(SUM(cnc.non_cash_credit_gross_of_vat_local_amount), 0) AS non_cash_credit_gross_of_vat_local_amount
FROM _fact_return_line__cash_non_cash AS cnc
GROUP BY cnc.order_id;
-- SELECT * FROM _fact_return_line__cash_non_cash_totals;

CREATE OR REPLACE TEMP TABLE _fact_return_line__order_line_fact AS
SELECT
    base.order_line_id,
    base.quantity,
    (base.extended_price / (1 + COALESCE(vrh.rate, 0))) AS subtotal_local_amount,
    (old.discount_gross_of_vat_local_amount	/ (1 + COALESCE(vrh.rate, 0))) AS discount_local_amount,
    od.order_line_ratio_of_total * (COALESCE(cnct.cash_credit_gross_of_vat_local_amount, 0) / (1 + COALESCE(vrh.rate, 0))) AS cash_credit_local_amount,
    od.order_line_ratio_of_total * (COALESCE(cnct.non_cash_credit_gross_of_vat_local_amount, 0) / (1 + COALESCE(vrh.rate, 0))) AS non_cash_credit_local_amount,
    od.order_line_ratio_of_total * COALESCE(o.tax, 0) AS tax_local_amount
FROM _fact_return_line__order_line_base AS base
    LEFT JOIN _fact_return_line__order_line_discount AS old
        ON old.order_line_id = base.order_line_id
    LEFT JOIN _fact_return_line__order_details AS od
        ON od.order_line_id = base.order_line_id
    LEFT JOIN _fact_return_line__cash_non_cash_totals AS cnct
        ON cnct.order_id = base.order_id
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    LEFT JOIN lake_consolidated.ultra_merchant.address AS a
        ON a.address_id = o.shipping_address_id
    LEFT JOIN reference.vat_rate_history AS vrh
        ON vrh.country_code = REPLACE(UPPER(a.country_code), 'UK', 'GB')
        AND IFF(o.order_id IS NOT NULL, o.date_placed, o.datetime_added)::DATE BETWEEN vrh.start_date AND vrh.expires_date;
-- SELECT * FROM _fact_return_line__order_line_fact;

CREATE OR REPLACE TEMP TABLE _fact_return_line__return_line_values AS
SELECT
    rl.return_line_id,
    rl.return_id,
    -1::INT AS rma_id,
    -1::INT AS rma_product_id,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.tax_local_amount)	AS return_tax_local_amount,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.subtotal_local_amount) AS return_subtotal_local_amount,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.discount_local_amount) AS return_discount_local_amount,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.cash_credit_local_amount) AS return_cash_credit_local_amount,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.non_cash_credit_local_amount) AS return_non_cash_credit_local_amount
FROM _fact_return_line__return_base AS base
    JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.return_id = base.return_id
    JOIN _fact_return_line__order_line_fact AS olf
        ON olf.order_line_id = rl.order_line_id;
-- SELECT * FROM _fact_return_line__return_line_values;

INSERT INTO _fact_return_line__return_line_values
SELECT
    -1 AS return_line_id,
    -1 AS return_id,
    rp.rma_id,
    rp.rma_product_id,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.tax_local_amount)	AS return_tax_local_amount,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.subtotal_local_amount) AS return_subtotal_local_amount,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.discount_local_amount) AS return_discount_local_amount,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.cash_credit_local_amount) AS return_cash_credit_local_amount,
    CAST((rl.quantity) AS DECIMAL(18, 2)) / olf.quantity * (olf.non_cash_credit_local_amount) AS return_non_cash_credit_local_amount
FROM _fact_return_line__return_base AS base
    JOIN _fact_return_line__rma_line_cleanup AS rl
            ON rl.rma_id = base.rma_id
    JOIN _fact_return_line__order_line_fact AS olf
        ON olf.order_line_id = rl.order_line_id
    JOIN _fact_return_line__rma_product_cleanup AS rp
        ON rp.rma_id = rl.rma_id
        AND rp.order_line_id = rl.order_line_id;


CREATE OR REPLACE TEMP TABLE _fact_return_line__return_product AS
SELECT DISTINCT
    rl.return_line_id,
    -1::INT AS rma_product_id,
    l.lpn_code,
    w.warehouse_id
FROM (
    SELECT
        rp.return_id,
        rp.order_line_id,
        rp.lpn_code
    FROM _fact_return_line__return_base AS base
        JOIN lake_consolidated.ultra_merchant.return_product AS rp
            ON rp.return_id = base.return_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rp.order_line_id ORDER BY rp.lpn_code DESC, rp.return_id DESC) = 1
    ) AS r
    JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.order_line_id = r.order_line_id
    JOIN lake.ultra_warehouse.lpn AS l
        ON l.lpn_code = r.lpn_code
    JOIN lake.ultra_warehouse.warehouse AS w
        ON w.warehouse_id = l.warehouse_id;
-- SELECT * FROM _fact_return_line__return_product;

/* making an educated guess for miracle miles returns that the same lpn code will be returned to the same FC it shipped from */
INSERT INTO _fact_return_line__return_product (return_line_id, rma_product_id, lpn_code, warehouse_id)
SELECT
    -1 AS return_line_id,
    rpc.rma_product_id,
    fol.lpn_code,
    fol.warehouse_id
FROM _fact_return_line__rma_product_cleanup AS rpc
    JOIN stg.fact_order_line AS fol
        ON fol.order_line_id = rpc.order_line_id;


CREATE OR REPLACE TEMP TABLE _fact_return_line__rma_product AS
SELECT
    rp.rma_id,
    rp.product_id,
    rp.order_line_id,
    MAX(rp.return_reason_id) AS return_reason_id,
    SUM(COALESCE(rp.restocking_fee_amount, 0)) AS restocking_fee_amount
FROM _fact_return_line__return_base AS base
    LEFT JOIN lake_consolidated.ultra_merchant.return AS r
        ON r.return_id = base.return_id
    JOIN lake_consolidated.ultra_merchant.rma_product AS rp
        ON rp.rma_id = COALESCE(r.rma_id, base.rma_id)
GROUP BY
    rp.rma_id,
    rp.product_id,
    rp.order_line_id;
-- SELECT * FROM _fact_return_line__rma_product;

CREATE OR REPLACE TEMP TABLE _fact_return_line__return_line_label AS
SELECT
    rl.return_line_id,
    -1::INT AS rma_product_id,
    rc.label AS return_condition,
    MAX(rd.label) AS return_disposition
FROM _fact_return_line__return_base AS base
    JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.return_id = base.return_id
    LEFT JOIN lake_consolidated.ultra_merchant.return_condition AS rc
        ON rc.code = rl.condition
    LEFT JOIN lake_consolidated.ultra_merchant.return_product AS rp
        ON rp.return_id = rl.return_id
        AND rp.order_line_id = rl.order_line_id
        AND rp.return_condition_id = rc.return_condition_id
    LEFT JOIN lake_consolidated.ultra_merchant.return_disposition AS rd
        ON rd.return_disposition_id = rp.return_disposition_id
GROUP BY
    rc.label,
    rl.return_line_id;
-- SELECT * FROM _fact_return_line__return_line_label;

INSERT INTO _fact_return_line__return_line_label
SELECT
    -1  AS return_line_id,
    rpc.rma_product_id,
    'Unknown' AS return_condition,
    MAX(rd.label) AS return_disposition
FROM _fact_return_line__rma_product_cleanup AS rpc
    LEFT JOIN lake_consolidated.ultra_merchant.return_product AS rp
        ON rp.rma_product_id = rpc.rma_product_id
    LEFT JOIN lake_consolidated.ultra_merchant.return_disposition AS rd
        ON rd.return_disposition_id = rp.return_disposition_id
GROUP BY
    rpc.rma_product_id;


CREATE OR REPLACE TEMP TABLE _fact_return_line__return_reason AS
SELECT
    base.order_line_id,
    rp.rma_id,
    rp.return_reason_id
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.rma_product AS rp
        ON rp.order_line_id = base.order_line_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY rp.rma_id, rp.order_line_id ORDER BY rp.datetime_added DESC) = 1;
-- SELECT * FROM _fact_return_line__return_reason;

CREATE OR REPLACE TEMP TABLE _fact_return_line__return_stores AS
SELECT DISTINCT
    r.order_id,
    NVL(TRY_TO_NUMBER(od.value), IFF((fo.store_id <> fo.cart_store_id OR st.store_type = 'Group Order'), fo.store_id, o.store_id)) AS store_id
FROM _fact_return_line__return_base AS base
    JOIN lake_consolidated.ultra_merchant.return AS r
        ON r.return_id = base.return_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_detail AS od
        ON od.order_id = r.order_id
        AND od.name = 'retail_store_id'
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = r.order_id
    LEFT JOIN stg.fact_order AS fo
        ON fo.order_id = r.order_id
    LEFT JOIN stg.dim_store AS st
        ON st.store_id = fo.store_id;
-- SELECT * FROM _fact_return_line__return_stores;

INSERT INTO _fact_return_line__return_stores
SELECT DISTINCT
    rma.order_id,
    fo.store_id
FROM _fact_return_line__return_base AS base
    JOIN lake_consolidated.ultra_merchant.rma AS rma
        ON rma.rma_id = base.rma_id
    JOIN stg.fact_order AS fo
        ON fo.order_id = rma.order_id
WHERE NOT EXISTS (
    SELECT 1
    FROM _fact_return_line__return_stores AS rs
    WHERE rs.order_id = rma.order_id
);

CREATE OR REPLACE TEMP TABLE _fact_return_line__bops_store_id AS
SELECT
    rl.return_line_id,
    base.order_id,
    base.order_line_id,
    TRY_TO_NUMBER(od.value) AS bops_original_store_id
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.order_line_id = base.order_line_id
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = base.order_id
        AND oc.order_type_id = 40
    JOIN lake_consolidated.ultra_merchant.order_detail AS od
        ON od.order_id = base.order_id
        AND od.name = 'original_store_id';
-- SELECT * FROM _fact_return_line__bops_store_id;


CREATE OR REPLACE TEMP TABLE _fact_return_line__amazon_orders AS
SELECT DISTINCT base.order_id
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = base.order_id
        AND oc.order_type_id in (48, 49);

CREATE OR REPLACE TEMP TABLE _fact_return_line__retail_return_store AS
SELECT
       rrd.return_id,
       rr.store_id AS retail_return_store_id
FROM _fact_return_line__return_base AS base
JOIN lake_consolidated.ultra_merchant.retail_return_detail AS rrd
    ON rrd.return_id = base.return_id
JOIN lake_consolidated.ultra_merchant.retail_return rr
    ON rrd.retail_return_id = rr.retail_return_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY rrd.return_id ORDER BY rr.datetime_modified DESC) = 1;
-- SELECT * FROM _fact_return_line__retail_return_store;

CREATE OR REPLACE TEMP TABLE _fact_return_line__returned_product_cost AS
SELECT
    rl.return_line_id,
    -1::INT AS rma_product_id,
    IFNULL(rl.quantity * COALESCE(fol.estimated_landed_cost_local_amount, 0) *
        (1-(pcm.markdown_adjustment_factor/100)) / NULLIF(fol.item_quantity, 0),
        0) AS estimated_returned_product_cost_local_amount
FROM _fact_return_line__order_line_base AS base
    JOIN stg.fact_order_line AS fol
        ON base.order_line_id = fol.order_line_id
    JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.order_line_id = fol.order_line_id
    LEFT JOIN reference.product_cost_markdown_adjustment AS pcm
        ON pcm.store_id = fol.store_id;
-- SELECT * FROM _fact_return_line__returned_product_cost;

INSERT INTO _fact_return_line__returned_product_cost
SELECT
    - 1 AS return_line_id,
    rp.rma_product_id,
    IFNULL(rl.quantity * COALESCE(fol.estimated_landed_cost_local_amount, 0) *
        (1-(pcm.markdown_adjustment_factor/100)) / NULLIF(fol.item_quantity, 0),
        0) AS estimated_returned_product_cost_local_amount
FROM _fact_return_line__order_line_base AS base
    JOIN stg.fact_order_line AS fol
        ON base.order_line_id = fol.order_line_id
    JOIN _fact_return_line__rma_line_cleanup AS rl
        ON rl.order_line_id = fol.order_line_id
    JOIN _fact_return_line__rma_product_cleanup AS rp
        ON rp.order_line_id = fol.order_line_id
        AND rp.rma_id = rl.rma_id
    LEFT JOIN reference.product_cost_markdown_adjustment AS pcm
        ON pcm.store_id = fol.store_id;

CREATE OR REPLACE TEMP TABLE _fact_return_line__exchanges AS
SELECT DISTINCT rl.return_line_id,
                -1::INT AS rma_product_id
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.exchange AS e
        ON e.original_order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.exchange_line AS el
        ON el.exchange_id = e.exchange_id
        AND el.original_product_id = base.product_id
    JOIN lake_consolidated.ultra_merchant.statuscode AS sc
        ON sc.statuscode = e.statuscode
    JOIN lake_consolidated.ultra_merchant.return AS r
        ON r.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.return_id = r.return_id
        AND rl.product_id = base.product_id
WHERE sc.label = 'Complete';

INSERT INTO _fact_return_line__exchanges (return_line_id, rma_product_id)
SELECT DISTINCT
    -1 AS return_line_id,
    rp.rma_product_id
FROM _fact_return_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.exchange AS e
        ON e.original_order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.exchange_line AS el
        ON el.exchange_id = e.exchange_id
        AND el.original_product_id = base.product_id
    JOIN lake_consolidated.ultra_merchant.statuscode AS sc
        ON sc.statuscode = e.statuscode
    JOIN _fact_return_line__rma_product_cleanup AS rp
        ON rp.order_line_id = base.order_line_id
WHERE sc.label = 'Complete';

-- Getting the transit date of third party rmas and using that in the return completion and receipt datetimes
CREATE OR REPLACE TEMP TABLE _fact_return_line__rma_transit AS
SELECT DISTINCT
    COALESCE(r.rma_id, base.rma_id) AS rma_id,
    MIN(sml.datetime_added) AS rma_transit_datetime
FROM _fact_return_line__return_base AS base
    LEFT JOIN lake_consolidated.ultra_merchant.return AS r
        ON r.return_id = base.return_id
    JOIN lake_consolidated.ultra_merchant.rma AS rma
        ON rma.rma_id = COALESCE(r.rma_id, base.rma_id)
    JOIN lake_consolidated.ultra_merchant.statuscode_modification_log AS sml
        ON sml.object_id = rma.meta_original_rma_id
        AND sml.object = 'rma'
        AND to_statuscode = 4656
GROUP BY COALESCE(r.rma_id, base.rma_id);
-- SELECT * FROM _fact_return_line__rma_transit


CREATE OR REPLACE TEMP TABLE _fact_return_line__data AS
SELECT DISTINCT
    rl.return_line_id,
    -1::INT AS rma_product_id,
    rl.meta_original_return_line_id,
    -1::INT AS meta_original_rma_product_id,
    r.return_id,
    COALESCE(rl.order_line_id,-1) AS order_line_id,
    r.order_id,
    o.customer_id,
    rl.product_id,
    COALESCE(bops.bops_original_store_id,rs.store_id) AS store_id,
    r.rma_id,
    COALESCE(rma.administrator_id,-1) AS administrator_id,
    r.return_category_id,
    rmal.carrier AS return_carrier,
    rma.rma_source_id AS return_source_id,
    COALESCE(rp.warehouse_id, -1) AS warehouse_id,
    COALESCE(drs.return_status_key, -1) AS return_status_key,
    COALESCE(drc.return_condition_key, -1) AS return_condition_key,
    COALESCE(rr.return_reason_id, -1) AS return_reason_id,
    COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(rma.datetime_added::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31') AS return_request_local_datetime,
    CASE
        WHEN oc.order_id IS NOT NULL
                 AND DATEDIFF(day, rmat.rma_transit_datetime::date, COALESCE(rl.datetime_added, r.datetime_added, '9999-12-31')::date) > 30
                 THEN CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(rmat.rma_transit_datetime::timestamp_ntz, 'America/Los_Angeles'))
        ELSE COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(COALESCE(rl.datetime_added, r.datetime_added)::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31')
    END AS return_receipt_local_datetime,
    CASE
        WHEN oc.order_id IS NOT NULL
                 AND DATEDIFF(day, rmat.rma_transit_datetime::date, COALESCE(r.datetime_resolved,rl.datetime_added, r.datetime_added, '9999-12-31')::date) > 30
                 THEN CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(rmat.rma_transit_datetime::timestamp_ntz, 'America/Los_Angeles'))
        WHEN r.statuscode = 4545 THEN CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(COALESCE(r.datetime_resolved,return_receipt_local_datetime)::timestamp_ntz, 'America/Los_Angeles'))
        ELSE '9999-12-31'
    END AS return_completion_local_datetime,
    COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(rma.datetime_resolved::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31') AS rma_resolution_local_datetime,
    COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(rmat.rma_transit_datetime::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31') AS rma_transit_local_datetime,
    IFF(e.return_line_id IS NOT NULL, TRUE, FALSE) AS is_exchange,
    rl.quantity AS return_item_quantity,
    0::NUMBER(19, 4) AS return_product_cost_local_amount,
    rlr.return_line_ratio_of_total::NUMBER(18, 6) AS return_line_ratio_of_total,
    COALESCE(rmap.restocking_fee_amount, 0)::NUMBER(19, 4) AS return_restocking_fee_local_amount,
    vrh.rate::NUMBER(18, 6) AS effective_vat_rate,
    COALESCE(rlv.return_tax_local_amount, 0)::NUMBER(19, 4) AS return_tax_local_amount,
    COALESCE(rlv.return_subtotal_local_amount, 0)::NUMBER(19, 4) AS return_subtotal_local_amount,
    COALESCE(rlv.return_discount_local_amount, 0)::NUMBER(19, 4) AS return_discount_local_amount,
    COALESCE(rlv.return_cash_credit_local_amount, 0)::NUMBER(19, 4) AS return_cash_credit_local_amount,
    COALESCE(rlv.return_non_cash_credit_local_amount,0)::NUMBER(19, 4) AS return_non_cash_credit_local_amount,
    COALESCE(rrs.retail_return_store_id,-1) AS retail_return_store_id,
    COALESCE(rpc.estimated_returned_product_cost_local_amount,0)::NUMBER(19, 4) AS estimated_returned_product_cost_local_amount
FROM _fact_return_line__return_base AS base
    JOIN lake_consolidated.ultra_merchant.return AS r
        ON r.return_id = base.return_id
    LEFT JOIN lake_consolidated.ultra_merchant.return_line AS rl
        ON rl.return_id = r.return_id
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = r.order_id
    LEFT JOIN _fact_return_line__return_stores AS rs
        ON rs.order_id = r.order_id
    LEFT JOIN _fact_return_line__return_line_values AS rlv
        ON rlv.return_line_id = rl.return_line_id
    LEFT JOIN _fact_return_line__return_product AS rp
        ON rp.return_line_id = rl.return_line_id
    LEFT JOIN _fact_return_line__return_line_ratio AS rlr
        ON rlr.return_line_id = rl.return_line_id
    LEFT JOIN _fact_return_line__rma_product AS rmap
        ON rmap.rma_id = r.rma_id
        AND rmap.product_id = rl.product_id
        AND rmap.order_line_id = rl.order_line_id
    LEFT JOIN _fact_return_line__return_reason AS rr
        ON rr.rma_id = r.rma_id
        AND rr.order_line_id = rl.order_line_id
    LEFT JOIN lake_consolidated.ultra_merchant.rma AS rma
        ON rma.rma_id = r.rma_id
    LEFT JOIN lake_consolidated.ultra_merchant.rma_label AS rmal
        ON rmal.rma_id = rma.rma_id
    LEFT JOIN _fact_return_line__return_line_label AS rll
        ON rll.return_line_id = rl.return_line_id
    LEFT JOIN stg.dim_return_status AS drs
        ON drs.return_status_code = r.statuscode
    LEFT JOIN stg.dim_return_condition AS drc
        ON drc.return_condition = rll.return_condition
        AND drc.return_disposition = COALESCE(rll.return_disposition, 'NA')
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = rs.store_id
    LEFT JOIN lake_consolidated.ultra_merchant.address AS a
        ON o.shipping_address_id = a.address_id
    LEFT JOIN _fact_return_line__bops_store_id AS bops
        ON bops.return_line_id = rl.return_line_id
    LEFT JOIN reference.vat_rate_history AS vrh
        ON vrh.country_code = REPLACE(UPPER(a.country_code), 'UK', 'GB')
        AND IFF(o.order_id IS NOT NULL, o.date_placed, o.datetime_added)::DATE BETWEEN vrh.start_date AND vrh.expires_date
    LEFT JOIN _fact_return_line__retail_return_store AS rrs
        ON rrs.return_id = base.return_id
    LEFT JOIN _fact_return_line__returned_product_cost AS rpc
        ON rpc.return_line_id = rl.return_line_id
    LEFT JOIN _fact_return_line__exchanges AS e
        ON e.return_line_id = rl.return_line_id
    LEFT JOIN _fact_return_line__rma_transit AS rmat
        ON rmat.rma_id = r.rma_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = o.order_id
        AND oc.order_type_id = 51
WHERE base.return_id <> -1
ORDER BY rl.return_line_id;
-- SELECT * FROM _fact_return_line__data;

INSERT INTO _fact_return_line__data
SELECT DISTINCT
    -1 AS return_line_id,
    rp.rma_product_id,
    -1 AS meta_original_return_line_id,
    rp.meta_original_rma_product_id,
    -1 AS return_id,
    COALESCE(rp.order_line_id,-1) AS order_line_id,
    r.order_id,
    o.customer_id,
    rp.product_id,
    rs.store_id AS store_id,
    r.rma_id,
    COALESCE(r.administrator_id,-1) AS administrator_id,
    -1 AS return_category_id,
    rmal.carrier AS return_carrier,
    r.rma_source_id AS return_source_id,
    -1 AS warehouse_id,
    -1 AS return_status_key,
    -1 AS return_condition_key,
    COALESCE(rr.return_reason_id, -1) AS return_reason_id,
    COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(r.datetime_added::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31') AS return_request_local_datetime,
    COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(rmat.rma_transit_datetime::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31') AS return_receipt_local_datetime,
    COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(rmat.rma_transit_datetime::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31') AS return_completion_local_datetime,
    COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(r.datetime_resolved::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31') AS rma_resolution_local_datetime,
    COALESCE(CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(rmat.rma_transit_datetime::timestamp_ntz, 'America/Los_Angeles')), '9999-12-31') AS rma_transit_local_datetime,
    IFF(e.return_line_id IS NOT NULL, TRUE, FALSE) AS is_exchange,
    rlc.quantity AS return_item_quantity,
    0::NUMBER(19, 4) AS return_product_cost_local_amount,
    rlr.return_line_ratio_of_total::NUMBER(18, 6) AS return_line_ratio_of_total,
    COALESCE(rmap.restocking_fee_amount, 0)::NUMBER(19, 4) AS return_restocking_fee_local_amount,
    vrh.rate::NUMBER(18, 6) AS effective_vat_rate,
    COALESCE(rlv.return_tax_local_amount, 0)::NUMBER(19, 4) AS return_tax_local_amount,
    COALESCE(rlv.return_subtotal_local_amount, 0)::NUMBER(19, 4) AS return_subtotal_local_amount,
    COALESCE(rlv.return_discount_local_amount, 0)::NUMBER(19, 4) AS return_discount_local_amount,
    COALESCE(rlv.return_cash_credit_local_amount, 0)::NUMBER(19, 4) AS return_cash_credit_local_amount,
    COALESCE(rlv.return_non_cash_credit_local_amount,0)::NUMBER(19, 4) AS return_non_cash_credit_local_amount,
    COALESCE(rrs.retail_return_store_id,-1) AS retail_return_store_id,
    COALESCE(rpc.estimated_returned_product_cost_local_amount,0)::NUMBER(19, 4) AS estimated_returned_product_cost_local_amount
FROM _fact_return_line__return_base AS base
    JOIN lake_consolidated.ultra_merchant.rma AS r
        ON r.rma_id = base.rma_id
    JOIN _fact_return_line__rma_product_cleanup AS rp
        ON rp.rma_id = base.rma_id
    LEFT JOIN _fact_return_line__rma_line_cleanup AS rlc
        ON rlc.rma_id = r.rma_id
        AND rlc.order_line_id = rp.order_line_id
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = r.order_id
    LEFT JOIN _fact_return_line__return_stores AS rs
        ON rs.order_id = r.order_id
    LEFT JOIN _fact_return_line__return_line_values AS rlv
        ON rlv.rma_product_id = rp.rma_product_id
    LEFT JOIN _fact_return_line__return_line_ratio AS rlr
        ON rlr.rma_product_id = rp.rma_product_id
    LEFT JOIN _fact_return_line__rma_product AS rmap
        ON rmap.rma_id = r.rma_id
        AND rmap.product_id = rp.product_id
        AND rmap.order_line_id = rp.order_line_id
    LEFT JOIN _fact_return_line__return_reason AS rr
        ON rr.rma_id = r.rma_id
        AND rr.order_line_id = rp.order_line_id
    LEFT JOIN lake_consolidated.ultra_merchant.rma_label AS rmal
        ON rmal.rma_id = r.rma_id
    LEFT JOIN _fact_return_line__return_line_label AS rll
        ON rll.rma_product_id = rp.rma_product_id
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = rs.store_id
    LEFT JOIN lake_consolidated.ultra_merchant.address AS a
        ON o.shipping_address_id = a.address_id
    LEFT JOIN reference.vat_rate_history AS vrh
        ON vrh.country_code = REPLACE(UPPER(a.country_code), 'UK', 'GB')
        AND IFF(o.order_id IS NOT NULL, o.date_placed, o.datetime_added)::DATE BETWEEN vrh.start_date AND vrh.expires_date
    LEFT JOIN _fact_return_line__retail_return_store AS rrs
        ON rrs.return_id = base.return_id
    LEFT JOIN _fact_return_line__returned_product_cost AS rpc
        ON rp.rma_product_id = rpc.rma_product_id
    LEFT JOIN _fact_return_line__exchanges AS e
        ON e.rma_product_id = rpc.rma_product_id
    LEFT JOIN _fact_return_line__rma_transit AS rmat
        ON rmat.rma_id = r.rma_id
WHERE base.rma_id <> -1
ORDER BY rp.rma_product_id;


/* For gift orders, we want the customer_id to remain tied to the sender instead of it being overwritten with the recipient customer_id */
UPDATE _fact_return_line__data AS rld
SET rld.customer_id = gft.sender_customer_id
FROM (
        SELECT DISTINCT frl.order_id, gfto.sender_customer_id
        FROM _fact_return_line__data AS frl
            JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
                ON gfto.order_id = frl.order_id
    ) AS gft
WHERE gft.order_id = rld.order_id
    AND NOT EQUAL_NULL(gft.sender_customer_id, rld.customer_id);

/* setting gift order exchange and reship orders to be sender customer_id */
UPDATE _fact_return_line__data AS rld
SET rld.customer_id = er.sender_customer_id
FROM (
        SELECT e.exchange_order_id AS order_id,
               gfto.sender_customer_id
        FROM _fact_return_line__data AS frl
            JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
                ON gfto.order_id = frl.order_id
            JOIN lake_consolidated.ultra_merchant.exchange AS e
                ON e.original_order_id = gfto.order_id
        UNION
        SELECT r.reship_order_id AS order_id,
               gfto.sender_customer_id
        FROM _fact_return_line__data AS frl
            JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
                ON gfto.order_id = frl.order_id
            JOIN lake_consolidated.ultra_merchant.reship AS r
                ON r.original_order_id = gfto.order_id
    ) AS er
WHERE er.order_id = rld.order_id
    AND NOT EQUAL_NULL(er.sender_customer_id, rld.customer_id);


CREATE OR REPLACE TEMP TABLE _fact_return_line__customer_info AS
SELECT
    dc.customer_id,
    dc.is_test_customer
FROM (SELECT DISTINCT customer_id FROM _fact_return_line__data) AS dat
    JOIN stg.dim_customer AS dc
        ON dc.customer_id = dat.customer_id;
-- SELECT * FROM _fact_return_line__customer_info;

CREATE OR REPLACE TEMP TABLE _fact_return_line__activation AS
SELECT
    stg.customer_id,
    fa.activation_key,
    fa.membership_event_key,
    fa.activation_local_datetime,
    fa.next_activation_local_datetime,
    fa.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.customer_id ORDER BY fa.activation_sequence_number, fa.activation_local_datetime) AS row_num
FROM (SELECT DISTINCT customer_id FROM _fact_return_line__data) AS stg
    JOIN stg.fact_activation AS fa
    	ON fa.customer_id = stg.customer_id
WHERE NOT NVL(fa.is_deleted, FALSE);
-- SELECT * FROM _fact_return_line__activation;

CREATE OR REPLACE TEMP TABLE _fact_return_line__first_activation AS
SELECT
    stg.return_line_id,
    stg.rma_product_id,
    stg.customer_id,
    a.activation_key,
    a.membership_event_key,
    a.activation_local_datetime,
    a.next_activation_local_datetime,
    a.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.return_line_id, stg.rma_product_id, stg.customer_id ORDER BY a.row_num) AS row_num
FROM _fact_return_line__data AS stg
    JOIN _fact_return_line__activation AS a
		ON a.customer_id = stg.customer_id
        AND a.activation_local_datetime <= stg.return_request_local_datetime;
-- SELECT * FROM _fact_return_line__first_activation;

CREATE OR REPLACE TEMP TABLE _fact_return_line__keys AS
SELECT
    dat.return_line_id,
    dat.rma_product_id,
    dat.customer_id,
    COALESCE(fa.activation_key, -1) AS activation_key,
    COALESCE(ffa.activation_key, -1) AS first_activation_key
FROM _fact_return_line__data AS dat
    LEFT JOIN _fact_return_line__activation AS fa
        ON fa.customer_id = dat.customer_id
        AND dat.return_request_local_datetime >= fa.activation_local_datetime
        AND dat.return_request_local_datetime < fa.next_activation_local_datetime
    LEFT JOIN _fact_return_line__first_activation AS ffa
        ON ffa.return_line_id = dat.return_line_id
        AND ffa.rma_product_id = dat.rma_product_id
        AND ffa.customer_id = dat.customer_id
        AND ffa.row_num = 1
WHERE dat.return_line_id IS NOT NULL;
-- SELECT * FROM _fact_return_line__keys;

CREATE OR REPLACE TEMP TABLE _fact_return_line__item_price_key AS
SELECT
    dat.return_line_id,
    dat.rma_product_id,
    dat.order_line_id,
    dat.order_id,
    dat.product_id,
    COALESCE(fol.item_price_key, -1) AS item_price_key
FROM _fact_return_line__data AS dat
    LEFT JOIN stg.fact_order_line AS fol
        ON fol.order_line_id = dat.order_line_id;

UPDATE _fact_return_line__item_price_key AS t
SET t.item_price_key = s.item_price_key
FROM (
        SELECT ipk.return_line_id, ipk.rma_product_id, fol.item_price_key
        FROM _fact_return_line__item_price_key AS ipk
            LEFT JOIN stg.fact_order_line AS fol
                ON fol.order_id = ipk.order_id
                AND fol.product_id = ipk.product_id
        WHERE ipk.order_line_id = -1 AND ipk.item_price_key = -1
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ipk.return_line_id ORDER BY fol.item_price_key DESC, fol.order_line_id DESC) = 1
) AS s
WHERE s.return_line_id = t.return_line_id
    AND s.rma_product_id = t.rma_product_id;

-- get finance assumption measures
CREATE OR REPLACE TEMP TABLE _fact_return_line__finance_assumptions_stg AS
SELECT
    dat.return_id,
    dat.return_line_id,
    dat.rma_product_id,
    IFF(dat.rma_product_id <> -1, dat.rma_id, -1) AS rma_id,
    st.store_brand,
    st.store_region,
    st.store_country,
    IFF(st.store_brand = 'Fabletics' AND st.store_region = 'NA' AND st.store_type IN ('R', 'Retail'), 'Retail', 'Online') AS store_type,
    DATE_TRUNC(MONTH, CAST(dat.return_completion_local_datetime AS DATE)) AS return_completion_local_date
FROM _fact_return_line__data AS dat
    JOIN stg.dim_store AS st
        ON st.store_id = dat.store_id
    JOIN stg.dim_return_status AS rs
        ON rs.return_status_key = dat.return_status_key
WHERE rs.return_status IN ('Resolved') OR dat.rma_product_id <> -1;
-- SELECT * FROM _fact_return_line__finance_assumptions_stg;


CREATE OR REPLACE TEMP TABLE _fact_return_line__finance_assumptions_line_ratio AS
SELECT return_id,
       -1::INT AS rma_id,
       1.000 / count(*) AS line_ratio
FROM _fact_return_line__finance_assumptions_stg
WHERE return_id <> -1
GROUP BY return_id;
-- SELECT * FROM _fact_return_line__finance_assumptions_line_ratio;

INSERT INTO _fact_return_line__finance_assumptions_line_ratio
SELECT -1 AS return_id,
       rma_id,
       1.000 / count(*) AS line_ratio
FROM _fact_return_line__finance_assumptions_stg
WHERE rma_product_id <> -1
GROUP BY rma_id;


CREATE OR REPLACE TEMP TABLE _fact_return_line__finance_assumptions AS
SELECT
    stg.return_line_id,
    stg.rma_product_id,
    fa.returned_product_resaleable_percent AS estimated_returned_product_resaleable_pct,
    fa.return_shipping_cost_per_order,
    fa.return_shipping_cost_per_order * falr.line_ratio AS estimated_return_shipping_cost_local_amount
FROM _fact_return_line__finance_assumptions_stg AS stg
    JOIN reference.finance_assumption AS fa
        ON fa.brand = stg.store_brand
        AND IFF(fa.region_type = 'Region', stg.store_region, stg.store_country) = fa.region_type_mapping
        AND fa.financial_date = stg.return_completion_local_date
        AND fa.store_type = stg.store_type
    LEFT JOIN _fact_return_line__finance_assumptions_line_ratio falr
        ON falr.return_id = stg.return_id
        AND falr.rma_id = stg.rma_id;
-- SELECT * FROM _fact_return_line__finance_assumptions;

CREATE OR REPLACE TEMP TABLE _fact_return_line__dropship_orders AS
SELECT rl.order_line_id,
       olsm.master_order_line_id,
       olsm.master_order_id
FROM _fact_return_line__data rl
         LEFT JOIN lake_consolidated.ultra_merchant.order_line_split_map olsm
                   ON rl.order_line_id = olsm.order_line_id
         JOIN stg.fact_order_line fo
              ON rl.order_line_id = fo.order_line_id
                  AND fo.order_status_key = 8;

CREATE OR REPLACE TEMP TABLE _fact_return_line__stg AS
SELECT DISTINCT /* Eliminate duplicates */
    dat.return_line_id,
    dat.meta_original_return_line_id,
    dat.rma_product_id,
    dat.meta_original_rma_product_id,
    dat.return_id,
    COALESCE(do.master_order_line_id, dat.order_line_id) AS order_line_id,
    dat.order_line_id                                    AS source_order_line_id,
    COALESCE(do.master_order_id, dat.order_id)           AS order_id,
    dat.order_id                                         AS source_order_id,
    dat.customer_id,
    COALESCE(key.activation_key, -1) AS activation_key,
    COALESCE(key.first_activation_key, -1) AS first_activation_key,
    dat.product_id,
    CASE WHEN dat.store_id = 41 THEN 26 ELSE dat.store_id END AS store_id,
    dat.rma_id,
    dat.administrator_id,
    dat.return_category_id,
    dat.return_carrier,
    dat.return_source_id,
    dat.warehouse_id,
    dat.return_status_key,
    dat.return_condition_key,
    dat.return_reason_id,
    dat.return_request_local_datetime,
    dat.return_receipt_local_datetime,
    dat.return_completion_local_datetime,
    dat.rma_resolution_local_datetime,
    dat.rma_transit_local_datetime,
    dat.is_exchange,
    dat.return_item_quantity,
    COALESCE(dat.return_product_cost_local_amount,0) AS return_product_cost_local_amount,
    COALESCE(dat.return_restocking_fee_local_amount, 0) AS return_restocking_fee_local_amount,
    dat.effective_vat_rate,
    COALESCE(dat.return_tax_local_amount, 0) AS return_tax_local_amount,
    COALESCE(dat.return_subtotal_local_amount, 0) AS return_subtotal_local_amount,
    COALESCE(dat.return_discount_local_amount, 0) AS return_discount_local_amount,
    COALESCE(dat.return_cash_credit_local_amount,0) AS return_cash_credit_local_amount,
    COALESCE(dat.return_non_cash_credit_local_amount, 0) AS return_non_cash_credit_local_amount,
    usd_cer.exchange_rate AS return_receipt_date_usd_conversion_rate,
    eur_cer.exchange_rate AS return_receipt_date_eur_conversion_rate,
    COALESCE(fa.estimated_returned_product_resaleable_pct, 0)::NUMBER(19, 4) AS estimated_returned_product_resaleable_pct,
    COALESCE(fa.estimated_return_shipping_cost_local_amount, 0)::NUMBER(19, 4) AS estimated_return_shipping_cost_local_amount,
    dat.return_line_ratio_of_total,
    IFNULL(ci.is_test_customer, FALSE) AS is_test_customer,
    IFF(a.order_id IS NOT NULL, TRUE, FALSE) AS is_deleted,
    CASE WHEN dat.retail_return_store_id = 41 THEN 26 ELSE dat.retail_return_store_id END AS retail_return_store_id,
    dat.estimated_returned_product_cost_local_amount,
    COALESCE(ipk.item_price_key, -1) AS item_price_key,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_return_line__data AS dat
    LEFT JOIN _fact_return_line__keys AS key
        ON key.return_line_id = dat.return_line_id
        AND key.rma_product_id = dat.rma_product_id
    LEFT JOIN _fact_return_line__return_stores AS rs
        ON rs.order_id = dat.order_id
    LEFT JOIN stg.dim_store AS ds
        ON ds.store_id = rs.store_id
        AND ds.is_current
    LEFT JOIN _fact_return_line__amazon_orders a
        ON a.order_id = dat.order_id
    LEFT JOIN reference.currency_exchange_rate_by_date AS usd_cer
        ON usd_cer.rate_date_pst = dat.return_receipt_local_datetime::TIMESTAMP_LTZ::DATE
        AND usd_cer.src_currency = ds.store_currency
        AND LOWER(usd_cer.dest_currency) = 'usd'
    LEFT JOIN reference.currency_exchange_rate_by_date AS eur_cer
        ON eur_cer.rate_date_pst = dat.return_receipt_local_datetime::TIMESTAMP_LTZ::DATE
        AND eur_cer.src_currency = ds.store_currency
        AND LOWER(eur_cer.dest_currency) = 'eur'
    LEFT JOIN _fact_return_line__finance_assumptions AS fa
        ON fa.return_line_id = dat.return_line_id
        AND fa.rma_product_id = dat.rma_product_id
    LEFT JOIN _fact_return_line__customer_info AS ci
        ON ci.customer_id = dat.customer_id
    LEFT JOIN _fact_return_line__dropship_orders do
        ON dat.order_line_id = do.order_line_id
    LEFT JOIN _fact_return_line__item_price_key AS ipk
        ON ipk.return_line_id = dat.return_line_id
        AND ipk.rma_product_id = dat.rma_product_id
WHERE dat.return_line_id IS NOT NULL;
-- SELECT * FROM _fact_return_line__stg;
-- SELECT return_line_id, COUNT(1) FROM _fact_return_line__stg GROUP BY 1 HAVING COUNT(1) > 1;

/* soft deleting rma records for third party orders if the return got fully processed at FC and created a return_id record */
INSERT INTO _fact_return_line__stg (
    return_line_id,
    rma_product_id,
    meta_original_return_line_id,
    meta_original_rma_product_id,
    return_id,
    order_line_id,
    order_id,
    customer_id,
    activation_key,
    first_activation_key,
    product_id,
    store_id,
    rma_id,
    administrator_id,
    return_category_id,
    return_carrier,
    return_source_id,
    warehouse_id,
    return_status_key,
    return_condition_key,
    return_reason_id,
    return_request_local_datetime,
    return_receipt_local_datetime,
    return_completion_local_datetime,
    rma_resolution_local_datetime,
    rma_transit_local_datetime,
    is_exchange,
    return_item_quantity,
    return_product_cost_local_amount,
    return_restocking_fee_local_amount,
    effective_vat_rate,
    return_tax_local_amount,
    return_subtotal_local_amount,
    return_discount_local_amount,
    return_cash_credit_local_amount,
    return_non_cash_credit_local_amount,
    return_receipt_date_usd_conversion_rate,
    return_receipt_date_eur_conversion_rate,
    estimated_returned_product_resaleable_pct,
    estimated_return_shipping_cost_local_amount,
    return_line_ratio_of_total,
    is_test_customer,
    is_deleted,
    retail_return_store_id,
    estimated_returned_product_cost_local_amount,
    source_order_line_id,
    source_order_id,
    item_price_key,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    frl.return_line_id,
    frl.rma_product_id,
    frl.meta_original_return_line_id,
    frl.meta_original_rma_product_id,
    frl.return_id,
    frl.order_line_id,
    frl.order_id,
    frl.customer_id,
    frl.activation_key,
    frl.first_activation_key,
    frl.product_id,
    frl.store_id,
    frl.rma_id,
    frl.administrator_id,
    frl.return_category_id,
    frl.return_carrier,
    frl.return_source_id,
    frl.warehouse_id,
    frl.return_status_key,
    frl.return_condition_key,
    frl.return_reason_id,
    frl.return_request_local_datetime,
    frl.return_receipt_local_datetime,
    frl.return_completion_local_datetime,
    frl.rma_resolution_local_datetime,
    frl.rma_transit_local_datetime,
    frl.is_exchange,
    frl.return_item_quantity,
    frl.return_product_cost_local_amount,
    frl.return_restocking_fee_local_amount,
    frl.effective_vat_rate,
    frl.return_tax_local_amount,
    frl.return_subtotal_local_amount,
    frl.return_discount_local_amount,
    frl.return_cash_credit_local_amount,
    frl.return_non_cash_credit_local_amount,
    frl.return_receipt_date_usd_conversion_rate,
    frl.return_receipt_date_eur_conversion_rate,
    frl.estimated_returned_product_resaleable_pct,
    frl.estimated_return_shipping_cost_local_amount,
    frl.return_line_ratio_of_total,
    frl.is_test_customer,
    TRUE AS is_deleted,
    frl.retail_return_store_id,
    frl.estimated_returned_product_cost_local_amount,
    frl.source_order_line_id,
    frl.source_order_id,
    frl.item_price_key,
    $execution_start_time,
    $execution_start_time
FROM stg.fact_return_line AS frl
    JOIN _fact_return_line__stg AS frl_stg
        ON frl_stg.order_line_id = frl.order_line_id
WHERE
    frl.rma_product_id <> -1
    AND frl.return_line_id = -1
    AND frl_stg.return_line_id <> -1
    AND frl_stg.rma_product_id = -1
    AND frl.is_deleted = FALSE;


INSERT INTO stg.fact_return_line_stg (
    return_line_id,
    rma_product_id,
    meta_original_return_line_id,
    meta_original_rma_product_id,
    return_id,
    order_line_id,
    order_id,
    customer_id,
    activation_key,
    first_activation_key,
    product_id,
    store_id,
    rma_id,
    administrator_id,
    return_category_id,
    return_carrier,
    return_source_id,
    warehouse_id,
    return_status_key,
    return_condition_key,
    return_reason_id,
    return_request_local_datetime,
    return_receipt_local_datetime,
    return_completion_local_datetime,
    rma_resolution_local_datetime,
    rma_transit_local_datetime,
    is_exchange,
    return_item_quantity,
    return_product_cost_local_amount,
    return_restocking_fee_local_amount,
    effective_vat_rate,
    return_tax_local_amount,
    return_subtotal_local_amount,
    return_discount_local_amount,
    return_cash_credit_local_amount,
    return_non_cash_credit_local_amount,
    return_receipt_date_usd_conversion_rate,
    return_receipt_date_eur_conversion_rate,
    estimated_returned_product_resaleable_pct,
    estimated_return_shipping_cost_local_amount,
    return_line_ratio_of_total,
    is_test_customer,
    is_deleted,
    retail_return_store_id,
    estimated_returned_product_cost_local_amount,
    source_order_line_id,
    source_order_id,
    item_price_key,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    return_line_id,
    rma_product_id,
    meta_original_return_line_id,
    meta_original_rma_product_id,
    return_id,
    order_line_id,
    order_id,
    customer_id,
    activation_key,
    first_activation_key,
    product_id,
    store_id,
    rma_id,
    administrator_id,
    return_category_id,
    return_carrier,
    return_source_id,
    warehouse_id,
    return_status_key,
    return_condition_key,
    return_reason_id,
    return_request_local_datetime,
    return_receipt_local_datetime,
    return_completion_local_datetime,
    rma_resolution_local_datetime,
    rma_transit_local_datetime,
    is_exchange,
    return_item_quantity,
    return_product_cost_local_amount,
    return_restocking_fee_local_amount,
    effective_vat_rate,
    return_tax_local_amount,
    return_subtotal_local_amount,
    return_discount_local_amount,
    return_cash_credit_local_amount,
    return_non_cash_credit_local_amount,
    return_receipt_date_usd_conversion_rate,
    return_receipt_date_eur_conversion_rate,
    estimated_returned_product_resaleable_pct,
    estimated_return_shipping_cost_local_amount,
    return_line_ratio_of_total,
    is_test_customer,
    is_deleted,
    retail_return_store_id,
    estimated_returned_product_cost_local_amount,
    source_order_line_id,
    source_order_id,
    item_price_key,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_return_line__stg
ORDER BY
    return_line_id;
