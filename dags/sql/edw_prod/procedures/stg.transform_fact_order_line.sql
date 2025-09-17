SET target_table = 'stg.fact_order_line';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

-- Switch warehouse if performing a full refresh
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);


-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_lake_ultra_merchant_order_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line'));
SET wm_lake_ultra_merchant_order_line_discount = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line_discount'));
SET wm_lake_ultra_merchant_order_line_token = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line_token'));
SET wm_lake_ultra_merchant_order_product_source = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_product_source'));
SET wm_lake_ultra_merchant_product = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.product'));
SET wm_lake_ultra_merchant_bounceback_endowment = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.bounceback_endowment'));
SET wm_lake_ultra_merchant_order_line_split_map = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line_split_map'));
SET wm_lake_ultra_warehouse_fulfillment = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.fulfillment'));
SET wm_lake_ultra_warehouse_fulfillment_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.fulfillment_item'));
SET wm_lake_ultra_warehouse_invoice = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.invoice'));
SET wm_lake_ultra_warehouse_invoice_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.invoice_item'));
SET wm_edw_stg_dim_product_price_history = (SELECT stg.udf_get_watermark($target_table,'edw_prod.stg.dim_product_price_history'));
SET wm_edw_stg_dim_bundle_component_history = (SELECT stg.udf_get_watermark($target_table,'edw_prod.stg.dim_bundle_component_history'));
SET wm_edw_stg_dim_item_price = (SELECT stg.udf_get_watermark($target_table,'edw_prod.stg.dim_item_price'));
SET wm_edw_stg_dim_related_product = (SELECT stg.udf_get_watermark($target_table,'edw_prod.stg.dim_related_product'));
SET wm_edw_stg_fact_order = (SELECT stg.udf_get_watermark($target_table,'edw_prod.stg.fact_order'));
SET wm_lake_oracle_ebs_landed_cost = (SELECT stg.udf_get_watermark($target_table,'lake.oracle_ebs.landed_cost'));
SET wm_lake_centric_ed_style = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_style'));
SET wm_lake_centric_ed_sku = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_sku'));
SET wm_lake_centric_ed_colorway= (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_colorway'));
SET wm_edw_prod_reference_miracle_miles_landed_cost = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.miracle_miles_landed_cost'));
SET wm_edw_reference_gfc_po_sku_line_lpn_mapping = (SELECT stg.udf_get_watermark($target_table,'edw_prod.reference.gfc_po_sku_line_lpn_mapping'));

CREATE OR REPLACE TEMP TABLE _fact_order_line__order_base (order_id INT);

-- Full Refresh
INSERT INTO _fact_order_line__order_base (order_id)
SELECT ol.order_id
FROM lake_consolidated.ultra_merchant.order_line AS ol
WHERE $is_full_refresh /* SET is_full_refresh = TRUE; */
GROUP BY ol.order_id
ORDER BY ol.order_id;

-- Incremental Refresh
INSERT INTO _fact_order_line__order_base (order_id)
SELECT DISTINCT incr.order_id
FROM (
    SELECT ol.order_id
    FROM lake_consolidated.ultra_merchant.order_line AS ol
        LEFT JOIN lake_consolidated.ultra_merchant.order_line_discount AS old ON old.order_line_id = ol.order_line_id
        LEFT JOIN lake.ultra_warehouse.fulfillment_item AS fi ON fi.foreign_order_line_id = ol.meta_original_order_line_id
    WHERE ol.meta_update_datetime > $wm_lake_ultra_merchant_order_line
        OR old.meta_update_datetime > $wm_lake_ultra_merchant_order_line_discount
        OR fi.hvr_change_time > $wm_lake_ultra_warehouse_fulfillment_item

    UNION ALL

    SELECT ol.order_id
    FROM lake_consolidated.ultra_merchant.order_line AS ol
        JOIN lake_consolidated.ultra_merchant.product AS p ON p.product_id = ol.product_id
        JOIN lake_consolidated.ultra_merchant.product AS mp ON mp.product_id = COALESCE(p.master_product_id, p.product_id)
        LEFT JOIN stg.dim_product_price_history AS dpph ON dpph.product_id = ol.product_id AND dpph.is_current
        LEFT JOIN stg.dim_bundle_component_history AS dbh ON dbh.bundle_product_id = mp.product_id AND dbh.is_current
        LEFT JOIN stg.dim_bundle_component_history AS dch ON dch.bundle_component_product_id = mp.product_id AND dch.is_current
        LEFT JOIN stg.dim_related_product AS drp ON drp.master_product_id = mp.product_id
    WHERE p.meta_update_datetime > $wm_lake_ultra_merchant_product
        OR mp.meta_update_datetime > $wm_lake_ultra_merchant_product
        OR dpph.meta_update_datetime > $wm_edw_stg_dim_product_price_history
        OR dbh.meta_update_datetime > $wm_edw_stg_dim_bundle_component_history
        OR dch.meta_update_datetime > $wm_edw_stg_dim_bundle_component_history
        OR drp.meta_update_datetime > $wm_edw_stg_dim_related_product

    UNION ALL

    SELECT ol.order_id
    FROM lake_consolidated.ultra_merchant.order_line AS ol
        JOIN lake_consolidated.ultra_merchant.product AS p
            ON p.product_id = ol.product_id
        LEFT JOIN lake_consolidated.ultra_merchant.item AS it
            ON it.item_id = ol.item_id||ol.meta_company_id
        LEFT JOIN stg.dim_item_price AS dip
            ON dip.product_id = ol.product_id
            AND dip.item_id = COALESCE(it.item_id, p.item_id)
    WHERE dip.meta_update_datetime > $wm_edw_stg_dim_item_price

    UNION ALL

    SELECT o.order_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        LEFT JOIN lake_consolidated.ultra_merchant.order_product_source opc ON opc.order_id = o.order_id
    WHERE o.meta_update_datetime > $wm_lake_ultra_merchant_order
        OR opc.meta_update_datetime > $wm_lake_ultra_merchant_order_product_source

    UNION ALL

    SELECT o.order_id
    FROM stg.fact_order AS o
    WHERE o.meta_update_datetime > $wm_edw_stg_fact_order

    UNION ALL

    SELECT o.order_id
    FROM lake.ultra_warehouse.invoice AS i
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.meta_original_order_id = i.foreign_order_id
        LEFT JOIN lake.ultra_warehouse.invoice_item ii ON i.invoice_id = ii.invoice_id
    WHERE i.hvr_change_time > $wm_lake_ultra_warehouse_invoice
        OR ii.hvr_change_time > $wm_lake_ultra_warehouse_invoice_item

    UNION ALL

    SELECT o.order_id
    FROM lake.ultra_warehouse.fulfillment AS f
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.meta_original_order_id = f.foreign_order_id
    WHERE f.hvr_change_time > $wm_lake_ultra_warehouse_fulfillment

    UNION ALL

    SELECT o.order_id
    FROM lake.oracle_ebs.landed_cost AS lc
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.meta_original_order_id = lc.sales_order
    WHERE lc.meta_update_datetime > $wm_lake_oracle_ebs_landed_cost
        AND lc.line_number IS NOT NULL
        AND lc.transaction_type_name IN ('TS Customer Shipment', 'TS Retail Shipment/Sale')

    UNION ALL

    SELECT olt.order_id
    FROM lake_consolidated.ultra_merchant.order_line_token AS olt
    WHERE olt.meta_update_datetime > $wm_lake_ultra_merchant_order_line_token

    UNION ALL

    SELECT order_id
    FROM lake_consolidated.ultra_merchant.bounceback_endowment
    WHERE
        meta_update_datetime > $wm_lake_ultra_merchant_bounceback_endowment
        AND order_id IS NOT NULL

    UNION ALL

    SELECT fol.order_id
    FROM lake.centric.ed_style eds
             LEFT JOIN lake.centric.ed_colorway edc
                  ON eds.id = edc.the_parent_id
             JOIN reference.miracle_miles_landed_cost mc -- INNER JOINs on purpose as we are only interested in MM products
                  ON mc.style = eds.code AND mc.colorway_cnl = edc.the_cnl
             LEFT JOIN lake.centric.ed_sku sku
                  ON sku.the_parent_id = eds.id AND edc.id = sku.realized_color
             LEFT JOIN stg.dim_product dp
                  ON sku.tfg_code = dp.sku
             JOIN lake_consolidated.ultra_merchant.order_line fol
                  ON fol.product_id = dp.product_id
    WHERE eds.meta_update_datetime > $wm_lake_centric_ed_style
       OR edc.meta_update_datetime > $wm_lake_centric_ed_colorway
       OR sku.meta_update_datetime > $wm_lake_centric_ed_sku
       OR mc.meta_update_datetime > $wm_edw_prod_reference_miracle_miles_landed_cost

    UNION ALL

    SELECT order_id
    FROM lake_consolidated.ultra_merchant.order_line_split_map
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_order_line_split_map

    UNION ALL

    SELECT master_order_id AS order_id
    FROM lake_consolidated.ultra_merchant.order_line_split_map
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_order_line_split_map

    UNION ALL

    SELECT ol.order_id
    FROM lake_consolidated.ultra_merchant.order_line ol
    JOIN lake.ultra_warehouse.lpn l
        ON ol.lpn_code = l.lpn_code
    JOIN reference.gfc_po_sku_line_lpn_mapping gcl
        ON gcl.lpn_id = l.lpn_id
    WHERE
        gcl.meta_update_datetime > $wm_edw_reference_gfc_po_sku_line_lpn_mapping

    UNION ALL

    SELECT e.order_id
    FROM excp.fact_order_line AS e
    WHERE e.meta_is_current_excp
        AND e.meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.order_id;

/* for dropship splits, we are making sure we are capturing the master order along with all child orders */
INSERT INTO _fact_order_line__order_base (order_id)
SELECT DISTINCT fo.master_order_id AS order_id
FROM _fact_order_line__order_base AS base
    JOIN stg.fact_order AS fo
        ON fo.order_id = base.order_id
WHERE $is_full_refresh = FALSE
    AND fo.order_status_key = 8
    AND fo.master_order_id IS NOT NULL
    AND NOT EXISTS (
        SELECT b.order_id
        FROM _fact_order_line__order_base AS b
        WHERE b.order_id = fo.master_order_id
    );

INSERT INTO _fact_order_line__order_base (order_id)
SELECT DISTINCT fo.order_id
FROM _fact_order_line__order_base AS base
    JOIN stg.fact_order AS fo
        ON fo.master_order_id = base.order_id
WHERE $is_full_refresh = FALSE
    AND fo.order_status_key = 8
    AND NOT EXISTS (
        SELECT b.order_id
        FROM _fact_order_line__order_base AS b
        WHERE b.order_id = fo.order_id
    );

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', current_warehouse()) FROM _fact_order_line__order_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

/* filter legacy orders (store group id < 9) and store_id 1 records from order_base */
DELETE FROM _fact_order_line__order_base AS base
USING (lake_consolidated.ultra_merchant."ORDER" AS o
    JOIN lake_consolidated.ultra_merchant.store AS s
        ON s.store_id = o.store_id)
WHERE o.order_id = base.order_id
    AND (o.store_id = 1 OR s.store_group_id < 9);
-- SELECT * FROM _fact_order_line__order_base;


ALTER TABLE _fact_order_line__order_base CLUSTER BY (order_id);

CREATE OR REPLACE TEMP TABLE _fact_order_line__order_line_base (
    order_id INT,
    order_line_id INT,
    meta_original_order_line_id INT,
    product_id INT,
    master_product_id INT,
    product_type_id INT,
    product_type VARCHAR(100),
    master_product_label VARCHAR(100),
    master_product_alias VARCHAR(100),
    master_product_group_code VARCHAR(50),
    group_key VARCHAR(50),
    extended_price NUMBER(19, 4),
    statuscode INT,
    warehouse_id INT,
    item_id INT,
    is_free BOOLEAN,
    custom_text VARCHAR(100),
    datetime_modified TIMESTAMP_NTZ(3),
    purchase_unit_price NUMBER(19, 4),
    normal_unit_price NUMBER(19, 4),
    member_unit_price NUMBER(19, 4),
    markdown_discount NUMBER(19, 4),
    item_quantity INT
    );

INSERT INTO _fact_order_line__order_line_base
SELECT DISTINCT
    ol.order_id,
    ol.order_line_id,
    ol.meta_original_order_line_id,
    p.product_id,
    mp.product_id AS master_product_id,
--  COALESCE(mp.product_type_id, p.product_type_id, ol.product_type_id) AS product_type_id,
    ol.product_type_id,
    pt.label AS product_type,
    mp.label AS master_product_label,
    mp.alias AS master_product_alias,
    mp.group_code AS master_product_group_code,
    ol.group_key,
    IFF(order_source_id = 9, ol.normal_unit_price, ol.extended_price) AS extended_price,
    ol.statuscode,
    ol.warehouse_id,
    p.item_id,
    COALESCE(TO_BOOLEAN(pt.is_free), FALSE) AS is_free,
    ol.custom_text,
    ol.datetime_modified,
    ol.purchase_unit_price,
    ol.normal_unit_price,
    ol.member_unit_price,
    ol.markdown_discount,
    ol.quantity
FROM _fact_order_line__order_base AS base
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.product AS p
        ON p.product_id = ol.product_id
    JOIN lake_consolidated.ultra_merchant.product AS mp
        ON mp.product_id = COALESCE(p.master_product_id, p.product_id)
    LEFT JOIN lake_consolidated.ultra_merchant.product_type AS pt
--      ON pt.product_type_id = COALESCE(mp.product_type_id, p.product_type_id, ol.product_type_id)
        ON pt.product_type_id = ol.product_type_id;
-- SELECT * FROM _fact_order_line__order_line_base;

ALTER TABLE _fact_order_line__order_line_base CLUSTER BY (order_line_id);

/* mapping order_line_id to master_order_line_id for dropship splits */
CREATE OR REPLACE TEMP TABLE _fact_order_line__dropship_child AS
SELECT
    base.order_line_id,
    olsm.master_order_line_id,
    base.order_id,
    olsm.master_order_id
FROM _fact_order_line__order_line_base AS base
    JOIN stg.fact_order AS fo ON fo.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.order_line_split_map AS olsm
        ON olsm.order_line_id = base.order_line_id
WHERE fo.order_status_key = 8; -- dropship child

/*  DA-29041 - There's an upstream issue where product available to loyalty redemptions are marked as loyalty
    but doesn't change to product_type_id 1 if the customer paid for it with cash/credit.  We are implementing a temporary fix until this is resolved upstream.
    If an order_line_id has product_type_id 11 and subtotal > 0 within a certain subset of SKUs, then are going to flag them "normal" instead of "loyalty"
 */

CREATE OR REPLACE TEMP TABLE _fact_order_line__misclassified_loyalty AS
SELECT order_line_id
FROM _fact_order_line__order_line_base AS base
    JOIN stg.dim_product AS p
        ON p.product_id = base.product_id
WHERE base.product_type_id = 11
    AND p.product_sku IN (SELECT product_sku FROM reference.misclassified_loyalty_skus)
    AND base.extended_price > 0;

UPDATE _fact_order_line__order_line_base AS t
SET
    t.product_type_id = 1,
    t.is_free = FALSE
FROM _fact_order_line__misclassified_loyalty AS s
WHERE s.order_line_id = t.order_line_id;

CREATE OR REPLACE TEMP TABLE _fact_order_line__product_bundle_component AS
SELECT DISTINCT /* Use DISTINCT to prevent dups due to same group_key in multiple bundles (order_id = 578760693) */
    base.order_id,
    base.order_line_id,
    base.group_key,
    b.bundle_product_id,
    base.master_product_id AS component_product_id
FROM _fact_order_line__order_line_base AS base
    JOIN (
        SELECT
            order_id,
            master_product_id AS bundle_product_id,
            group_key
        FROM _fact_order_line__order_line_base
        WHERE LOWER(product_type) = 'bundle'
        ) AS b
        ON b.order_id = base.order_id
        AND EQUAL_NULL(b.group_key, base.group_key)
WHERE LOWER(base.product_type) = 'bundle component';
-- SELECT * FROM _fact_order_line__product_bundle_component;

CREATE OR REPLACE TEMP TABLE _fact_order_line__order_product_source AS
SELECT DISTINCT
    ops.order_id,
    ops.product_id,
    LOWER(TRIM(ops.value)) AS order_product_source_name
FROM lake_consolidated.ultra_merchant.order_product_source AS ops
WHERE EXISTS (SELECT 1 FROM _fact_order_line__order_base AS base WHERE base.order_id = ops.order_id);
-- SELECT * FROM _fact_order_line__order_product_source;

--Get the pre-discount total of each order

CREATE OR REPLACE TEMP TABLE _fact_order_line__order_total AS
SELECT
    base.order_id,
    SUM(COALESCE(base.extended_price, 0)) AS extended_price_total
FROM _fact_order_line__order_line_base AS base
WHERE LOWER(base.product_type) <> 'bundle'
    AND NOT base.is_free
GROUP BY base.order_id;
-- SELECT * FROM _fact_order_line__order_total;

--Retrieve discount amounts for each order line.
--Discount rules starting on 5-15-2014 onwards. Use the amount field from the order_line_discount table.
CREATE OR REPLACE TEMP TABLE _fact_order_line__base_discounts AS
SELECT
    base.order_line_id,
    SUM(IFF(o.order_source_id = 9, COALESCE(base.markdown_discount, 0), COALESCE(old.amount, 0))) AS discount_gross_of_vat_local_amount
FROM _fact_order_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_line_discount AS old
        ON old.order_line_id = base.order_line_id
WHERE o.datetime_added >= '2014-05-15'
GROUP BY base.order_line_id;
-- SUM aggregate required because each order_line_id can have multiple discounts applied from the order_line_discount table.
-- SELECT * FROM _fact_order_line__base_discounts;

--bundle discount
CREATE OR REPLACE TEMP TABLE _fact_order_line__bundle_discounts AS
SELECT
    base.order_line_id,
    base.order_id,
    base.group_key,
    COALESCE(base.extended_price, 0) AS bundle_subtotal,
    COALESCE(bd.discount_gross_of_vat_local_amount, 0) AS bundle_discount,
    ROW_NUMBER() OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.datetime_modified DESC) AS row_num
FROM _fact_order_line__order_line_base AS base
    LEFT JOIN _fact_order_line__base_discounts AS bd
        ON bd.order_line_id = base.order_line_id
WHERE LOWER(base.product_type) = 'bundle'
QUALIFY row_num = 1;
-- SELECT * FROM _fact_order_line__bundle_discounts;

CREATE OR REPLACE TEMP TABLE _fact_order_line__order_line_discounts (
    order_line_id INT,
    discount_gross_of_vat_local_amount NUMBER(18,4)
    );

INSERT INTO _fact_order_line__order_line_discounts
SELECT DISTINCT
    base.order_line_id,
    d.discount_gross_of_vat_local_amount + COALESCE(IFF(bd.bundle_subtotal = 0, 0, bd.bundle_discount * 1.0 * base.extended_price / bd.bundle_subtotal), 0) AS discount_gross_of_vat_local_amount
FROM _fact_order_line__order_line_base AS base
    JOIN _fact_order_line__base_discounts AS d
        ON d.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__bundle_discounts AS bd
        ON bd.order_id = base.order_id
        AND EQUAL_NULL(bd.group_key, base.group_key);

--Discount rules prior to 5-15-2014. Separate out the order.discount value by the pro rated weight of order_line.extended_price.
INSERT INTO _fact_order_line__order_line_discounts
SELECT
    base.order_line_id,
    CASE /* discount amount at order level times ratio of unit_cost at order_line level */
        WHEN COALESCE(base.extended_price, 0) = 0 THEN 0
        WHEN base.is_free THEN 0
        ELSE COALESCE((o.discount * (base.extended_price / ot.extended_price_total)), 0)
    END AS discount_gross_of_vat_local_amount
FROM _fact_order_line__order_line_base AS base
    LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    LEFT JOIN _fact_order_line__order_total AS ot
        ON ot.order_id = o.order_id
WHERE o.datetime_added < '2014-05-15'
    AND LOWER(base.product_type) != 'bundle';
-- SELECT * FROM _fact_order_line__order_line_discounts;

CREATE OR REPLACE TEMP TABLE _fact_order_line__bundle_order_line AS
SELECT DISTINCT
    base.order_line_id,
    bd.order_line_id as bundle_order_line_id,
    COALESCE(vpt.variant_price, bd.bundle_price_offered_local_amount) AS bundle_price_offered_local_amount
FROM _fact_order_line__order_line_base AS base
    JOIN (
        SELECT
            order_line_id,
            meta_original_order_line_id,
            order_id,
            group_key,
            COALESCE(normal_unit_price, purchase_unit_price) AS bundle_price_offered_local_amount,
            ROW_NUMBER() OVER (PARTITION BY order_id, group_key ORDER BY datetime_modified) AS row_num
        FROM _fact_order_line__order_line_base
        WHERE LOWER(product_type) = 'bundle'
        QUALIFY row_num = 1
        ) AS bd
        ON bd.order_id = base.order_id
        AND EQUAL_NULL(bd.group_key, base.group_key)
LEFT JOIN reference.variant_price_test_price_correction AS vpt
    ON vpt.order_line_id = bd.order_line_id
WHERE bd.order_line_id != base.order_line_id
    AND LOWER(base.product_type) = 'bundle component';
-- SELECT * FROM _fact_order_line__bundle_order_line;

-------------------------------------------------
-- aggregate order_line metrics to order level --
-------------------------------------------------

--aggregate the order line totals to the order level

CREATE OR REPLACE TEMP TABLE _fact_order_line__order_agg AS
SELECT /* calculate the order line values after applying any discounts */
    base.order_id,
    SUM(COALESCE(base.extended_price, 0) - COALESCE(old.discount_gross_of_vat_local_amount, 0)) AS order_total,
    SUM(IFF(base.product_type_id <> 22, COALESCE(base.extended_price, 0) - COALESCE(old.discount_gross_of_vat_local_amount, 0), 0)) AS order_total_excl_embroidery,
    SUM(COALESCE(base.extended_price, 0)) AS order_total_gross_discount,
    SUM(IFF(base.product_type_id <> 22, COALESCE(base.extended_price, 0), 0)) AS order_total_gross_discount_excl_embroidery,
    SUM(COALESCE(old.discount_gross_of_vat_local_amount, 0)) AS discount_total,
    SUM(IFF(base.product_type_id = 22, 0, IFNULL(old.discount_gross_of_vat_local_amount,0))) AS discount_total_excl_embroidery,
    SUM(IFF(base.product_type_id <> 22, base.item_quantity, 0)) AS order_total_unit_count
FROM _fact_order_line__order_line_base AS base
    LEFT JOIN _fact_order_line__order_line_discounts AS old
        ON old.order_line_id = base.order_line_id
WHERE LOWER(base.product_type) != 'bundle' /* Filters out Bundle order lines */
    AND base.statuscode != 2830 /* Filters out cancelled order lines */
    AND NOT base.is_free /* Filters out free items */
GROUP BY base.order_id;
-- SELECT * FROM _fact_order_line__order_agg;

/*
   There are orders that only contain loyalty redemptions that a customer could have paid shipping for.
   Because it's a free item, they are getting left out of _fact_order_line__order_agg and the breakout process
   Putting these edge case order in their own table
 */
CREATE OR REPLACE TEMP TABLE _fact_order_line__loyalty_only AS
SELECT
    base.order_line_id,
    fo.loyalty_unit_count as loyalty_order_total_unit_count
FROM _fact_order_line__order_line_base AS base
JOIN stg.fact_order as fo ON fo.order_id = base.order_id
LEFT JOIN _fact_order_line__order_agg  as oa ON oa.order_id = fo.order_id
WHERE fo.subtotal_excl_tariff_local_amount = 0
AND base.product_type_id = 11
AND oa.order_id IS NULL
AND fo.unit_count = fo.loyalty_unit_count
AND base.statuscode <> 2830;

CREATE OR REPLACE TEMP TABLE _fact_order_line__order_details AS
SELECT
    base.order_id,
    base.order_line_id,
    COALESCE(oa.order_total, 0) AS order_total,
    COALESCE(oa.order_total_unit_count, 0) AS order_total_unit_count,
    COALESCE(old.discount_gross_of_vat_local_amount, 0) AS discount_gross_of_vat_local_amount,
    CASE
        WHEN lo.order_line_id IS NOT NULL THEN TO_NUMBER(base.item_quantity / lo.loyalty_order_total_unit_count, 18, 6)
        WHEN base.statuscode = 2830 OR base.is_free THEN 0 /* Cancelled and Free Order lines */
        WHEN COALESCE(oa.order_total, 0) = 0 AND COALESCE(oa.order_total_gross_discount,0) > 0 THEN TO_NUMBER(base.extended_price / oa.order_total_gross_discount, 18, 6)
        WHEN COALESCE(oa.order_total_gross_discount, 0) = 0 AND COALESCE(oa.order_total_unit_count, 0) > 0 THEN TO_NUMBER(base.item_quantity / oa.order_total_unit_count, 18, 6)
        WHEN COALESCE(oa.order_total, 0) = 0 THEN 0
        ELSE TO_NUMBER(((base.extended_price - old.discount_gross_of_vat_local_amount) / oa.order_total), 18, 6)
    END AS order_line_ratio_of_total, /* ratio of order line to order */
    CASE
        WHEN lo.order_line_id IS NOT NULL THEN TO_NUMBER(base.item_quantity / lo.loyalty_order_total_unit_count, 18, 6)
        WHEN base.statuscode = 2830 OR base.is_free THEN 0 /* Cancelled and Free Order lines */
        WHEN COALESCE(oa.order_total_excl_embroidery, 0) = 0 AND oa.order_total_gross_discount_excl_embroidery > 0 THEN TO_NUMBER(base.extended_price / oa.order_total_gross_discount_excl_embroidery, 18, 6)
        WHEN COALESCE(oa.order_total_gross_discount_excl_embroidery, 0) = 0 AND COALESCE(oa.order_total_unit_count, 0) > 0 THEN TO_NUMBER(base.item_quantity / oa.order_total_unit_count, 18, 6)
        WHEN COALESCE(oa.order_total_excl_embroidery, 0) = 0 THEN 0
        WHEN base.product_type_id = 22 THEN 0 /* embroidery */
        ELSE TO_NUMBER(((base.extended_price - old.discount_gross_of_vat_local_amount) / oa.order_total_excl_embroidery), 18, 6)
    END AS order_line_ratio_of_total_excl_embroidery, /* ratio of order line to order */
    CASE
        WHEN COALESCE(oa.discount_total, 0) = 0 THEN 0
        WHEN base.statuscode = 2830 OR base.is_free THEN 0 /* Cancelled and Free Order lines */
        ELSE TO_NUMBER((old.discount_gross_of_vat_local_amount / oa.discount_total), 18, 6)
    END AS discount_order_line_ratio_of_total, /* ratio of discount order line amount to order */
    CASE
        WHEN COALESCE(oa.discount_total_excl_embroidery, 0) = 0 THEN 0
        WHEN base.statuscode = 2830 OR base.is_free THEN 0
        WHEN base.product_type_id = 22 THEN 0
        ELSE TO_NUMBER((old.discount_gross_of_vat_local_amount / oa.discount_total_excl_embroidery), 18, 6)
    END AS discount_order_line_ratio_of_total_excl_embroidery
FROM _fact_order_line__order_line_base AS base
    LEFT JOIN _fact_order_line__order_agg AS oa
        ON oa.order_id = base.order_id
    LEFT JOIN _fact_order_line__order_line_discounts AS old
        ON old.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__loyalty_only as lo
        ON lo.order_line_id = base.order_line_id
WHERE LOWER(base.product_type) != 'bundle'; /* Filters out Bundle order lines */
-- SELECT * FROM _fact_order_line__order_details;

-- tokens data
CREATE OR REPLACE TEMP TABLE _fact_order_line__order_line_token (
    order_line_id NUMBER(38, 0),
    product_type_id NUMBER(38, 0),
    token_count NUMBER(38, 0),
    token_amount NUMBER(19, 4),
    cash_token_count NUMBER(38, 0),
    cash_token_amount NUMBER(19, 4),
    non_cash_token_count NUMBER(38, 0),
    non_cash_token_amount NUMBER(19, 4),
    cash_refund_token_amount NUMBER(19, 4)
    );

INSERT INTO _fact_order_line__order_line_token (
    order_line_id,
    product_type_id,
    token_count,
    token_amount,
    cash_token_count,
    cash_token_amount,
    non_cash_token_count,
    non_cash_token_amount,
    cash_refund_token_amount
    )
SELECT
    base.order_line_id,
    base.product_type_id,
    COUNT(oc.membership_token_id) AS token_count,
    SUM(COALESCE(oc.amount, 0)) AS token_amount,
    COUNT(IFF(mtr.cash = 1, olt.membership_token_id, NULL)) AS cash_token_count,
    SUM(IFF(mtr.cash = 1, COALESCE(oc.amount, 0), 0)) AS cash_token_amount,
    COUNT(IFF(mtr.cash = 0, olt.membership_token_id, NULL)) AS noncash_token_count,
    SUM(IFF(mtr.cash = 0, COALESCE(oc.amount, 0), 0)) AS noncash_token_amount,
    SUM(IFF(mtr.label IN ('Refund', 'Refund - Converted Credit') AND mtr.cash = 1, COALESCE(oc.amount,0),0)) AS cash_refund_token_amount
FROM _fact_order_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.order_line_token AS olt
        ON olt.order_line_id = base.order_line_id
    JOIN lake_consolidated.ultra_merchant.membership_token AS mt
        ON mt.membership_token_id = olt.membership_token_id
    JOIN lake_consolidated.ultra_merchant.membership_token_reason AS mtr
        ON mtr.membership_token_reason_id = mt.membership_token_reason_id
    JOIN lake_consolidated.ultra_merchant.order_credit AS oc
        ON oc.order_id = olt.order_id
        AND oc.membership_token_id = mt.membership_token_id
WHERE LOWER(base.product_type) != 'bundle component'
AND oc.hvr_is_deleted = 0
GROUP BY
    base.order_line_id,
    base.product_type_id;
-- SELECT * FROM _fact_order_line__order_line_token;

CREATE OR REPLACE TEMP TABLE _fact_order_line__bundle_tokens AS
SELECT
    olt.order_line_id,
    olt.product_type_id,
    bd.order_id,
    bd.group_key,
    olt.token_count,
    olt.token_amount,
    olt.cash_token_count,
    olt.cash_token_amount,
    olt.non_cash_token_count,
    olt.non_cash_token_amount,
    olt.cash_refund_token_amount,
    bd.bundle_subtotal
FROM _fact_order_line__order_line_token AS olt
    JOIN _fact_order_line__bundle_discounts bd
        ON olt.order_line_id = bd.order_line_id;
-- SELECT * FROM _fact_order_line__bundle_tokens;

CREATE OR REPLACE TEMP TABLE _fact_order_line__bundle_component_tokens AS
WITH bundle_component_tokens AS (
    SELECT
        base.order_line_id,
        base.group_key,
        ROW_NUMBER() OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.order_line_id DESC) AS row_num,
        ROUND(IFF(bd.token_count = 0, 0, bd.token_count * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2) AS token_count,
        SUM(ROUND(IFF(bd.token_count = 0, 0, bd.token_count * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2)) OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.order_line_id) AS token_count_rt,
        bd.token_count AS token_count_total,
        ROUND(IFF(bd.token_amount = 0, 0, bd.token_amount * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2) AS token_amount,
        SUM(ROUND(IFF(bd.token_amount = 0, 0, bd.token_amount * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2)) OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.order_line_id) AS token_amount_rt,
        bd.token_amount AS token_amount_total,
        ROUND(IFF(bd.cash_token_count = 0, 0, bd.cash_token_count * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2) AS cash_token_count,
        SUM(ROUND(IFF(bd.cash_token_count = 0, 0, bd.cash_token_count * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2)) OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.order_line_id) AS cash_token_count_rt,
        bd.cash_token_count AS cash_token_count_total,
        ROUND(IFF(bd.cash_token_amount = 0, 0, bd.cash_token_amount * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2) AS cash_token_amount,
        SUM(ROUND(IFF(bd.cash_token_amount = 0, 0, bd.cash_token_amount * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2)) OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.order_line_id) AS cash_token_amount_rt,
        bd.cash_token_amount AS cash_token_amount_total,
        ROUND(IFF(bd.non_cash_token_count = 0, 0, bd.non_cash_token_count * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2) AS non_cash_token_count,
        SUM(ROUND(IFF(bd.non_cash_token_count = 0, 0, bd.non_cash_token_count * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2)) OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.order_line_id) AS non_cash_token_count_rt,
        bd.non_cash_token_count AS non_cash_token_count_total,
        ROUND(IFF(bd.non_cash_token_amount = 0, 0, bd.non_cash_token_amount * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2) AS non_cash_token_amount,
        SUM(ROUND(IFF(bd.non_cash_token_amount = 0, 0, bd.non_cash_token_amount * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2)) OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.order_line_id) AS non_cash_token_amount_rt,
        bd.non_cash_token_amount AS non_cash_token_amount_total,
        ROUND(IFF(bd.cash_refund_token_amount = 0, 0, bd.cash_refund_token_amount * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2) AS cash_refund_token_amount,
        SUM(ROUND(IFF(bd.cash_refund_token_amount = 0, 0, bd.cash_refund_token_amount * 1.0 * base.extended_price / COALESCE(bd.bundle_subtotal, 0)), 2)) OVER (PARTITION BY base.order_id, base.group_key ORDER BY base.order_line_id) AS cash_refund_token_amount_rt,
        bd.cash_refund_token_amount AS cash_refund_token_amount_total
    FROM _fact_order_line__order_line_base AS base
        JOIN _fact_order_line__bundle_tokens AS bd
            ON bd.order_id = base.order_id
            AND EQUAL_NULL(bd.group_key, base.group_key)
    WHERE base.product_type_id = 15 /* bundle components */
    )
SELECT
    bct.order_line_id,
    IFF(bct.row_num = 1, bct.token_count_total - bct.token_count_rt + bct.token_count, bct.token_count) AS token_count,
    IFF(bct.row_num = 1, bct.token_amount_total - bct.token_amount_rt + bct.token_amount, bct.token_amount) AS token_amount,
    IFF(bct.row_num = 1, bct.cash_token_count_total - bct.cash_token_count_rt + bct.cash_token_count, bct.cash_token_count) AS cash_token_count,
    IFF(bct.row_num = 1, bct.cash_token_amount_total - bct.cash_token_amount_rt + bct.cash_token_amount, bct.cash_token_amount) AS cash_token_amount,
    IFF(bct.row_num = 1, bct.non_cash_token_count_total - bct.non_cash_token_count_rt + bct.non_cash_token_count, bct.non_cash_token_count) AS non_cash_token_count,
    IFF(bct.row_num = 1, bct.non_cash_token_amount_total - bct.non_cash_token_amount_rt + bct.non_cash_token_amount, bct.non_cash_token_amount) AS non_cash_token_amount,
    IFF(bct.row_num = 1, bct.cash_refund_token_amount_total - bct.cash_refund_token_amount_rt + bct.cash_refund_token_amount, bct.cash_refund_token_amount) AS cash_refund_token_amount
FROM bundle_component_tokens AS bct;
-- SELECT * FROM _fact_order_line__bundle_component_tokens;

CREATE OR REPLACE TEMP TABLE _fact_order_line__warehouse_fulfillment AS
SELECT
    base.order_line_id,
    COALESCE(f.warehouse_id, base.warehouse_id) AS warehouse_id,
    ROW_NUMBER() OVER (PARTITION BY base.order_line_id ORDER BY COALESCE(fi.datetime_modified, base.datetime_modified) DESC) AS row_num
FROM _fact_order_line__order_line_base AS base
    LEFT JOIN lake.ultra_warehouse.fulfillment_item AS fi
        ON fi.foreign_order_line_id = base.meta_original_order_line_id
    LEFT JOIN lake.ultra_warehouse.fulfillment AS f
        ON f.fulfillment_id = fi.fulfillment_id
QUALIFY row_num = 1;
-- SELECT * FROM _fact_order_line__warehouse_fulfillment;

UPDATE _fact_order_line__warehouse_fulfillment AS t
SET t.warehouse_id = s.warehouse_id
FROM (
        SELECT dc.master_order_line_id as order_line_id,
               wf.warehouse_id
        FROM _fact_order_line__dropship_child AS dc
            JOIN _fact_order_line__warehouse_fulfillment AS wf
                ON wf.order_line_id = dc.order_line_id
     ) AS s
WHERE s.order_line_id = t.order_line_id;

CREATE OR REPLACE TEMP TABLE _fact_order_line__warehouse_invoice AS
SELECT
    base.order_line_id,
    wh.warehouse_id
FROM _fact_order_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.item AS i
        ON i.item_id = base.item_id
    JOIN (
        SELECT
            order_base.order_id,
            it.item_number,
            COALESCE(i.warehouse_id, -1) AS warehouse_id,
            ROW_NUMBER() OVER (PARTITION BY order_base.order_id, it.item_number ORDER BY i.warehouse_id) AS row_num
        FROM _fact_order_line__order_base AS order_base
            JOIN lake.ultra_warehouse.invoice AS i
                ON i.foreign_order_id = stg.udf_unconcat_brand(order_base.order_id)
            JOIN lake.ultra_warehouse.invoice_item AS ii
                ON i.invoice_id = ii.invoice_id
            JOIN lake.ultra_warehouse.item AS it
                ON it.item_id = ii.item_id
        QUALIFY row_num = 1
        ) AS wh
        ON wh.order_id = base.order_id
        AND wh.item_number = i.item_number;
-- SELECT * FROM _fact_order_line__warehouse_invoice;

INSERT INTO _fact_order_line__warehouse_invoice
SELECT dc.master_order_line_id as order_line_id,
        wi.warehouse_id
FROM _fact_order_line__dropship_child AS dc
    JOIN _fact_order_line__warehouse_invoice AS wi
        ON wi.order_line_id = dc.order_line_id
WHERE dc.master_order_line_id NOT IN (SELECT order_line_id FROM _fact_order_line__warehouse_invoice);
-----------------------------
-- load initial stg tables --
-----------------------------

CREATE OR REPLACE TEMP TABLE _fact_order_line__stg_order_line_base AS
SELECT
    base.order_line_id,
    base.meta_original_order_line_id,
    base.order_id,
    base.product_id,
    base.master_product_id,
    pbc.bundle_product_id,
    COALESCE(base.product_type_id, -1) AS product_type_id,
    base.master_product_label,
    base.master_product_alias,
    base.master_product_group_code,
    COALESCE(base.group_key, 'Unknown') AS bundle_group_key,
    COALESCE(sc.label, 'Unknown') AS order_line_status,
    COALESCE(bol.bundle_order_line_id, -1) AS bundle_order_line_id,
    bol.bundle_price_offered_local_amount,
    COALESCE(bundle_ops.order_product_source_name, ops.order_product_source_name, 'Unknown') AS order_product_source_name,
    COALESCE(wi.warehouse_id, wf.warehouse_id, -1) AS warehouse_id,
    ol.quantity AS item_quantity,
    base.extended_price,
    ol.extended_shipping_price,
    CASE WHEN ZEROIFNULL(ol.unit_price_adjustment) <= 0 THEN 0 ELSE ZEROIFNULL(ol.unit_price_adjustment) END AS tariff_revenue_local_amount,
    ol.retail_unit_price,
    COALESCE(vpt.variant_price, ol.normal_unit_price, ol.purchase_unit_price) AS price_offered_local_amount,
    IFF(vpt.order_line_id IS NOT NULL AND vpt.product_type_id <> 15, vpt.variant_price, ol.member_unit_price) AS air_vip_price,
    ol.group_key,
    ol.sub_group_key,
    ol.custom_text,
    base.item_id AS product_item_id,
    ol.item_id||ol.meta_company_id AS order_line_item_id,
    base.is_free
FROM _fact_order_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_line_id = base.order_line_id
    LEFT JOIN lake_consolidated.ultra_merchant.statuscode AS sc
        ON sc.statuscode = base.statuscode
    LEFT JOIN _fact_order_line__product_bundle_component AS pbc
        ON pbc.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__bundle_order_line AS bol
        ON bol.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__order_product_source AS bundle_ops
        ON bundle_ops.order_id = base.order_id
        AND bundle_ops.product_id = pbc.bundle_product_id
    LEFT JOIN _fact_order_line__order_product_source AS ops
        ON ops.order_id = base.order_id
        AND ops.product_id = base.product_id
    LEFT JOIN _fact_order_line__warehouse_invoice AS wi
        ON wi.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__warehouse_fulfillment AS wf
        ON wf.order_line_id = base.order_line_id
    LEFT JOIN reference.variant_price_test_price_correction AS vpt
        ON vpt.order_line_id = base.order_line_id
WHERE LOWER(base.product_type) <> 'bundle'; /* Filters out Bundle order lines */
-- SELECT * FROM _fact_order_line__stg_order_line_base;

CREATE OR REPLACE TEMP TABLE _fact_order_line__stg_order_line_amounts AS
SELECT
    base.order_line_id,
    base.order_id,
    base.product_type_id,
    base.sub_group_key,
    od.order_line_ratio_of_total * COALESCE(fo.payment_transaction_local_amount, 0) AS payment_transaction_local_amount, /* pro_rated */
    (base.extended_price / (1 + COALESCE(fo.effective_vat_rate,0))) AS subtotal_excl_tariff_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.tax_local_amount,0) AS tax_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.tax_cash_local_amount,0) AS tax_cash_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.tax_credit_local_amount,0) AS tax_credit_local_amount,
    od.discount_order_line_ratio_of_total * COALESCE(fo.product_discount_local_amount,0) AS product_discount_local_amount,
    od.discount_order_line_ratio_of_total_excl_embroidery * COALESCE(fo.shipping_discount_local_amount,0) AS shipping_discount_local_amount,
    od.order_line_ratio_of_total_excl_embroidery * COALESCE(fo.shipping_cost_local_amount, 0) AS shipping_cost_local_amount,
    od.order_line_ratio_of_total_excl_embroidery * COALESCE(fo.shipping_revenue_before_discount_local_amount,0) AS shipping_revenue_before_discount_local_amount,
    od.order_line_ratio_of_total_excl_embroidery * COALESCE(fo.shipping_revenue_credit_local_amount, 0) AS shipping_revenue_credit_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.cash_credit_local_amount, 0) AS cash_credit_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.cash_membership_credit_local_amount, 0) AS cash_membership_credit_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.cash_refund_credit_local_amount, 0) AS cash_refund_credit_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.cash_giftco_credit_local_amount, 0) AS cash_giftco_credit_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.cash_giftcard_credit_local_amount, 0) AS cash_giftcard_credit_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.non_cash_credit_local_amount, 0) AS non_cash_credit_local_amount,
    od.order_line_ratio_of_total * COALESCE(fo.bounceback_endowment_local_amount, 0) AS bounceback_endowment_local_amount,
	od.order_line_ratio_of_total * COALESCE(fo.vip_endowment_local_amount, 0) AS vip_endowment_local_amount,
    od.order_line_ratio_of_total_excl_embroidery * COALESCE(fo.estimated_shipping_supplies_cost_local_amount, 0) AS estimated_shipping_supplies_cost_local_amount,
    od.order_line_ratio_of_total_excl_embroidery * COALESCE(fo.estimated_shipping_cost_local_amount, 0) AS estimated_shipping_cost_local_amount,
    od.order_line_ratio_of_total_excl_embroidery * COALESCE(fo.estimated_variable_warehouse_cost_local_amount, 0) AS estimated_variable_warehouse_cost_local_amount,
    od.order_line_ratio_of_total_excl_embroidery * COALESCE(fo.estimated_variable_gms_cost_local_amount, 0) AS estimated_variable_gms_cost_local_amount,
    od.order_line_ratio_of_total_excl_embroidery * COALESCE(fo.estimated_variable_payment_processing_pct_cash_revenue, 0) AS estimated_variable_payment_processing_pct_cash_revenue,
    IFF(base.order_line_status = 'Cancelled' OR base.is_free = TRUE OR base.product_type_id = 22, 0, fo.delivery_fee_local_amount / NULLIF(od.order_total_unit_count, 0)) AS delivery_fee_local_amount,
    COALESCE(fo.payment_transaction_local_amount, 0) AS order_total_payment_transaction_local_amount,
    COALESCE(fo.cash_credit_local_amount, 0) AS order_total_cash_credit_local_amount,
    COALESCE(fo.non_cash_credit_local_amount, 0) AS order_total_non_cash_credit_local_amount,
    COALESCE(fo.cash_membership_credit_local_amount, 0) AS order_total_cash_membership_credit_local_amount,
    COALESCE(fo.cash_refund_credit_local_amount, 0) AS order_total_cash_refund_credit_local_amount,
    COALESCE(fo.cash_giftco_credit_local_amount, 0) AS order_total_cash_giftco_credit_local_amount,
    COALESCE(fo.cash_giftcard_credit_local_amount, 0) AS order_total_cash_giftcard_credit_local_amount,
    COALESCE(fo.bounceback_endowment_local_amount, 0) AS order_total_bounceback_endowment_local_amount,
    COALESCE(fo.vip_endowment_local_amount, 0) AS order_total_vip_endowment_local_amount
FROM _fact_order_line__stg_order_line_base AS base
    LEFT JOIN stg.fact_order AS fo
        ON fo.order_id = base.order_id
    LEFT JOIN _fact_order_line__order_details AS od
        ON od.order_line_id = base.order_line_id;
-- SELECT * FROM _fact_order_line__stg_order_line_amounts;

CREATE OR REPLACE TEMP TABLE _fact_order_line__stg_order_line_token_data AS
SELECT
    base.order_line_id,
    base.order_id,
    COALESCE(bct.token_count, olt.token_count, 0) AS token_count,
    COALESCE(bct.token_amount, olt.token_amount, 0) /  (1 + COALESCE(fo.effective_vat_rate,0)) AS token_local_amount,
    COALESCE(bct.cash_token_count, olt.cash_token_count, 0) AS cash_token_count,
    COALESCE(bct.cash_token_amount, olt.cash_token_amount, 0) /  (1 + COALESCE(fo.effective_vat_rate,0)) AS cash_token_local_amount,
    COALESCE(bct.non_cash_token_count, olt.non_cash_token_count, 0) AS non_cash_token_count,
    COALESCE(bct.non_cash_token_amount, olt.non_cash_token_amount, 0) /  (1 + COALESCE(fo.effective_vat_rate,0)) AS non_cash_token_local_amount,
    COALESCE(bct.cash_refund_token_amount, olt.cash_refund_token_amount, 0) /  (1 + COALESCE(fo.effective_vat_rate,0)) AS cash_refund_token_local_amount
FROM _fact_order_line__stg_order_line_base AS base
    LEFT JOIN stg.fact_order AS fo
        ON fo.order_id = base.order_id
    LEFT JOIN _fact_order_line__bundle_component_tokens AS bct
        ON bct.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__order_line_token AS olt
        ON olt.order_line_id = base.order_line_id;
-- SELECT * FROM _fact_order_line__stg_order_line_token_data;

/* for dropship splits, we are applying token amounts to the master order_line_id */
UPDATE _fact_order_line__stg_order_line_token_data AS t
SET t.token_count = s.token_count,
    t.token_local_amount = s.token_local_amount,
    t.cash_token_count = s.cash_token_count,
    t.cash_token_local_amount = s.cash_token_local_amount,
    t.non_cash_token_count = s.non_cash_token_count,
    t.non_cash_token_local_amount = s.non_cash_token_local_amount,
    t.cash_refund_token_local_amount = s.cash_refund_token_local_amount
FROM (
        SELECT
            dc.master_order_line_id AS order_line_id,
            td.token_count,
            td.token_local_amount,
            td.cash_token_count,
            td.cash_token_local_amount,
            td.non_cash_token_count,
            td.non_cash_token_local_amount,
            td.cash_refund_token_local_amount
        FROM _fact_order_line__stg_order_line_token_data AS td
            JOIN _fact_order_line__dropship_child AS dc
                ON dc.order_line_id = td.order_line_id
     ) AS s
WHERE s.order_line_id = t.order_line_id;


CREATE OR REPLACE TEMP TABLE _fact_order_line__token_order_breakout_base AS
SELECT
    base.order_line_id,
    base.order_id,
    base.order_line_status,
    amt.subtotal_excl_tariff_local_amount,
    amt.product_discount_local_amount,
    amt.tax_local_amount,
    amt.tax_cash_local_amount,
    amt.tax_credit_local_amount,
    amt.shipping_revenue_before_discount_local_amount,
    amt.shipping_revenue_credit_local_amount,
    COALESCE(amt.shipping_revenue_before_discount_local_amount, 0) - COALESCE(amt.shipping_revenue_credit_local_amount, 0) AS shipping_revenue_cash_local_amount,
    base.tariff_revenue_local_amount,
    td.token_local_amount,
    td.cash_token_local_amount,
    td.cash_refund_token_local_amount,
    td.non_cash_token_local_amount,
    amt.order_total_payment_transaction_local_amount,
    amt.order_total_cash_credit_local_amount,
    amt.order_total_non_cash_credit_local_amount,
    SUM(td.token_local_amount) OVER(PARTITION BY base.order_id) AS order_total_token_local_amount,
    SUM(td.cash_token_local_amount) OVER(PARTITION BY base.order_id) AS order_total_cash_token_local_amount,
    SUM(td.cash_refund_token_local_amount) OVER(PARTITION BY base.order_id) AS order_total_cash_refund_token_local_amount,
    SUM(td.non_cash_token_local_amount) OVER(PARTITION BY base.order_id) AS order_total_non_cash_token_local_amount,
    amt.order_total_cash_membership_credit_local_amount,
    amt.order_total_cash_refund_credit_local_amount,
    amt.order_total_cash_giftco_credit_local_amount,
    amt.order_total_cash_giftcard_credit_local_amount,
    amt.order_total_bounceback_endowment_local_amount,
    amt.order_total_vip_endowment_local_amount,
    IFF(base.order_line_status = 'Cancelled', 0,
        amt.subtotal_excl_tariff_local_amount
            + base.tariff_revenue_local_amount
            - amt.product_discount_local_amount
            + amt.shipping_revenue_credit_local_amount
            + amt.tax_credit_local_amount
            - td.cash_token_local_amount
            - td.non_cash_token_local_amount
        ) AS amount_to_pay_net_token
FROM _fact_order_line__stg_order_line_base AS base
    LEFT JOIN _fact_order_line__stg_order_line_token_data AS td
        ON td.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__stg_order_line_amounts AS amt
        ON amt.order_line_id = base.order_line_id
QUALIFY order_total_token_local_amount > 0;
--select * from _fact_order_line__token_order_breakout_base;

CREATE OR REPLACE TEMP TABLE _fact_order_line__token_order_credits AS
WITH _fact_order_line__credit_ratio AS (
    SELECT
        order_line_id,
        cash_token_local_amount,
        non_cash_token_local_amount,
        cash_refund_token_local_amount,
        order_total_cash_credit_local_amount,
        order_total_non_cash_credit_local_amount,
        order_total_cash_membership_credit_local_amount,
        order_total_cash_refund_credit_local_amount,
        order_total_cash_giftco_credit_local_amount,
        order_total_cash_giftcard_credit_local_amount,
        order_total_token_local_amount,
        order_total_cash_token_local_amount,
        order_total_non_cash_token_local_amount,
        order_total_cash_refund_token_local_amount,
        order_total_bounceback_endowment_local_amount,
        order_total_vip_endowment_local_amount,
        amount_to_pay_net_token,
        COALESCE(amount_to_pay_net_token / NULLIF(SUM(amount_to_pay_net_token) OVER(PARTITION BY order_id), 0), 0) AS credit_redemption_order_line_ratio
    FROM _fact_order_line__token_order_breakout_base
)
SELECT
    order_line_id,
    cash_token_local_amount + (order_total_cash_credit_local_amount - order_total_cash_token_local_amount) * credit_redemption_order_line_ratio AS cash_credit_local_amount,
    non_cash_token_local_amount + (order_total_non_cash_credit_local_amount - order_total_non_cash_token_local_amount) * credit_redemption_order_line_ratio AS non_cash_credit_local_amount,
    order_total_cash_membership_credit_local_amount * credit_redemption_order_line_ratio AS cash_membership_credit_local_amount,
    cash_refund_token_local_amount + (order_total_cash_refund_credit_local_amount - order_total_cash_refund_token_local_amount) * credit_redemption_order_line_ratio AS cash_refund_credit_local_amount,
    order_total_cash_giftco_credit_local_amount * credit_redemption_order_line_ratio AS cash_giftco_credit_local_amount,
    order_total_cash_giftcard_credit_local_amount * credit_redemption_order_line_ratio AS cash_giftcard_credit_local_amount,
    order_total_bounceback_endowment_local_amount * credit_redemption_order_line_ratio AS bounceback_endowment_local_amount,
    order_total_vip_endowment_local_amount * credit_redemption_order_line_ratio AS vip_endowment_local_amount
FROM _fact_order_line__credit_ratio;
--select * from _fact_order_line__token_order_credits;

CREATE OR REPLACE TEMP TABLE _fact_order_line__token_orders_cash AS
WITH _fact_order_line__cash_gross_rev_ratio AS (
    SELECT
        base.order_line_id,
        base.order_id,
        base.order_total_payment_transaction_local_amount,
        IFF(base.order_line_status = 'Cancelled', 0,
            COALESCE(base.subtotal_excl_tariff_local_amount, 0)
                + COALESCE(base.tariff_revenue_local_amount, 0)
                - COALESCE(base.product_discount_local_amount, 0)
                + COALESCE(base.shipping_revenue_cash_local_amount, 0)
                + COALESCE(base.shipping_revenue_credit_local_amount, 0)
                + COALESCE(base.tax_cash_local_amount, 0)
                + COALESCE(base.tax_credit_local_amount, 0)
                - COALESCE(toc.cash_credit_local_amount, 0)
                - COALESCE(toc.non_cash_credit_local_amount, 0)
            )AS cash_gross_rev_calc
    FROM _fact_order_line__token_order_breakout_base AS base
    LEFT JOIN _fact_order_line__token_order_credits AS toc
        ON toc.order_line_id = base.order_line_id
)

SELECT
    order_line_id,
    cash_gross_rev_calc,
    SUM(cash_gross_rev_calc) OVER(PARTITION BY order_id) AS order_total_cash_gross_rev_calc,
    COALESCE(cash_gross_rev_calc / NULLIF(order_total_cash_gross_rev_calc, 0), 0) AS cash_order_line_ratio,
    order_total_payment_transaction_local_amount * cash_order_line_ratio AS payment_transaction_local_amount /* cash_order_line_ratio calculated one line above */
FROM _fact_order_line__cash_gross_rev_ratio;
 --SELECT * FROM _fact_order_line__token_orders_cash;

CREATE OR REPLACE TEMP TABLE _fact_order_line__token_order_amounts AS
SELECT
    base.order_line_id,
    COALESCE(tocrdt.cash_credit_local_amount, 0) AS cash_credit_local_amount,
    COALESCE(tocrdt.non_cash_credit_local_amount, 0) AS non_cash_credit_local_amount,
    COALESCE(tocrdt.cash_refund_credit_local_amount, 0) AS cash_refund_credit_local_amount,
    COALESCE(tocrdt.cash_giftcard_credit_local_amount, 0) AS cash_giftcard_credit_local_amount,
    COALESCE(tocrdt.cash_giftco_credit_local_amount, 0) AS cash_giftco_credit_local_amount,
    COALESCE(tocrdt.cash_membership_credit_local_amount, 0) AS cash_membership_credit_local_amount,
    COALESCE(tocrdt.bounceback_endowment_local_amount, 0) AS bounceback_endowment_local_amount,
    COALESCE(tocrdt.vip_endowment_local_amount, 0) AS vip_endowment_local_amount,
    COALESCE(toc.payment_transaction_local_amount, 0) AS payment_transaction_local_amount
FROM _fact_order_line__token_order_breakout_base AS base
    LEFT JOIN _fact_order_line__token_order_credits AS tocrdt
        ON tocrdt.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__token_orders_cash AS toc
        ON toc.order_line_id = base.order_line_id;
--select * from _fact_order_line__token_order_amounts;

UPDATE _fact_order_line__stg_order_line_amounts AS t
SET
    t.cash_credit_local_amount = s.cash_credit_local_amount,
    t.non_cash_credit_local_amount = s.non_cash_credit_local_amount,
    t.cash_refund_credit_local_amount = s.cash_refund_credit_local_amount,
    t.cash_giftcard_credit_local_amount = s.cash_giftcard_credit_local_amount,
    t.cash_giftco_credit_local_amount = s.cash_giftco_credit_local_amount,
    t.cash_membership_credit_local_amount = s.cash_membership_credit_local_amount,
    t.payment_transaction_local_amount = s.payment_transaction_local_amount,
    t.bounceback_endowment_local_amount = s.bounceback_endowment_local_amount,
    t.vip_endowment_local_amount = s.vip_endowment_local_amount
FROM _fact_order_line__token_order_amounts AS s
WHERE t.order_line_id = s.order_line_id;
--SELECT * FROM _fact_order_line__stg_order_line_amounts;

CREATE OR REPLACE TEMP TABLE _fact_order_line__stg_order_line_embroidery_amounts AS
SELECT ei.order_line_id,
       e.subtotal_excl_tariff_local_amount,
       e.payment_transaction_local_amount,
       e.tax_local_amount,
       e.tax_cash_local_amount,
       e.tax_credit_local_amount,
       e.product_discount_local_amount,
       e.cash_credit_local_amount,
       e.cash_membership_credit_local_amount,
       e.cash_refund_credit_local_amount,
       e.cash_giftco_credit_local_amount,
       e.cash_giftcard_credit_local_amount,
       e.non_cash_credit_local_amount,
       e.bounceback_endowment_local_amount,
       e.vip_endowment_local_amount
FROM _fact_order_line__stg_order_line_amounts AS e
    JOIN _fact_order_line__stg_order_line_amounts AS ei /* embroidered item */
        ON ei.order_id = e.order_id
        AND ei.sub_group_key = e.sub_group_key
        AND ei.product_type_id <> 22
WHERE e.product_type_id = 22; /* embroidery */

/* adding embroidery amounts to the embroidered item order_line_id */
UPDATE _fact_order_line__stg_order_line_amounts AS t
SET
t.subtotal_excl_tariff_local_amount = COALESCE(t.subtotal_excl_tariff_local_amount, 0) + COALESCE(s.subtotal_excl_tariff_local_amount, 0),
t.payment_transaction_local_amount = COALESCE(t.payment_transaction_local_amount, 0) + COALESCE(s.payment_transaction_local_amount, 0),
t.tax_local_amount = COALESCE(t.tax_local_amount, 0) + COALESCE(s.tax_local_amount, 0),
t.tax_cash_local_amount = COALESCE(t.tax_cash_local_amount, 0) + COALESCE(s.tax_cash_local_amount, 0),
t.tax_credit_local_amount = COALESCE(t.tax_credit_local_amount, 0) + COALESCE(s.tax_credit_local_amount, 0),
t.product_discount_local_amount = COALESCE(t.product_discount_local_amount, 0) + COALESCE(s.product_discount_local_amount, 0),
t.cash_credit_local_amount = COALESCE(t.cash_credit_local_amount, 0) + COALESCE(s.cash_credit_local_amount, 0),
t.cash_membership_credit_local_amount = COALESCE(t.cash_membership_credit_local_amount, 0) + COALESCE(s.cash_membership_credit_local_amount, 0),
t.cash_refund_credit_local_amount = COALESCE(t.cash_refund_credit_local_amount, 0) + COALESCE(s.cash_refund_credit_local_amount, 0),
t.cash_giftco_credit_local_amount = COALESCE(t.cash_giftco_credit_local_amount, 0) + COALESCE(s.cash_giftco_credit_local_amount, 0),
t.cash_giftcard_credit_local_amount = COALESCE(t.cash_giftcard_credit_local_amount, 0) + COALESCE(s.cash_giftcard_credit_local_amount, 0),
t.non_cash_credit_local_amount = COALESCE(t.non_cash_credit_local_amount, 0) + COALESCE(s.non_cash_credit_local_amount, 0),
t.bounceback_endowment_local_amount = COALESCE(t.bounceback_endowment_local_amount, 0) + COALESCE(s.bounceback_endowment_local_amount, 0),
t.vip_endowment_local_amount = COALESCE(t.vip_endowment_local_amount, 0) + COALESCE(s.vip_endowment_local_amount, 0)
FROM _fact_order_line__stg_order_line_embroidery_amounts as s
WHERE t.order_line_id = s.order_line_id;

/*
Product Cost Logic:
	1. Historical data from existing fact_order_line table (pre 201804)
	2. From 2018-04-01, ingest data from oracle feed
	3. Get cost from estimated cost (Same store and SKU's cost of recent orders) from oracle feed data
	4. Get cost from estimated cost (same region and SKU of recent orders) from oracle feed data
	5. Get cost from PO + SKU table for
        a. Shipped Online orders
        b. Retail orders
	6. For all other orders,
		a. Most recent PO cost for that SKU. from PO+SKU table
		b. 0 if not available in all the above cases.

** Due to QUALIFY issues in this cost portion of the code, we created a reference table that stores historical estimated cost values
so the numbers will freeze, stay consistent and can match edw_prod to it
 */

CREATE OR REPLACE TEMP TABLE _fact_order_line__historical_costs AS
    SELECT
        hplc.order_line_id,
        hplc.estimated_landed_cost_local_amount,
        hplc.po_cost_local_amount,
        hplc.misc_cogs_local_amount,
        hplc.lpn_po_cost_local_amount,
        hplc.cost_source_id,
        hplc.cost_source
    FROM _fact_order_line__order_line_base AS olb
        JOIN reference.historical_po_landed_cost AS hplc
            ON hplc.order_line_id = olb.order_line_id;

CREATE OR REPLACE TEMP TABLE _fact_order_line__sku AS
SELECT
    stg.order_line_id,
    stg.product_id,
    COALESCE(dip.sku,dp.sku) AS sku
FROM _fact_order_line__stg_order_line_base AS stg
LEFT JOIN stg.dim_item_price AS dip
    ON dip.product_id = stg.product_id
    AND dip.item_id = COALESCE(stg.order_line_item_id, stg.product_item_id)
LEFT JOIN stg.dim_product AS dp
    ON stg.product_id = dp.product_id;

CREATE OR REPLACE TEMP TABLE _fact_order_line__product_cost_base AS
SELECT
    base.order_line_id,
    base.meta_original_order_line_id,
    base.order_id,
    fo.meta_original_order_id,
    fo.store_id,
    sc.currency_code AS store_currency_code,
    sk.sku,
    IFF(base.item_quantity = 0, 1, base.item_quantity) AS units,
    COALESCE(fo.shipped_local_datetime, fo.order_local_datetime)::DATE AS transaction_date,
    fo.order_sales_channel_key
FROM _fact_order_line__stg_order_line_base AS base
    JOIN stg.fact_order AS fo
        ON fo.order_id = base.order_id
    LEFT JOIN _fact_order_line__sku sk
        ON sk.order_line_id = base.order_line_id
    JOIN reference.store_currency AS sc
        ON sc.store_id = fo.store_id
        AND COALESCE(fo.shipped_local_datetime, fo.order_local_datetime)::DATE
            BETWEEN sc.effective_start_date AND sc.effective_end_date;
-- SELECT * FROM _fact_order_line__product_cost_base;

-- Temp table to eliminate duplicates in #2
CREATE OR REPLACE TEMP TABLE _fact_order_line__landed_cost AS
SELECT
    lc.line_number,
    lc.currency_code,
    lc.transaction_cost,
    lc.mmt_pk,
    ROW_NUMBER() OVER (PARTITION BY lc.line_number ORDER BY lc.mmt_pk DESC) AS row_num
FROM _fact_order_line__product_cost_base AS pcb
    JOIN lake.oracle_ebs.landed_cost AS lc
        ON lc.line_number = pcb.meta_original_order_line_id
WHERE LOWER(lc.transaction_type_name) IN ('ts customer shipment', 'ts retail shipment/sale')
AND pcb.order_line_id NOT IN (SELECT order_line_id FROM _fact_order_line__historical_costs)
QUALIFY row_num = 1;
-- SELECT * FROM _fact_order_line__landed_cost;

-- #2 and #3 from logic
CREATE OR REPLACE TEMP TABLE _fact_order_line__stg_product_cost AS
SELECT
    pcb.order_id,
    pcb.order_line_id,
    COALESCE(IFF(pcb.store_currency_code = lc.currency_code, lc.transaction_cost, lc.transaction_cost * COALESCE(cer.exchange_rate, 1)),
        (pcb.units * cs.unit_cost)) AS estimated_landed_cost,
    CASE
        WHEN lc.line_number IS NOT NULL THEN 'Oracle EBS Feed'
        WHEN cs.store_id IS NOT NULL THEN 'Estimated EBS Same Store'
    END AS cost_source,
    COALESCE(lc.mmt_pk, cs.estimated_cogs_id, -1) AS cost_source_id
FROM _fact_order_line__product_cost_base AS pcb
    LEFT JOIN _fact_order_line__landed_cost AS lc
        ON lc.line_number = pcb.meta_original_order_line_id
    LEFT JOIN reference.order_lines_with_jfca_store ocs
        ON ocs.order_line_id = pcb.order_line_id
    LEFT JOIN reference.landed_cost_by_sku AS cs
        ON cs.store_id = COALESCE(ocs.store_id, pcb.store_id)
        AND LOWER(cs.sku) = LOWER(pcb.sku)
        AND pcb.transaction_date BETWEEN cs.start_date AND cs.end_date
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer
        ON cer.rate_date_pst = pcb.transaction_date
        AND UPPER(cer.src_currency) = UPPER(lc.currency_code)
        AND UPPER(cer.dest_currency) = UPPER(pcb.store_currency_code)
WHERE pcb.transaction_date >= '2018-04-01'
    OR lc.line_number IS NOT NULL;
-- SELECT * FROM _fact_order_line__stg_product_cost;

-- Temp table #4 from logic
CREATE OR REPLACE TEMP TABLE _fact_order_line__region_sku_cost AS
SELECT
    pcb.order_line_id,
    lcs.unit_cost,
    lcs.estimated_cogs_id,
    lcs.currency_code,
    ds.store_currency AS store_currency_code,
    pcb.transaction_date,
    ROW_NUMBER() OVER (PARTITION BY pcb.order_line_id ORDER BY lcs.start_date DESC, lcs.unit_cost DESC) AS row_num
FROM _fact_order_line__product_cost_base AS pcb
    JOIN stg.dim_store AS ds
        ON ds.store_id = pcb.store_id
    JOIN _fact_order_line__stg_product_cost AS spc
        ON spc.order_line_id = pcb.order_line_id
        AND spc.estimated_landed_cost IS NULL
    JOIN reference.landed_cost_by_sku AS lcs
        ON LOWER(lcs.sku) = LOWER(pcb.sku)
        AND lcs.store_region = ds.store_region
        AND pcb.transaction_date BETWEEN lcs.start_date AND lcs.end_date
WHERE pcb.order_line_id NOT IN (SELECT order_line_id FROM _fact_order_line__historical_costs)
    AND lcs.unit_cost > 0
QUALIFY row_num = 1;
-- SELECT * FROM _fact_order_line__region_sku_cost;

CREATE OR REPLACE TEMP TABLE _fact_order_line__region_sku_cost_local AS
SELECT
    rsc.order_line_id,
    CASE
        WHEN rsc.store_currency_code = rsc.currency_code
            THEN rsc.unit_cost
        ELSE rsc.unit_cost * COALESCE(cer.exchange_rate, 1)
    END AS unit_cost,
    estimated_cogs_id
FROM _fact_order_line__region_sku_cost AS rsc
LEFT JOIN reference.currency_exchange_rate_by_date AS cer
    ON cer.rate_date_pst = rsc.transaction_date
    AND upper(cer.src_currency) = upper(rsc.currency_code)
    AND upper(cer.dest_currency) = upper(rsc.store_currency_code);
-- SELECT * FROM _fact_order_line__region_sku_cost_local;

-- updating #4 data
UPDATE _fact_order_line__stg_product_cost AS spc
SET spc.estimated_landed_cost = pcb.units * rscl.unit_cost,
    spc.cost_source = 'Estimated EBS Same Region',
    spc.cost_source_id = rscl.estimated_cogs_id
FROM _fact_order_line__product_cost_base AS pcb
    JOIN _fact_order_line__region_sku_cost_local AS rscl
        ON rscl.order_line_id = pcb.order_line_id
WHERE spc.estimated_landed_cost IS NULL
    AND spc.order_line_id = pcb.order_line_id;
-- SELECT * FROM _fact_order_line__order_agg;

-- Temp table for #5 in logic for lpn_po_cost_local_amount
CREATE OR REPLACE TEMP TABLE _fact_order_line__lpn_po_base_stg AS
SELECT DISTINCT
    i.invoice_id,
    pcb.order_line_id,
    l.receipt_id,
    it.item_number,
    l.lpn_code,
    l.lpn_id
FROM _fact_order_line__product_cost_base AS pcb
    JOIN lake.ultra_warehouse.invoice AS i
        ON i.foreign_order_id = pcb.meta_original_order_id
    JOIN lake.ultra_warehouse.invoice_box AS ib
        ON ib.invoice_id = i.invoice_id
    JOIN lake.ultra_warehouse.invoice_box_item AS ibi
        ON ibi.invoice_box_id = ib.invoice_box_id
    JOIN lake.ultra_warehouse.lpn AS l
        ON l.lpn_id = ibi.lpn_id
    JOIN lake.ultra_warehouse.item AS it
        ON it.item_id = l.item_id
WHERE LOWER(it.item_number) = LOWER(pcb.sku)
AND pcb.order_line_id NOT IN (SELECT order_line_id FROM _fact_order_line__historical_costs);
-- SELECT * FROM _fact_order_line__lpn_po_base_stg;

-- #5.b logic for lpn_po_cost_local_amount for Online Orders
-- In BlueCherry, we have multiple PO line numbers associated with a given PO, allowing for different freight methods per line.
-- reference.gfc_po_sku_line_lpn_mapping is a mapping table that provides the PO number, PO line number, and SKU for a given LPN.
-- Below temp table is to get po,sku and po_line_number using lpn for a given order_line_id
CREATE OR REPLACE TEMP TABLE _fact_order_line__lpn_po_base AS
SELECT DISTINCT
    bs.order_line_id,
    trim(ld.po_number) as po_number,
    ld.sku,
    ld.po_line_number,
    IFF(EQUAL_NULL(bs.lpn_code, ol.lpn_code), 1, 2) AS rnk
FROM _fact_order_line__lpn_po_base_stg AS bs
JOIN lake_consolidated.ultra_merchant.order_line AS ol
    ON ol.order_line_id = bs.order_line_id
JOIN reference.gfc_po_sku_line_lpn_mapping AS ld
    ON ld.lpn_id = bs.lpn_id
WHERE NOT ld.is_deleted;

-- Temp table to retrieve PO number and SKU using receipt for a given order_line_id
-- if the above temp table does not provide PO#, SKU, or PO line#.
INSERT INTO _fact_order_line__lpn_po_base
SELECT
    bs.order_line_id,
    UPPER(nvl(lpb.po_number, r.po_number)) AS po_number,
    nvl(lpb.sku, bs.item_number) AS sku,
    NULL AS po_line_number,
    IFF(EQUAL_NULL(bs.lpn_code, ol.lpn_code), 1, 2) AS rnk
FROM _fact_order_line__lpn_po_base_stg AS bs
    JOIN lake.ultra_warehouse.receipt AS r
        ON r.receipt_id = bs.receipt_id
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_line_id = bs.order_line_id
    LEFT JOIN _fact_order_line__lpn_po_base lpb
        ON lpb.order_line_id = bs.order_line_id
WHERE lpb.order_line_id IS NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY bs.order_line_id ORDER BY r.datetime_received DESC) = 1;
-- SELECT * FROM _fact_order_line__lpn_po_base;

--  #5.b logic for lpn_po_cost_local_amount for Retail Orders

-- In BlueCherry, we have multiple PO line numbers associated with a given PO, allowing for different freight methods per line.
-- reference.gfc_po_sku_line_lpn_mapping is a mapping table that provides the PO number, PO line number, and SKU for a given LPN.
-- Below temp table is to get po,sku and po_line_number using lpn for a given retail orders
INSERT INTO _fact_order_line__lpn_po_base
WITH _cte_base_orders AS (
    SELECT DISTINCT order_id
    FROM _fact_order_line__product_cost_base base
    LEFT JOIN _fact_order_line__lpn_po_base AS lpb
        ON lpb.order_line_id = base.order_line_id
    WHERE base.order_line_id NOT IN (SELECT order_line_id FROM _fact_order_line__historical_costs)
    AND lpb.order_line_id IS NULL
    )
SELECT
    pcb.order_line_id,
    trim(ld.po_number) as po_number,
    ld.sku,
    ld.po_line_number,
    1 AS rnk
FROM _cte_base_orders AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_id = o.order_id
    JOIN lake.ultra_warehouse.lpn AS l
        ON l.lpn_code = ol.lpn_code
    JOIN reference.gfc_po_sku_line_lpn_mapping AS ld
        ON ld.lpn_id = l.lpn_id
    JOIN lake_consolidated.ultra_merchant.store AS s
        ON s.store_id = o.store_id
    JOIN _fact_order_line__product_cost_base AS pcb
        ON pcb.order_line_id = ol.order_line_id
        AND LOWER(pcb.sku) = LOWER(ld.sku)
WHERE s.store_group_id = 16 /* Retail stores */
    AND NOT ld.is_deleted;

-- Temp table to get po,sku using receipt for a given retail order_line_id
-- if the above temp table does not provide PO#, SKU, or PO line#
INSERT INTO _fact_order_line__lpn_po_base
WITH _cte_base_orders AS (
    SELECT DISTINCT order_id
    FROM _fact_order_line__product_cost_base base
    LEFT JOIN _fact_order_line__lpn_po_base AS lpb
        ON lpb.order_line_id = base.order_line_id
    WHERE base.order_line_id NOT IN (SELECT order_line_id FROM _fact_order_line__historical_costs)
    AND lpb.order_line_id IS NULL
    )
SELECT
    pcb.order_line_id,
    UPPER(r.po_number) AS po_number,
    i.item_number AS sku,
    NULL AS po_line_number,
    1 AS rnk
FROM _cte_base_orders AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_id = o.order_id
    LEFT JOIN _fact_order_line__lpn_po_base AS lpb
        ON lpb.order_line_id = ol.order_line_id
    JOIN lake.ultra_warehouse.lpn AS l
        ON l.lpn_code = ol.lpn_code
    JOIN lake.ultra_warehouse.item AS i
        ON i.item_id = l.item_id
    JOIN lake.ultra_warehouse.receipt AS r
        ON r.receipt_id = l.receipt_id
    JOIN lake_consolidated.ultra_merchant.store AS s
        ON s.store_id = o.store_id
    JOIN _fact_order_line__product_cost_base AS pcb
        ON pcb.order_line_id = ol.order_line_id
        AND LOWER(pcb.sku) = LOWER(i.item_number)
WHERE s.store_group_id = 16 /* Retail stores */
QUALIFY ROW_NUMBER() OVER(PARTITION BY pcb.order_line_id ORDER BY r.datetime_received DESC) = 1;

CREATE OR REPLACE TEMP TABLE  _fact_order_line__lpn_po AS
SELECT
    order_line_id,
    po_number,
    sku,
    po_line_number,
    ROW_NUMBER() OVER (PARTITION BY order_line_id ORDER BY rnk) AS row_num
FROM _fact_order_line__lpn_po_base
QUALIFY row_num = 1;

-- Temp table for lpn_po_cost_local_amount, cost is derived based on po, sku and po_line_number
CREATE OR REPLACE TEMP TABLE _fact_order_line__lpn_po_cost AS
SELECT
    lp.order_line_id,
    psc.po_sku_cost_id,
    CASE
        WHEN psc.currency = ds.currency_code THEN pcb.units * psc.landed_cost
        ELSE pcb.units * COALESCE(cer.exchange_rate, 1) * psc.landed_cost
    END AS lpn_po_cost
FROM reference.po_sku_cost AS psc
    JOIN _fact_order_line__lpn_po AS lp
        ON UPPER(lp.po_number) = UPPER(psc.po_number)
        AND LOWER(lp.sku) = LOWER(psc.sku)
        AND EQUAL_NULL(lp.po_line_number, psc.po_line_number)
    JOIN _fact_order_line__product_cost_base AS pcb
        ON pcb.order_line_id = lp.order_line_id
    JOIN reference.store_currency AS ds
        ON ds.store_id = pcb.store_id
        AND pcb.transaction_date BETWEEN ds.effective_start_date AND ds.effective_end_date
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer
        ON cer.rate_date_pst = pcb.transaction_date
        AND UPPER(cer.src_currency) = UPPER(psc.currency)
        AND UPPER(cer.dest_currency) = UPPER(ds.currency_code)
WHERE lp.po_line_number IS NOT NULL;

-- Temp table for lpn_po_cost_local_amount, cost is derived based on po, sku
INSERT INTO _fact_order_line__lpn_po_cost
SELECT
    lp.order_line_id,
    psc.po_sku_cost_id,
    CASE
        WHEN psc.currency = ds.currency_code THEN pcb.units * psc.landed_cost
        ELSE pcb.units * COALESCE(cer.exchange_rate, 1) * psc.landed_cost
    END AS lpn_po_cost
FROM reference.po_sku_cost AS psc
    JOIN _fact_order_line__lpn_po AS lp
        ON UPPER(lp.po_number) = UPPER(psc.po_number)
        AND LOWER(lp.sku) = LOWER(psc.sku)
    JOIN _fact_order_line__product_cost_base AS pcb
        ON pcb.order_line_id = lp.order_line_id
    JOIN reference.store_currency AS ds
        ON ds.store_id = pcb.store_id
        AND pcb.transaction_date BETWEEN ds.effective_start_date AND ds.effective_end_date
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer
        ON cer.rate_date_pst = pcb.transaction_date
        AND UPPER(cer.src_currency) = UPPER(psc.currency)
        AND UPPER(cer.dest_currency) = UPPER(ds.currency_code)
LEFT JOIN _fact_order_line__lpn_po_cost lpc
        ON lpc.order_line_id = lp.order_line_id
WHERE
    (lp.po_line_number IS NULL OR psc.po_line_number IS NULL)
    AND lpc.order_line_id IS NULL
    AND psc.landed_cost >0
QUALIFY ROW_NUMBER() OVER(PARTITION BY lp.order_line_id ORDER BY  psc.start_date DESC, psc.quantity DESC, psc.landed_cost DESC) = 1;
-- SELECT * FROM _fact_order_line__lpn_po_cost;

-- updating records with #5 logic
 UPDATE _fact_order_line__stg_product_cost AS spc
 SET spc.estimated_landed_cost = lpc.lpn_po_cost,
    spc.cost_source = 'reference.po_sku_cost',
    spc.cost_source_id = lpc.po_sku_cost_id
 FROM _fact_order_line__lpn_po_cost AS lpc
 WHERE spc.order_line_id = lpc.order_line_id
    AND spc.estimated_landed_cost IS NULL;
-- SELECT * FROM _fact_order_line__stg_product_cost;

-- Temp table for #6 in logic
CREATE OR REPLACE TEMP table _fact_order_line__po_sku_dates AS
SELECT
    pcb.order_line_id,
    pcb.sku,
    MAX(IFF(psc.start_date <= pcb.transaction_date, psc.start_date, NULL)) AS start_date,
    MIN(IFF(psc.start_date > pcb.transaction_date, psc.start_date, NULL)) AS after_transaction_date
FROM _fact_order_line__product_cost_base AS pcb
    JOIN reference.po_sku_cost AS psc
        ON LOWER(psc.sku) = LOWER(pcb.sku)
WHERE pcb.order_line_id NOT IN (SELECT order_line_id FROM _fact_order_line__historical_costs)
GROUP BY
    pcb.order_line_id,
    pcb.sku;
-- SELECT * FROM _fact_order_line__po_sku_dates;

CREATE OR REPLACE TEMP table _fact_order_line__recent_po_sku AS
SELECT
    psd.order_line_id,
    psc.landed_cost,
    psc.currency,
    psc.po_sku_cost_id,
    ROW_NUMBER() OVER (PARTITION BY psd.order_line_id ORDER BY psc.start_date DESC, psc.quantity DESC, psc.landed_cost DESC) AS row_num
FROM _fact_order_line__po_sku_dates AS psd
    JOIN reference.po_sku_cost AS psc
        ON LOWER(psc.sku) = LOWER(psd.sku)
WHERE COALESCE(psd.start_date, psd.after_transaction_date) = psc.start_date
  AND psc.landed_cost >0
QUALIFY row_num = 1;
-- SELECT * FROM _fact_order_line__recent_po_sku;

-- Temp table for po_cost_local_amount
CREATE OR REPLACE TEMP TABLE _fact_order_line__po_cost AS
SELECT
    pcb.order_line_id,
    CASE
        WHEN rps.currency = ds.currency_code THEN pcb.units * rps.landed_cost
        ELSE pcb.units * COALESCE(cer.exchange_rate, 1) * rps.landed_cost
    END AS po_cost,
    rps.po_sku_cost_id
FROM _fact_order_line__recent_po_sku AS rps
    JOIN _fact_order_line__product_cost_base AS pcb
        ON pcb.order_line_id = rps.order_line_id
    JOIN reference.store_currency AS ds
        ON ds.store_id = pcb.store_id
        AND pcb.transaction_date BETWEEN ds.effective_start_date AND ds.effective_end_date
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer
        ON cer.rate_date_pst = pcb.transaction_date
        AND UPPER(cer.src_currency) = UPPER(rps.currency)
        AND UPPER(cer.dest_currency) = UPPER(ds.currency_code);
-- SELECT * FROM _fact_order_line__po_cost;

-- updating records with #6 logic
UPDATE _fact_order_line__stg_product_cost sc
SET sc.estimated_landed_cost = ps.po_cost,
    sc.cost_source = 'Recent PO SKU from po_sku_cost',
    sc.cost_source_id = ps.po_sku_cost_id
 FROM _fact_order_line__po_cost AS ps
WHERE sc.order_line_id = ps.order_line_id
AND sc.estimated_landed_cost IS NULL;
-- SELECT * FROM _fact_order_line__stg_product_cost;

/*
Historical data is loaded from reference.fact_order_line_product_cost_archive.
This data is copied from archive_edw01_edw_prod.dbo.fact_order_line_product_cost_20201019.
*/

-- Inserting records for #1 from logic

INSERT INTO _fact_order_line__stg_product_cost
SELECT
    pcb.order_id,
    pcb.order_line_id,
    fol.estimated_total_cost_local_amount AS estimated_landed_cost,
    'Historical data from EDW01' AS cost_source,
    fol.order_line_product_cost_key AS cost_source_id
FROM reference.fact_order_line_product_cost_archive AS fol
    JOIN _fact_order_line__product_cost_base AS pcb
        ON pcb.order_line_id = fol.order_line_id
    LEFT JOIN _fact_order_line__stg_product_cost AS spc
        ON spc.order_line_id = pcb.order_line_id
WHERE pcb.transaction_date < '2018-04-01'
    AND spc.order_line_id IS NULL;
-- SELECT * FROM _fact_order_line__stg_product_cost;

CREATE OR REPLACE TEMP TABLE _fact_order_line__markdown_adjustment AS
SELECT
    pcb.order_line_id,
    (ca.product_cost_markdown_adjustment_percent / 100.0) * COALESCE(spc.estimated_landed_cost, 0) AS misc_cogs_local_amount
FROM _fact_order_line__product_cost_base AS pcb
    JOIN reference.cogs_assumptions AS ca
        ON ca.store_id = pcb.store_id
        AND pcb.transaction_date::DATE BETWEEN ca.start_date AND ca.end_date
    JOIN stg.dim_order_sales_channel AS dosc
        ON dosc.order_sales_channel_key = pcb.order_sales_channel_key
    JOIN _fact_order_line__stg_product_cost AS spc
        ON spc.order_line_id = pcb.order_line_id
WHERE dosc.order_classification_l2 IN ('Product Order', 'Reship', 'Exchange')
AND pcb.order_line_id NOT IN (SELECT order_line_id FROM _fact_order_line__historical_costs);
-- SELECT * FROM _fact_order_line__markdown_adjustment;

CREATE OR REPLACE TEMP TABLE _fact_order_line__lpn_code AS
SELECT
    pcb.order_line_id,
    ol.lpn_code AS order_line_lpn_code,
    l.lpn_code
FROM _fact_order_line__product_cost_base AS pcb
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_line_id = pcb.order_line_id
    LEFT JOIN lake.ultra_warehouse.lpn AS l
        ON l.lpn_code = ol.lpn_code;

UPDATE _fact_order_line__lpn_code AS t
SET t.lpn_code = lc.lpn_code,
    t.order_line_lpn_code = lc.order_line_lpn_code
FROM _fact_order_line__dropship_child AS dc
    JOIN _fact_order_line__lpn_code AS lc
        ON dc.order_line_id = lc.order_line_id
WHERE dc.master_order_line_id = t.order_line_id;
-- SELECT * FROM _fact_order_line__lpn_code;
-- SELECT COUNT(1) FROM _fact_order_line__lpn_code WHERE NOT EQUAL_NULL(order_line_lpn_code, lpn_code);

-- Miracle Mile landed costs
CREATE OR REPLACE TEMP TABLE _fact_order_line__miracle_mile_landed_cost AS
SELECT DISTINCT base.order_line_id,
        'Centric Miracle Miles' AS cost_source,
        base.units * COALESCE(cer.exchange_rate, 1) * mc.landed_cost AS estimated_landed_cost_local_amount
FROM _fact_order_line__product_cost_base base
         JOIN lake.centric.ed_sku sku
              ON sku.tfg_code = base.sku
         JOIN lake.centric.ed_style eds
              ON sku.the_parent_id = eds.id
         JOIN lake.centric.ed_colorway edc
              ON eds.id = edc.the_parent_id AND edc.id = sku.realized_color
         JOIN reference.miracle_miles_landed_cost mc
              ON mc.style = eds.code AND mc.colorway_cnl = edc.the_cnl
                  AND base.transaction_date BETWEEN mc.effective_start_datetime AND mc.effective_end_datetime
         JOIN reference.store_currency AS ds
              ON ds.store_id = base.store_id
                  AND base.transaction_date BETWEEN ds.effective_start_date AND ds.effective_end_date
         LEFT JOIN reference.currency_exchange_rate_by_date AS cer
                   ON cer.rate_date_pst = base.transaction_date
                       AND UPPER(cer.src_currency) = 'USD'
                       AND UPPER(cer.dest_currency) = UPPER(ds.currency_code)
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.order_line_id, sku.tfg_code ORDER BY eds.code ASC) = 1;


-- Final Product Cost table
CREATE OR REPLACE TEMP TABLE _fact_order_line__stg_order_line_product_cost_data AS
SELECT
    base.order_line_id,
    dpt.product_type_key,
    dpt.product_type_name,
    COALESCE(hc.cost_source_id, spc.cost_source_id) AS cost_source_id,
    COALESCE(mm.cost_source, hc.cost_source, spc.cost_source) AS cost_source,
    lpn.lpn_code,
    COALESCE(mm.estimated_landed_cost_local_amount, hc.estimated_landed_cost_local_amount, spc.estimated_landed_cost) AS estimated_landed_cost,
    COALESCE(hc.lpn_po_cost_local_amount, lpc.lpn_po_cost) AS lpn_po_cost,
    COALESCE(hc.po_cost_local_amount, ps.po_cost) AS po_cost,
    COALESCE(hc.misc_cogs_local_amount, ma.misc_cogs_local_amount) AS misc_cogs_local_amount
FROM _fact_order_line__stg_order_line_base AS base
    LEFT JOIN stg.dim_product_type AS dpt
        ON base.Product_type_id = dpt.Product_type_id
    LEFT JOIN _fact_order_line__stg_product_cost AS spc
        ON spc.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__lpn_po_cost AS lpc
        ON lpc.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__po_cost AS ps
        ON ps.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__markdown_adjustment AS ma
        ON ma.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__lpn_code AS lpn
        ON lpn.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__historical_costs AS hc
        ON hc.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__miracle_mile_landed_cost mm
        ON mm.order_line_id = base.order_line_id;
-- SELECT * FROM _fact_order_line__stg_order_line_product_cost_data;

/* assigning cost values to the master order for dropship since sources like oracle are tied to child order */
UPDATE _fact_order_line__stg_order_line_product_cost_data AS t
SET t.estimated_landed_cost = s.estimated_landed_cost,
    t.cost_source = s.cost_source,
    t.cost_source_id = s.cost_source_id,
    t.lpn_po_cost = s.lpn_po_cost,
    t.misc_cogs_local_amount = s.misc_cogs_local_amount
FROM (
        SELECT
            dc.master_order_line_id as order_line_id,
            pc.estimated_landed_cost,
            pc.cost_source,
            pc.cost_source_id,
            pc.lpn_po_cost,
            pc.misc_cogs_local_amount
        FROM _fact_order_line__dropship_child AS dc
            JOIN _fact_order_line__stg_order_line_product_cost_data AS pc
                ON pc.order_line_id = dc.order_line_id

     ) AS s
WHERE s.order_line_id = t.order_line_id
AND (
    NOT EQUAL_NULL(t.cost_source_id, s.cost_source_id)
    OR NOT EQUAL_NULL(t.estimated_landed_cost, s.estimated_landed_cost)
    OR NOT EQUAL_NULL(t.lpn_po_cost, s.lpn_po_cost)
    OR NOT EQUAL_NULL(t.misc_cogs_local_amount, s.misc_cogs_local_amount)
    )
;

CREATE OR REPLACE TEMP TABLE _fact_order_line__keys AS
SELECT
    stg.order_line_id,
    stg.order_id,
    stg.bundle_product_id,
    IFF(stg.bundle_product_id IS NOT NULL, stg.master_product_id, NULL) AS component_product_id,
    dbch.bundle_component_product_id, /* this is the component product that ties to the bundle */
    stg.master_product_label AS component_product_label,
    stg.master_product_alias AS component_product_alias,
    stg.master_product_group_code AS component_product_group_code,
    fo.store_id,
    fo.order_local_datetime,
    COALESCE(dpph.product_price_history_key, -1) AS product_price_history_key,
    COALESCE(bdpph.product_price_history_key, -1) AS bundle_product_price_history_key,
    COALESCE(dbch.bundle_component_history_key, -1) AS bundle_component_history_key,
    COALESCE(ols.order_line_status_key, -1) AS order_line_status_key,
    COALESCE(dops.order_product_source_key, -1) AS order_product_source_key,
    COALESCE(dosc.order_classification_l2, 'Unknown') AS order_classification,
    COALESCE(dip.item_price_key, -1) AS item_price_key,
    ROW_NUMBER() OVER (PARTITION BY stg.order_line_id ORDER BY dpph.product_price_history_key DESC, dbch.bundle_component_history_key DESC) AS row_num
FROM _fact_order_line__stg_order_line_base AS stg
    JOIN stg.fact_order AS fo
        ON fo.order_id = stg.order_id
    LEFT JOIN stg.dim_product_price_history AS dpph
		ON dpph.product_id = stg.product_id
		AND fo.order_local_datetime BETWEEN dpph.effective_start_datetime AND dpph.effective_end_datetime
        AND NOT dpph.is_deleted
    LEFT JOIN stg.dim_product_price_history AS bdpph
		ON bdpph.product_id = stg.bundle_product_id
		AND fo.order_local_datetime BETWEEN bdpph.effective_start_datetime AND bdpph.effective_end_datetime
        AND NOT dpph.is_deleted
    LEFT JOIN stg.dim_bundle_component_history AS dbch
		ON dbch.bundle_product_id = stg.bundle_product_id
		AND dbch.bundle_component_product_id = stg.master_product_id
		AND fo.order_local_datetime BETWEEN dbch.effective_start_datetime AND dbch.effective_end_datetime
        AND NOT dbch.is_deleted
    LEFT JOIN stg.dim_order_line_status AS ols
        ON ols.order_line_status = stg.order_line_status
        AND COALESCE(fo.order_local_datetime, CURRENT_TIMESTAMP) BETWEEN ols.effective_start_datetime AND ols.effective_end_datetime
    LEFT JOIN stg.dim_order_product_source AS dops
        ON LOWER(dops.order_product_source_name) = LOWER(stg.order_product_source_name)
    LEFT JOIN stg.dim_order_sales_channel AS dosc
        ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    LEFT JOIN stg.dim_item_price AS dip
        ON dip.product_id = stg.product_id
        AND dip.item_id = COALESCE(stg.order_line_item_id, stg.product_item_id)
QUALIFY row_num = 1;
-- SELECT * FROM _fact_order_line__keys;

-- Try to substitute unmatched component products with suggested products (SXF method)
UPDATE _fact_order_line__keys AS t
SET
    t.bundle_component_product_id = s.bundle_component_product_id,
    t.bundle_component_history_key = s.bundle_component_history_key
FROM (
    SELECT
        s.order_line_id,
        dbch.bundle_component_product_id,
        dbch.bundle_component_history_key
    FROM _fact_order_line__keys AS s
    JOIN lake_consolidated.ultra_merchant.suggested_product AS sp
        ON sp.product_id = s.component_product_id
        AND sp.suggested_product_type_id = 3
    JOIN stg.dim_bundle_component_history AS dbch
        ON dbch.bundle_product_id = s.bundle_product_id
        AND dbch.bundle_component_product_id = sp.related_product_id
		AND s.order_local_datetime BETWEEN dbch.effective_start_datetime AND dbch.effective_end_datetime
        AND NOT dbch.is_deleted
    QUALIFY ROW_NUMBER() OVER(PARTITION BY s.order_line_id ORDER BY dbch.bundle_component_product_id DESC, dbch.meta_event_datetime DESC) = 1
    ) AS s
WHERE t.order_line_id = s.order_line_id
    AND t.bundle_product_id IS NOT NULL
    AND t.bundle_component_history_key = -1;

-- Try to substitute unmatched component products with related products (non-SXF method)
UPDATE _fact_order_line__keys AS t
SET
    t.bundle_component_product_id = s.bundle_component_product_id,
    t.bundle_component_history_key = s.bundle_component_history_key
FROM (
    SELECT
        s.order_line_id,
        dbch.bundle_component_product_id,
        dbch.bundle_component_history_key,
        ROW_NUMBER() OVER(PARTITION BY s.order_line_id ORDER BY dbch.bundle_component_product_id DESC, dbch.meta_event_datetime DESC) AS rn
    FROM _fact_order_line__keys AS s
    JOIN data_model.dim_related_product AS rgc
        ON LEFT(rgc.group_code, 9) = LEFT(s.component_product_group_code, 9)
        OR LEFT(rgc.alias, 9) = LEFT(s.component_product_alias, 9)
        OR LEFT(rgc.label, 9) = LEFT(s.component_product_label, 9)
    JOIN stg.dim_bundle_component_history AS dbch
        ON dbch.bundle_product_id = s.bundle_product_id
        AND dbch.bundle_component_product_id = rgc.master_product_id
		AND s.order_local_datetime BETWEEN dbch.effective_start_datetime AND dbch.effective_end_datetime
        AND NOT dbch.is_deleted
    WHERE NOT EXISTS (
        SELECT 1
        FROM _fact_order_line__keys AS k
        WHERE k.order_id = s.order_id
            AND k.bundle_product_id = dbch.bundle_product_id
            AND k.bundle_component_product_id = dbch.bundle_component_product_id
        )
    QUALIFY rn = 1) AS s
WHERE t.order_line_id = s.order_line_id
    AND t.bundle_product_id IS NOT NULL
    AND t.bundle_component_history_key = -1;
-- SELECT * FROM _fact_order_line__keys WHERE bundle_product_id IS NOT NULL AND bundle_component_history_key = -1;
-- SELECT COUNT(1) FROM _fact_order_line__keys WHERE bundle_product_id IS NOT NULL AND bundle_component_history_key = -1;

-- Check if there are any bundles remaining with only a single unmatched component product
CREATE OR REPLACE TEMP TABLE _fact_order_line__unknown_components AS
SELECT
    k.order_id,
    k.order_line_id,
    k.bundle_product_id,
    k.component_product_id,
    k.bundle_component_product_id,
    k.bundle_component_history_key,
    k.order_local_datetime
FROM _fact_order_line__keys AS k
    JOIN (
        SELECT
            order_id,
            bundle_product_id,
            COUNT(1) AS unknown_component_product_count
        FROM _fact_order_line__keys
        WHERE bundle_product_id IS NOT NULL
            AND bundle_component_history_key = -1
        GROUP BY
            order_id,
            bundle_product_id
        ) AS s
WHERE s.order_id = k.order_id
    AND s.bundle_product_id = k.bundle_product_id
    AND s.unknown_component_product_count = 1;
-- SELECT * FROM _fact_order_line__unknown_components;
-- SELECT COUNT(1) FROM _fact_order_line__unknown_components;

-- For single remaining unmatched component products, select the unused component product for the same bundle
-- in the dim_bundle_component_history table
CREATE OR REPLACE TEMP TABLE _fact_order_line__unmatched_components AS
SELECT
    unk.order_id,
    unk.order_line_id,
    unk.bundle_product_id,
    unk.component_product_id,
    dbch.bundle_component_product_id,
    dbch.bundle_component_history_key
FROM _fact_order_line__unknown_components AS unk
    JOIN stg.dim_bundle_component_history AS dbch
        ON dbch.bundle_product_id = unk.bundle_product_id
        AND unk.order_local_datetime BETWEEN dbch.effective_start_datetime AND dbch.effective_end_datetime
        AND NOT dbch.is_deleted
WHERE unk.bundle_component_history_key = -1
    AND NOT EXISTS (
        SELECT 1
        FROM _fact_order_line__keys AS k
        WHERE k.order_id = unk.order_id
            AND k.bundle_product_id = dbch.bundle_product_id
            AND k.bundle_component_product_id = dbch.bundle_component_product_id
        );
-- SELECT * FROM _fact_order_line__unmatched_components;
-- SELECT COUNT(1) FROM _fact_order_line__unmatched_components;

-- Substitute single unmatched component product with most recent unmatched remaining component product
-- for the same bundle in the dim_bundle_component_history table
UPDATE _fact_order_line__keys AS t
SET
    t.bundle_component_product_id = s.bundle_component_product_id,
    t.bundle_component_history_key = s.bundle_component_history_key
FROM (
    SELECT
        order_line_id,
        bundle_component_product_id,
        bundle_component_history_key,
        ROW_NUMBER() OVER (PARTITION BY order_line_id ORDER BY bundle_component_product_id DESC) AS row_num
    FROM _fact_order_line__unmatched_components
    QUALIFY row_num = 1
    ) AS s
WHERE t.order_line_id = s.order_line_id
    AND t.bundle_product_id IS NOT NULL
    AND t.bundle_component_history_key = -1;
-- SELECT * FROM _fact_order_line__keys WHERE bundle_product_id IS NOT NULL AND bundle_component_history_key = -1;
-- SELECT COUNT(1) FROM _fact_order_line__keys WHERE bundle_product_id IS NOT NULL AND bundle_component_history_key = -1;

CREATE OR REPLACE TEMP TABLE _fact_order_line__bounceback_endowment AS
SELECT
    base.order_line_id,
    be.bounceback_endowment_id
FROM _fact_order_line__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
        ON be.order_line_id = base.order_line_id
WHERE be.statuscode <> 5632
      AND be.object_id IS NOT NULL;

----------------------------
-- prepare staging table  --
----------------------------

CREATE OR REPLACE TEMP TABLE _fact_order_line__stg AS
SELECT
    base.order_line_id,
    base.meta_original_order_line_id,
    COALESCE(fo.currency_key, -1) AS currency_key,
    COALESCE(fo.customer_id, -1) AS customer_id,
    COALESCE(fo.store_id, -1) AS store_id,
    COALESCE(base.product_id, -1) AS product_id,
    COALESCE(base.master_product_id, -1) AS master_product_id,
    COALESCE(base.bundle_product_id, -1) AS bundle_product_id,
    COALESCE(k.bundle_component_product_id, -1) AS bundle_component_product_id,
    COALESCE(base.master_product_group_code, 'Unknown') AS group_code,
    COALESCE(k.bundle_component_history_key, -1) AS bundle_component_history_key,
    COALESCE(pc.product_type_key, -1) AS product_type_key,
    COALESCE(base.bundle_order_line_id, -1) AS bundle_order_line_id,
    COALESCE(fo.order_membership_classification_key, -1) AS order_membership_classification_key,
    COALESCE(fo.order_sales_channel_key, -1) AS order_sales_channel_key,
    COALESCE(k.order_line_status_key, -1) AS order_line_status_key,
    COALESCE(base.order_id, -1) AS order_id,
    COALESCE(fo.shipping_address_id, -1) AS shipping_address_id,
    COALESCE(fo.billing_address_id, -1) AS billing_address_id,
    COALESCE(fo.order_status_key, -1) AS order_status_key,
    COALESCE(k.order_product_source_key, -1) AS order_product_source_key,
    COALESCE(base.warehouse_id, -1) AS warehouse_id,
    COALESCE(pc.cost_source_id, -1) AS cost_source_id,
    COALESCE(pc.cost_source,
        CASE
            WHEN pc.product_type_name IN ('Directed Insert', 'Gift Certificate',
                'Membership Fee Placeholder', 'Free Third Party Subscription')
                OR k.order_classification IN ('Membership Fee', 'Credit Billing')
                OR base.product_id IN (
                    1042905420, /* Fabletics Membership Token */
                    775063920, /* Bag Fee */
                    869844120, /* Bag Fee */
                    663639430 /* Xtra Savage Membership */
                ) THEN 'Not Expected to Have Cost'
            ELSE 'Unknown'
            END
        ) AS cost_source,
    base.group_key,
    base.sub_group_key,
    COALESCE(base.item_quantity, 0) AS item_quantity,
    fo.order_local_datetime,
    fo.payment_transaction_local_datetime,
    fo.shipped_local_datetime,
    fo.order_completion_local_datetime,
    COALESCE(fo.order_date_usd_conversion_rate, 1) AS order_date_usd_conversion_rate,
    COALESCE(fo.order_date_eur_conversion_rate, 1) AS order_date_eur_conversion_rate,
    COALESCE(fo.payment_transaction_date_usd_conversion_rate, 1) AS payment_transaction_date_usd_conversion_rate,
    COALESCE(fo.payment_transaction_date_eur_conversion_rate, 1) AS payment_transaction_date_eur_conversion_rate,
    COALESCE(fo.shipped_date_usd_conversion_rate, 1) AS shipped_date_usd_conversion_rate,
    COALESCE(fo.shipped_date_eur_conversion_rate, 1) AS shipped_date_eur_conversion_rate,
    COALESCE(fo.reporting_usd_conversion_rate, 1) AS reporting_usd_conversion_rate,
    COALESCE(fo.reporting_eur_conversion_rate, 1) AS reporting_eur_conversion_rate,
    COALESCE(fo.effective_vat_rate, 0) AS effective_vat_rate,
    COALESCE(amt.payment_transaction_local_amount, 0) AS payment_transaction_local_amount,
    COALESCE(amt.subtotal_excl_tariff_local_amount, 0) AS subtotal_excl_tariff_local_amount,
    COALESCE(amt.tax_local_amount, 0) AS tax_local_amount,
    COALESCE(amt.tax_cash_local_amount, 0) AS tax_cash_local_amount,
    COALESCE(amt.tax_credit_local_amount, 0) AS tax_credit_local_amount,
    COALESCE(amt.delivery_fee_local_amount,0) AS delivery_fee_local_amount,
    COALESCE(amt.product_discount_local_amount, 0) AS product_discount_local_amount,
    COALESCE(amt.shipping_discount_local_amount, 0) AS shipping_discount_local_amount,
    COALESCE(amt.shipping_revenue_before_discount_local_amount, 0) AS shipping_revenue_before_discount_local_amount,
    COALESCE(amt.shipping_revenue_before_discount_local_amount, 0) - COALESCE(amt.shipping_revenue_credit_local_amount, 0) AS shipping_revenue_cash_local_amount,
    COALESCE(amt.shipping_revenue_credit_local_amount, 0) AS shipping_revenue_credit_local_amount,
    COALESCE(amt.cash_credit_local_amount, 0) AS cash_credit_local_amount,
    COALESCE(amt.cash_membership_credit_local_amount, 0) AS cash_membership_credit_local_amount,
    COALESCE(amt.cash_refund_credit_local_amount, 0) AS cash_refund_credit_local_amount,
    COALESCE(amt.cash_giftco_credit_local_amount, 0) AS cash_giftco_credit_local_amount,
    COALESCE(amt.cash_giftcard_credit_local_amount, 0) AS cash_giftcard_credit_local_amount,
    COALESCE(td.token_count, 0) AS token_count,
    COALESCE(td.token_local_amount, 0) AS token_local_amount,
    COALESCE(td.cash_token_count, 0) AS cash_token_count,
    COALESCE(td.cash_token_local_amount, 0) AS cash_token_local_amount,
    COALESCE(td.non_cash_token_count, 0) AS non_cash_token_count,
    COALESCE(td.non_cash_token_local_amount, 0) AS non_cash_token_local_amount,
    COALESCE(amt.non_cash_credit_local_amount, 0) AS non_cash_credit_local_amount,
    COALESCE(pc.estimated_landed_cost, 0) AS estimated_landed_cost_local_amount,
    IFF(pc.cost_source = 'Oracle EBS Feed', pc.estimated_landed_cost, NULL) AS oracle_cost_local_amount,
    pc.lpn_po_cost AS lpn_po_cost_local_amount,
    pc.po_cost AS po_cost_local_amount,
    COALESCE(pc.misc_cogs_local_amount, 0) AS misc_cogs_local_amount,
    COALESCE(amt.shipping_cost_local_amount, 0) AS shipping_cost_local_amount,
    COALESCE(amt.estimated_shipping_supplies_cost_local_amount, 0) AS estimated_shipping_supplies_cost_local_amount,
    COALESCE(amt.estimated_shipping_cost_local_amount, 0) AS estimated_shipping_cost_local_amount,
    COALESCE(amt.estimated_variable_gms_cost_local_amount, 0) AS estimated_variable_gms_cost_local_amount,
    COALESCE(amt.estimated_variable_warehouse_cost_local_amount, 0) AS estimated_variable_warehouse_cost_local_amount,
    COALESCE(amt.estimated_variable_payment_processing_pct_cash_revenue, 0) AS estimated_variable_payment_processing_pct_cash_revenue,
    COALESCE(amt.bounceback_endowment_local_amount, 0) AS bounceback_endowment_local_amount,
    COALESCE(amt.vip_endowment_local_amount, 0) AS vip_endowment_local_amount,
    COALESCE(base.tariff_revenue_local_amount, 0) AS tariff_revenue_local_amount,
    COALESCE(base.retail_unit_price, 0) / (1 + COALESCE(fo.effective_vat_rate,0)) AS retail_unit_price,
    COALESCE(base.price_offered_local_amount,0) / (1 + COALESCE(fo.effective_vat_rate,0)) AS price_offered_local_amount,
    COALESCE(base.air_vip_price,0) /  (1 + COALESCE(fo.effective_vat_rate,0)) AS air_vip_price,
    base.bundle_price_offered_local_amount / (1 + COALESCE(fo.effective_vat_rate,0)) AS bundle_price_offered_local_amount  ,  /* no COALESCE, want this to be NULL instead of 0 */
    COALESCE(k.product_price_history_key, -1) AS product_price_history_key,
    COALESCE(k.bundle_product_price_history_key,-1) AS bundle_product_price_history_key,
    COALESCE(k.item_price_key, -1) AS item_price_key,
    fo.administrator_id,
    pc.lpn_code,
    COALESCE(be.bounceback_endowment_id, -1) AS bounceback_endowment_id,
    fo.is_deleted,
    fo.is_test_customer,
    base.custom_text AS custom_printed_text,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_order_line__stg_order_line_base AS base
    JOIN stg.fact_order AS fo
        ON fo.order_id = base.order_id
    LEFT JOIN _fact_order_line__stg_order_line_amounts AS amt
        ON amt.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__stg_order_line_token_data AS td
        ON td.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__stg_order_line_product_cost_data AS pc
        ON pc.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__keys AS k
        ON k.order_line_id = base.order_line_id
    LEFT JOIN _fact_order_line__bounceback_endowment AS be
        ON be.order_line_id = base.order_line_id;
-- SELECT * FROM _fact_order_line__stg;
-- SELECT count(1) FROM _fact_order_line__stg;
-- SELECT order_line_id, count(1) FROM _fact_order_line__stg group by 1 having count(1) > 1;


INSERT INTO stg.fact_order_line_stg (
    order_line_id,
    meta_original_order_line_id,
    currency_key,
    customer_id,
    store_id,
    product_id,
    master_product_id,
    bundle_product_id,
    bundle_component_product_id,
    group_code,
    bundle_component_history_key,
    product_type_key,
    bundle_order_line_id,
    order_membership_classification_key,
    order_sales_channel_key,
    order_line_status_key,
    order_id,
    shipping_address_id,
    billing_address_id,
    order_status_key,
    order_product_source_key,
    warehouse_id,
    cost_source_id,
    cost_source,
    group_key,
    sub_group_key,
    item_quantity,
    order_local_datetime,
    payment_transaction_local_datetime,
    shipped_local_datetime,
    order_completion_local_datetime,
    order_date_usd_conversion_rate,
    order_date_eur_conversion_rate,
    payment_transaction_date_usd_conversion_rate,
    payment_transaction_date_eur_conversion_rate,
    shipped_date_usd_conversion_rate,
    shipped_date_eur_conversion_rate,
    reporting_usd_conversion_rate,
    reporting_eur_conversion_rate,
    effective_vat_rate,
    payment_transaction_local_amount,
    subtotal_excl_tariff_local_amount,
    tax_local_amount,
    tax_cash_local_amount,
    tax_credit_local_amount,
    delivery_fee_local_amount,
    product_discount_local_amount,
    shipping_cost_local_amount,
    shipping_discount_local_amount,
    shipping_revenue_before_discount_local_amount,
    shipping_revenue_cash_local_amount,
    shipping_revenue_credit_local_amount,
    cash_credit_local_amount,
    cash_membership_credit_local_amount,
    cash_refund_credit_local_amount,
    cash_giftco_credit_local_amount,
    cash_giftcard_credit_local_amount,
    token_count,
    token_local_amount,
    cash_token_count,
    cash_token_local_amount,
    non_cash_token_count,
    non_cash_token_local_amount,
    non_cash_credit_local_amount,
    estimated_landed_cost_local_amount,
    oracle_cost_local_amount,
    lpn_po_cost_local_amount,
    po_cost_local_amount,
    misc_cogs_local_amount,
    estimated_shipping_supplies_cost_local_amount,
    estimated_shipping_cost_local_amount,
    estimated_variable_gms_cost_local_amount,
    estimated_variable_warehouse_cost_local_amount,
    estimated_variable_payment_processing_pct_cash_revenue,
    bounceback_endowment_local_amount,
    vip_endowment_local_amount,
    tariff_revenue_local_amount,
    retail_unit_price,
    price_offered_local_amount,
    air_vip_price,
    bundle_price_offered_local_amount,
    product_price_history_key,
    bundle_product_price_history_key,
    item_price_key,
    administrator_id,
    lpn_code,
    bounceback_endowment_id,
    is_deleted,
    is_test_customer,
    custom_printed_text,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    order_line_id,
    meta_original_order_line_id,
    currency_key,
    customer_id,
    store_id,
    product_id,
    master_product_id,
    bundle_product_id,
    bundle_component_product_id,
    group_code,
    bundle_component_history_key,
    product_type_key,
    bundle_order_line_id,
    order_membership_classification_key,
    order_sales_channel_key,
    order_line_status_key,
    order_id,
    shipping_address_id,
    billing_address_id,
    order_status_key,
    order_product_source_key,
    warehouse_id,
    cost_source_id,
    cost_source,
    group_key,
    sub_group_key,
    item_quantity,
    order_local_datetime,
    payment_transaction_local_datetime,
    shipped_local_datetime,
    order_completion_local_datetime,
    order_date_usd_conversion_rate,
    order_date_eur_conversion_rate,
    payment_transaction_date_usd_conversion_rate,
    payment_transaction_date_eur_conversion_rate,
    shipped_date_usd_conversion_rate,
    shipped_date_eur_conversion_rate,
    reporting_usd_conversion_rate,
    reporting_eur_conversion_rate,
    effective_vat_rate,
    payment_transaction_local_amount,
    subtotal_excl_tariff_local_amount,
    tax_local_amount,
    tax_cash_local_amount,
    tax_credit_local_amount,
    delivery_fee_local_amount,
    product_discount_local_amount,
    shipping_cost_local_amount,
    shipping_discount_local_amount,
    shipping_revenue_before_discount_local_amount,
    shipping_revenue_cash_local_amount,
    shipping_revenue_credit_local_amount,
    cash_credit_local_amount,
    cash_membership_credit_local_amount,
    cash_refund_credit_local_amount,
    cash_giftco_credit_local_amount,
    cash_giftcard_credit_local_amount,
    token_count,
    token_local_amount,
    cash_token_count,
    cash_token_local_amount,
    non_cash_token_count,
    non_cash_token_local_amount,
    non_cash_credit_local_amount,
    estimated_landed_cost_local_amount,
    oracle_cost_local_amount,
    lpn_po_cost_local_amount,
    po_cost_local_amount,
    misc_cogs_local_amount,
    estimated_shipping_supplies_cost_local_amount,
    estimated_shipping_cost_local_amount,
    estimated_variable_gms_cost_local_amount,
    estimated_variable_warehouse_cost_local_amount,
    estimated_variable_payment_processing_pct_cash_revenue,
    bounceback_endowment_local_amount,
    vip_endowment_local_amount,
    tariff_revenue_local_amount,
    retail_unit_price,
    price_offered_local_amount,
    air_vip_price,
    bundle_price_offered_local_amount,
    product_price_history_key,
    bundle_product_price_history_key,
    item_price_key,
    administrator_id,
    lpn_code,
    bounceback_endowment_id,
    is_deleted,
    is_test_customer,
    custom_printed_text,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_order_line__stg
ORDER BY
    order_line_id;
