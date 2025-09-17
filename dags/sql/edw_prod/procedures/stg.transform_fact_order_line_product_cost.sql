SET target_table = 'stg.fact_order_line_product_cost';
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

-- Switch warehouse if performing a full refresh
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_edw_stg_fact_order_line = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_line'));
SET wm_edw_stg_dim_lpn = (SELECT stg.udf_get_watermark($target_table,'edw_prod.stg.dim_lpn'));
SET wm_lake_ultra_warehouse_receipt = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.receipt'));
SET wm_edw_stg_dim_product = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_product'));
SET wm_reference_gsc_landed_cost = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.gsc_landed_cost'));
SET wm_lake_ultra_merchant_order_line_split_map = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line_split_map'));
SET wm_reference_gfc_po_sku_line_lpn_mapping= (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.gfc_po_sku_line_lpn_mapping'));


/*
SELECT
    $wm_self,
    $wm_edw_stg_fact_order_line,
    $wm_edw_stg_dim_lpn,
    $wm_lake_ultra_warehouse_receipt,
    $wm_edw_stg_dim_product,
    $wm_reference_gsc_landed_cost;
    $wm_reference_gfc_po_sku_line_lpn_mapping;
*/

CREATE OR REPLACE TEMP TABLE _fact_order_line_product_cost__order_line_base (order_line_id INT);

-- Full Refresh
INSERT INTO _fact_order_line_product_cost__order_line_base (order_line_id)
SELECT ol.order_line_id
FROM stg.fact_order_line AS ol
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
ORDER BY ol.order_id;

-- Incremental Refresh
INSERT INTO _fact_order_line_product_cost__order_line_base (order_line_id)
SELECT DISTINCT incr.order_line_id
FROM (
    /* Self-check for manual updates */
    SELECT order_line_id
    FROM stg.fact_order_line_product_cost
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    /* Check for dependency table updates */
    SELECT ol.order_line_id
    FROM stg.fact_order_line AS ol
    WHERE ol.meta_update_datetime > $wm_edw_stg_fact_order_line

    UNION ALL

    SELECT ol.order_line_id
    FROM stg.fact_order_line ol
    JOIN stg.dim_product p
        ON ol.product_id = p.product_id
    LEFT JOIN stg.dim_lpn l
        ON ol.lpn_code = l.lpn_code
    LEFT JOIN lake.ultra_warehouse.receipt r
        ON l.receipt_id = r.receipt_id
    LEFT JOIN reference.gfc_po_sku_line_lpn_mapping gcl
        ON l.lpn_id = gcl.lpn_id
    LEFT JOIN (
                SELECT DISTINCT
                        pod.warehouse_id,
                        pod.po_number,
                        pod.show_room,
                        pod.sku,
                        rcv.datetime_received
                FROM reference.gsc_po_detail_dataset pod
                JOIN lake.ultra_warehouse.receipt rcv
                    ON UPPER(pod.po_number) = UPPER(rcv.po_number)
                WHERE pod.po_status_text != 'Cancelled'
                    AND rcv.label NOT LIKE '%-C'
            ) x
        ON ol.warehouse_id = x.warehouse_id
        AND LOWER(p.sku) = LOWER(x.sku)
        AND x.datetime_received < convert_timezone('America/Los_Angeles', ol.order_completion_local_datetime)
    LEFT JOIN reference.gsc_landed_cost lc
        ON  UPPER(NVL(r.po_number, x.po_number)) = UPPER(lc.po_number)
        AND LOWER(p.sku) = LOWER(lc.sku)
    WHERE
        ol.order_completion_local_datetime IS NOT NULL
      AND p.product_type IN ('Unknown','Normal','Membership Reward Points Item')
      AND
        (
            ol.meta_update_datetime > $wm_edw_stg_fact_order_line
            OR l.meta_update_datetime > $wm_edw_stg_dim_lpn
            OR r.hvr_change_time > $wm_lake_ultra_warehouse_receipt
            OR p.meta_update_datetime > $wm_edw_stg_dim_product
            OR lc.meta_update_datetime > $wm_reference_gsc_landed_cost
            OR gcl.meta_update_datetime > $wm_reference_gfc_po_sku_line_lpn_mapping
        )

    UNION ALL

    SELECT order_line_id
    FROM lake_consolidated.ultra_merchant.order_line_split_map
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_order_line_split_map

    UNION ALL

    SELECT master_order_line_id AS order_line_id
    FROM lake_consolidated.ultra_merchant.order_line_split_map
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_order_line_split_map

    UNION ALL

    SELECT order_line_id
    FROM excp.fact_order_line_product_cost AS e
    WHERE e.meta_is_current_excp
        AND e.meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.order_line_id;

/* if we picked up a dropship split, we want to make sure we are refreshing both the order_line_id and master_order_line_id */
INSERT INTO _fact_order_line_product_cost__order_line_base (order_line_id)
SELECT
    olsm.master_order_line_id AS order_line_id
FROM _fact_order_line_product_cost__order_line_base AS base
    JOIN stg.fact_order_line AS fol
        ON fol.order_line_id = base.order_line_id
    JOIN lake_consolidated.ultra_merchant.order_line_split_map AS olsm
        ON olsm.order_line_id = base.order_line_id
WHERE fol.order_status_key = 8
AND $is_full_refresh = FALSE
AND NOT EXISTS (
    SELECT b.order_line_id
    FROM _fact_order_line_product_cost__order_line_base AS b
    WHERE b.order_line_id = olsm.master_order_line_id
    );

INSERT INTO _fact_order_line_product_cost__order_line_base (order_line_id)
SELECT
    fol.order_line_id AS order_line_id
FROM _fact_order_line_product_cost__order_line_base AS base
    JOIN lake_consolidated.ultra_merchant.order_line_split_map AS olsm
        ON olsm.master_order_line_id = base.order_line_id
      JOIN stg.fact_order_line AS fol
        ON fol.order_line_id = olsm.order_line_id
WHERE fol.order_status_key = 8
AND $is_full_refresh = FALSE
AND NOT EXISTS (
    SELECT b.order_line_id
    FROM _fact_order_line_product_cost__order_line_base AS b
    WHERE b.order_line_id = fol.order_line_id
    );

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', current_warehouse()) FROM _fact_order_line_product_cost__order_line_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

CREATE OR REPLACE TEMP TABLE _fact_order_line_product_cost__product_details_pre AS
SELECT
    base.order_line_id,
    ol.shipped_local_datetime,
    ol.order_completion_local_datetime,
    ol.estimated_landed_cost_local_amount,
    ol.order_local_datetime,
    ol.store_id,
    ol.lpn_code,
    ol.warehouse_id,
    p.product_id,
    p.master_product_id,
    p.item_id,
    p.membership_brand_id,
    p.product_type_id,
    p.default_product_category_id
FROM _fact_order_line_product_cost__order_line_base AS base
JOIN stg.fact_order_line ol
    ON base.order_line_id = ol.order_line_id
LEFT JOIN lake_consolidated.ultra_merchant_history.product AS p
    ON p.product_id = ol.product_id
    and convert_timezone('America/Los_Angeles', ol.order_local_datetime) between p.effective_start_datetime and p.effective_end_datetime
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.order_line_id ORDER BY p.effective_start_datetime DESC) = 1;

CREATE OR REPLACE TEMP TABLE _fact_order_line_product_cost__product_details AS
SELECT
    ol.order_line_id,
    ol.shipped_local_datetime,
    ol.order_completion_local_datetime,
    ol.estimated_landed_cost_local_amount,
    ol.store_id,
    ol.lpn_code,
    ol.warehouse_id,
    ol.product_id,
    TRIM(i.item_number) AS sku,
    dsku.product_sku,
    COALESCE(ol.membership_brand_id, mp.membership_brand_id) AS membership_brand_id,
    IFNULL(c1.label, 'Unknown') AS product_category,
    IFNULL(pt.label, 'Unknown') AS product_type
FROM _fact_order_line_product_cost__product_details_pre ol
LEFT JOIN lake_consolidated.ultra_merchant_history.product AS mp
    ON mp.product_id = nvl(ol.master_product_id, -1)
    AND convert_timezone('America/Los_Angeles', ol.order_local_datetime) between mp.effective_start_datetime and mp.effective_end_datetime
LEFT JOIN lake_consolidated.ultra_merchant.item AS i
    ON i.item_id = COALESCE(mp.item_id, ol.item_id)
LEFT JOIN stg.dim_sku AS dsku
    ON TRIM(i.item_number) = dsku.sku
LEFT JOIN lake_consolidated.ultra_merchant.product_type pt
        ON COALESCE(mp.product_type_id, ol.product_type_id) = pt.product_type_id
LEFT JOIN lake_consolidated.ultra_merchant.product_category c1
    ON COALESCE(mp.default_product_category_id, ol.default_product_category_id)  = c1.product_category_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY ol.order_line_id ORDER BY mp.effective_start_datetime DESC) = 1;

CREATE OR REPLACE TEMP TABLE _fact_order_line_product_cost__actual_cost_base AS
SELECT
    ob.order_line_id,
    ob.shipped_local_datetime,
    ob.order_completion_local_datetime,
    ob.estimated_landed_cost_local_amount,
    ds.store_brand as brand,
    nvl(gcl.sku, ob.sku) as sku,
    IFF(gcl.sku is not null, dsku.product_sku, ob.product_sku) as style_color,
    ob.product_category,
    NULL AS department,
    NULL AS category,
    NULL AS subcategory,
    NULL AS class,
    UPPER(r.po_number) as receipt_po,
    date_trunc('month', r.datetime_received) as receipt_year_month,
    gcl.po_line_number as receipt_po_line_number,
    UPPER(x.po_number) as alternate_po,
    date_trunc('month', x.datetime_received) as alternate_year_month,
    r.datetime_received as receipt_datetime,
    x.datetime_received as alternate_receipt_datetime,
    x.po_line_number as alternate_po_line_number,
    CASE WHEN l.lpn_code IS NULL THEN FALSE ELSE TRUE END AS is_lpn_found,
    CASE WHEN r.receipt_id IS NULL THEN FALSE ELSE TRUE END AS is_po_receipt_found,
    ob.product_type,
    ob.membership_brand_id
FROM _fact_order_line_product_cost__product_details ob
LEFT JOIN stg.dim_lpn l
    ON ob.lpn_code = l.lpn_code
LEFT JOIN stg.dim_store ds
    ON ds.store_id = ob.store_id
LEFT JOIN lake.ultra_warehouse.receipt r
    ON l.receipt_id = r.receipt_id
LEFT JOIN reference.gfc_po_sku_line_lpn_mapping gcl
        ON l.lpn_id = gcl.lpn_id
        AND NOT gcl.is_deleted
LEFT JOIN stg.dim_sku AS dsku
    ON lower(gcl.sku) = lower(dsku.sku)
LEFT JOIN (
            SELECT DISTINCT
                pod.warehouse_id,
                UPPER(pod.po_number) AS po_number,
                pod.po_line_number,
                pod.show_room,
                pod.sku,
                rcv.datetime_received,
                pod.landed_cost_estimated AS landed_cost,
                pod.qty,
                pod.date_create
            FROM reference.gsc_po_detail_dataset pod
            JOIN lake.ultra_warehouse.receipt rcv
                ON UPPER(pod.po_number) = UPPER(rcv.po_number)
            WHERE pod.po_status_text != 'Cancelled'
                AND rcv.label NOT LIKE '%-C'
                AND NOT pod.is_deleted
            QUALIFY row_number() over(partition by pod.warehouse_id, UPPER(pod.po_number),pod.sku order by pod.qty DESC, rcv.datetime_received DESC) = 1
        ) x
    ON ob.warehouse_id = x.warehouse_id
    AND LOWER(nvl(gcl.sku,ob.sku)) = LOWER(x.sku)
    AND x.datetime_received < convert_timezone('America/Los_Angeles', ob.order_completion_local_datetime)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ob.order_line_id ORDER BY alternate_year_month DESC, x.datetime_received DESC,  x.qty DESC, x.landed_cost DESC, x.date_create DESC) = 1;

CREATE OR REPLACE TEMP TABLE _gfb_product_skus AS
SELECT DISTINCT
     b.order_line_id
    ,a.department as department
    ,a.department_detail as category
    ,a.subcategory as subcategory
FROM _fact_order_line_product_cost__actual_cost_base b
JOIN reporting_prod.gfb.merch_dim_product a
    ON LOWER(a.product_sku) = LOWER(b.style_color)
WHERE b.brand IN ('JustFab', 'FabKids', 'ShoeDazzle')
QUALIFY ROW_NUMBER()OVER(PARTITION BY b.order_line_id ORDER BY a.department, a.department_detail, a.subcategory) = 1;


UPDATE _fact_order_line_product_cost__actual_cost_base a
SET
    a.department = b.department,
    a.category =  b.category,
    a.subcategory = b.subcategory
FROM _gfb_product_skus b
WHERE a.order_line_id = b.order_line_id;

CREATE OR REPLACE TEMP TABLE _sxf_product_skus AS
SELECT DISTINCT
    b.order_line_id,
    a.gender AS department,
    a.category AS category,
    a.subcategory AS subcategory
FROM _fact_order_line_product_cost__actual_cost_base b
JOIN reporting_prod.sxf.view_style_master_size a
    ON LOWER(a.sku) = LOWER(b.sku)
WHERE b.brand = 'Savage X';

UPDATE _fact_order_line_product_cost__actual_cost_base a
SET
    a.department = b.department,
    a.category =  b.category,
    a.subcategory = b.subcategory
FROM _sxf_product_skus b
WHERE a.order_line_id = b.order_line_id;


CREATE OR REPLACE TEMP TABLE _fl_product_skus AS(
with cte as
(
SELECT DISTINCT
    ubt.sku,
    ubt.gender,
    ubt.category,
    ubt.class,
    ubt.subclass
FROM lake_view.excel.fl_merch_items_ubt_hierarchy ubt
JOIN (SELECT
    sku,
    MAX(current_showroom) AS max_current_showroom
FROM lake_view.excel.fl_merch_items_ubt_hierarchy group by 1) as ms
    ON LOWER(ubt.sku) = LOWER(ms.sku)
    AND ubt.current_showroom = ms.max_current_showroom
)
SELECT
    b.order_line_id,
    a.gender AS department,
    a.category AS category,
    a.class AS subcategory,
    a.subclass AS class
FROM _fact_order_line_product_cost__actual_cost_base b
JOIN cte a
    ON LOWER(a.sku) = LOWER(b.style_color)
WHERE b.brand = 'Fabletics'
);

UPDATE _fact_order_line_product_cost__actual_cost_base a
SET
    a.department = b.department,
    a.category =  b.category,
    a.subcategory = b.subcategory,
    a.class = b.class
FROM _fl_product_skus b
WHERE a.order_line_id = b.order_line_id;

CREATE OR REPLACE TEMP TABLE _fact_order_line_product_cost__stg_actual_cost
(
    order_line_id number(38,0),
    actual_landed_cost number(38,6),
    actual_po_cost number(38,6),
    actual_cmt_cost number(38,6),
    actual_tariff_duty_cost number(38,6),
    actual_freight_cost number(38,6),
    actual_other_cost number(38,6),
    calculation_type VARCHAR(100),
    is_fully_landed BOOLEAN,
    exchange_rate_date DATE,
    estimated_landed_cost_modified number(38,6)
);

-- The conversion and addition of extra costs to estimated_landed_cost is to satisfy the requirement for diverted shipments from (DA-35330).
-- The repeated logic to get estimated_landed_cost converted to local amount joining fol and currency_exchange can be improvised, which we will take up in coming days.
INSERT INTO _fact_order_line_product_cost__stg_actual_cost
(
    order_line_id,
    actual_landed_cost,
    actual_po_cost,
    actual_cmt_cost,
    actual_tariff_duty_cost,
    actual_freight_cost,
    actual_other_cost,
    calculation_type,
    is_fully_landed,
    exchange_rate_date,
    estimated_landed_cost_modified
)
SELECT
    ol.order_line_id,
    lc.actual_landed_cost_per_unit as total_actual_landed_cost,
    lc.po_cost_without_commission as actual_po_cost,
    nvl(lc.cmt_cost, 0)  as actual_cmt_cost,
    nvl(lc.actual_duty_cost_per_unit, 0) + nvl(lc.actual_tariff_cost_per_unit, 0) as actual_tariff_duty_cost,
    nvl(lc.cpu_freight, 0) + nvl(lc.cpu_ocean, 0) + nvl(lc.cpu_air, 0) as actual_freight_cost,
    nvl(lc.cpu_transload, 0) + nvl(lc.cpu_otr, 0) + nvl(lc.cpu_domestic, 0) +nvl(cpu_pierpass,0)+ nvl(agency_cost_and_po_cost,0)-nvl(po_cost_without_commission,0) as actual_other_cost,
    'NEW (PO/SKU/PO_LINE_NUMBER/RECEIPT)' as calculation_type,
    fully_landed as is_fully_landed,
    nvl(ol.receipt_datetime, ol.alternate_receipt_datetime)::DATE as exchange_rate_date,
    ol.estimated_landed_cost_local_amount
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_freight_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_freight_cost_per_unit, 0))
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_duty_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_duty_cost_per_unit, 0))
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_tariff_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_tariff_cost_per_unit, 0))
            as estimated_landed_cost_modified
FROM _fact_order_line_product_cost__actual_cost_base ol
JOIN reference.gsc_landed_cost lc
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po))  =  UPPER(lc.po_number)
    AND LOWER(ol.sku) = LOWER(lc.sku)
    AND iff(ol.receipt_po is not null, ol.receipt_year_month, ol.alternate_year_month) = lc.year_month_received
    AND EQUAL_NULL(iff(ol.receipt_po is not null, ol.receipt_po_line_number, ol.alternate_po_line_number) , lc.po_line_number)
LEFT JOIN reporting_base_prod.reference.po_override_lcosts_modified polm
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po))  =  UPPER(polm.po)
    AND LOWER(ol.sku) = LOWER(polm.sku)
    AND iff(ol.receipt_po is not null, ol.receipt_year_month, ol.alternate_year_month) = polm.year_month_received
    AND EQUAL_NULL(iff(ol.receipt_po is not null, ol.receipt_po_line_number, ol.alternate_po_line_number) , polm.po_line_number)
LEFT JOIN stg.fact_order_line AS fol
    ON fol.order_line_id = ol.order_line_id
LEFT JOIN reference.store_currency AS ds
    ON ds.store_id = fol.store_id
    AND fol.order_local_datetime BETWEEN ds.effective_start_date AND ds.effective_end_date
LEFT JOIN reference.currency_exchange_rate_by_date AS cer
    ON cer.rate_date_pst = nvl(ol.receipt_datetime, ol.alternate_receipt_datetime)::DATE
    AND upper(cer.src_currency) = 'USD'
    AND upper(cer.dest_currency) = upper(ds.currency_code)
WHERE nvl(ol.receipt_year_month, ol.alternate_year_month) >= '2021-07-01'
    AND lc.history = 'N'
    AND lc.total_actual_landed_cost IS NOT NULL
    AND ol.order_completion_local_datetime IS NOT NULL
    AND ol.product_type IN ('Unknown','Normal','Membership Reward Points Item')
    AND NOT lc.is_deleted
QUALIFY row_number() OVER (PARTITION BY ol.order_line_id ORDER BY lc.year_month_received DESC, lc.actual_landed_cost_per_unit DESC) = 1;

INSERT INTO _fact_order_line_product_cost__stg_actual_cost
(
    order_line_id,
    actual_landed_cost,
    actual_po_cost,
    actual_cmt_cost,
    actual_tariff_duty_cost,
    actual_freight_cost,
    actual_other_cost,
    calculation_type,
    is_fully_landed,
    exchange_rate_date,
    estimated_landed_cost_modified
)
SELECT
    ol.order_line_id,
    lc.actual_landed_cost_per_unit as total_actual_landed_cost,
    lc.po_cost_without_commission as actual_po_cost,
    nvl(lc.cmt_cost, 0)  as actual_cmt_cost,
    nvl(lc.actual_duty_cost_per_unit, 0) + nvl(lc.actual_tariff_cost_per_unit, 0) as actual_tariff_duty_cost,
    nvl(lc.cpu_freight, 0) + nvl(lc.cpu_ocean, 0) + nvl(lc.cpu_air, 0) as actual_freight_cost,
    nvl(lc.cpu_transload, 0) + nvl(lc.cpu_otr, 0) + nvl(lc.cpu_domestic, 0) +nvl(cpu_pierpass,0)+ nvl(agency_cost_and_po_cost,0)-nvl(po_cost_without_commission,0) as actual_other_cost,
    'NEW (PO/SKU/RECEIPT)' as calculation_type,
    fully_landed as is_fully_landed,
    nvl(ol.receipt_datetime, ol.alternate_receipt_datetime)::DATE as exchange_rate_date,
    ol.estimated_landed_cost_local_amount
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_freight_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_freight_cost_per_unit, 0))
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_duty_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_duty_cost_per_unit, 0))
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_tariff_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_tariff_cost_per_unit, 0))
            as estimated_landed_cost_modified
FROM _fact_order_line_product_cost__actual_cost_base ol
JOIN reference.gsc_landed_cost lc
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po)) = UPPER(lc.po_number)
    AND LOWER(ol.sku) = LOWER(lc.sku)
    AND iff(ol.receipt_po is not null, ol.receipt_year_month, ol.alternate_year_month) = lc.year_month_received
LEFT JOIN reporting_base_prod.reference.po_override_lcosts_modified polm
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po))  =  UPPER(polm.po)
    AND LOWER(ol.sku) = LOWER(polm.sku)
    AND iff(ol.receipt_po is not null, ol.receipt_year_month, ol.alternate_year_month) = polm.year_month_received
LEFT JOIN _fact_order_line_product_cost__stg_actual_cost apc
    ON apc.order_line_id = ol.order_line_id
LEFT JOIN stg.fact_order_line AS fol
    ON fol.order_line_id = ol.order_line_id
LEFT JOIN reference.store_currency AS ds
    ON ds.store_id = fol.store_id
    AND fol.order_local_datetime BETWEEN ds.effective_start_date AND ds.effective_end_date
LEFT JOIN reference.currency_exchange_rate_by_date AS cer
    ON cer.rate_date_pst = nvl(ol.receipt_datetime, ol.alternate_receipt_datetime)::DATE
    AND upper(cer.src_currency) = 'USD'
    AND upper(cer.dest_currency) = upper(ds.currency_code)
WHERE nvl(ol.receipt_year_month, ol.alternate_year_month) >= '2021-07-01'
    AND apc.order_line_id IS NULL
    AND lc.history = 'N'
    AND lc.total_actual_landed_cost IS NOT NULL
    AND ol.order_completion_local_datetime IS NOT NULL
    AND ol.product_type IN ('Unknown','Normal','Membership Reward Points Item')
    AND NOT lc.is_deleted
QUALIFY row_number() OVER (PARTITION BY ol.order_line_id ORDER BY lc.year_month_received DESC, lc.actual_landed_cost_per_unit DESC) = 1;


INSERT INTO _fact_order_line_product_cost__stg_actual_cost
(
    order_line_id,
    actual_landed_cost,
    actual_po_cost,
    actual_cmt_cost,
    actual_tariff_duty_cost,
    actual_freight_cost,
    actual_other_cost,
    calculation_type,
    is_fully_landed,
    exchange_rate_date
)
SELECT
    ol.order_line_id,
    lc.actual_landed_cost_per_unit as total_actual_landed_cost,
    lc.po_cost_without_commission as actual_po_cost,
    nvl(lc.cmt_cost, 0)  as actual_cmt_cost,
    nvl(lc.actual_duty_cost_per_unit, 0) + nvl(lc.actual_tariff_cost_per_unit, 0) as actual_tariff_duty_cost,
    nvl(lc.cpu_freight, 0) + nvl(lc.cpu_ocean, 0) + nvl(lc.cpu_air, 0) as actual_freight_cost,
    nvl(lc.cpu_transload, 0) + nvl(lc.cpu_otr, 0) + nvl(lc.cpu_domestic, 0) +nvl(cpu_pierpass,0)+ nvl(agency_cost_and_po_cost,0)-nvl(po_cost_without_commission,0) as actual_other_cost,
    'HISTORY (PO/STYLE-COLOR/RECEIPT)' as calculation_type,
    TRUE as is_fully_landed,
    nvl(ol.receipt_datetime, ol.alternate_receipt_datetime)::DATE as exchange_rate_date
FROM _fact_order_line_product_cost__actual_cost_base ol
LEFT JOIN _fact_order_line_product_cost__stg_actual_cost apc
    ON apc.order_line_id = ol.order_line_id
JOIN reporting_base_prod.gsc.landed_cost_history lc
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po)) = UPPER(lc.po_number)
    AND LOWER(ol.style_color) = LOWER(lc.sku)
    AND iff(ol.receipt_po is not null, ol.receipt_year_month, ol.alternate_year_month) = lc.year_month_received
WHERE nvl(ol.receipt_year_month, ol.alternate_year_month) < '2021-07-01'
    AND apc.order_line_id IS NULL
    AND lc.total_actual_landed_cost IS NOT NULL
    AND ol.order_completion_local_datetime IS NOT NULL
    AND ol.product_type IN ('Unknown','Normal','Membership Reward Points Item')
QUALIFY row_number() OVER (PARTITION BY ol.order_line_id ORDER BY lc.year_month_received DESC, lc.actual_landed_cost_per_unit DESC) = 1;

INSERT INTO _fact_order_line_product_cost__stg_actual_cost
(
    order_line_id,
    actual_landed_cost,
    actual_po_cost,
    actual_cmt_cost,
    actual_tariff_duty_cost,
    actual_freight_cost,
    actual_other_cost,
    calculation_type,
    is_fully_landed,
    exchange_rate_date,
    estimated_landed_cost_modified
)
SELECT
    ol.order_line_id,
    lc.actual_landed_cost_per_unit as total_actual_landed_cost,
    lc.po_cost_without_commission as actual_po_cost,
    nvl(lc.cmt_cost, 0)  as actual_cmt_cost,
    nvl(lc.actual_duty_cost_per_unit, 0) + nvl(lc.actual_tariff_cost_per_unit, 0) as actual_tariff_duty_cost,
    nvl(lc.cpu_freight, 0) + nvl(lc.cpu_ocean, 0) + nvl(lc.cpu_air, 0) as actual_freight_cost,
    nvl(lc.cpu_transload, 0) + nvl(lc.cpu_otr, 0) + nvl(lc.cpu_domestic, 0) +nvl(cpu_pierpass,0)+ nvl(agency_cost_and_po_cost,0)-nvl(po_cost_without_commission,0) as actual_other_cost,
    'NEW (PO/SKU/PO_LINE_NUMBER)' as calculation_type,
    fully_landed as is_fully_landed,
    lc.year_month_received::DATE as exchange_rate_date,
    ol.estimated_landed_cost_local_amount
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_freight_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_freight_cost_per_unit, 0))
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_duty_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_duty_cost_per_unit, 0))
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_tariff_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_tariff_cost_per_unit, 0))
            as estimated_landed_cost_modified
FROM _fact_order_line_product_cost__actual_cost_base ol
LEFT JOIN _fact_order_line_product_cost__stg_actual_cost apc
    ON apc.order_line_id = ol.order_line_id
JOIN reference.gsc_landed_cost lc
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po)) =  UPPER(lc.po_number)
    AND LOWER(ol.sku) = LOWER(lc.sku)
    AND EQUAL_NULL(nvl(ol.receipt_po_line_number, ol.alternate_po_line_number) , lc.po_line_number)
LEFT JOIN reporting_base_prod.reference.po_override_lcosts_modified polm
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po))  =  UPPER(polm.po)
    AND LOWER(ol.sku) = LOWER(polm.sku)
    AND EQUAL_NULL(iff(ol.receipt_po is not null, ol.receipt_po_line_number, ol.alternate_po_line_number) , polm.po_line_number)
LEFT JOIN stg.fact_order_line AS fol
    ON fol.order_line_id = ol.order_line_id
LEFT JOIN reference.store_currency AS ds
    ON ds.store_id = fol.store_id
    AND fol.order_local_datetime BETWEEN ds.effective_start_date AND ds.effective_end_date
LEFT JOIN reference.currency_exchange_rate_by_date AS cer
    ON cer.rate_date_pst =  nvl(ol.receipt_datetime, ol.alternate_receipt_datetime)::DATE
    AND upper(cer.src_currency) = 'USD'
    AND upper(cer.dest_currency) = upper(ds.currency_code)
WHERE  nvl(ol.receipt_year_month, ol.alternate_year_month) >= '2021-07-01'
    AND lc.history = 'N'
    AND apc.order_line_id IS NULL
    AND lc.total_actual_landed_cost IS NOT NULL
    AND ol.order_completion_local_datetime IS NOT NULL
    AND ol.product_type IN ('Unknown','Normal','Membership Reward Points Item')
    AND NOT lc.is_deleted
QUALIFY row_number() OVER (PARTITION BY ol.order_line_id ORDER BY lc.year_month_received DESC, lc.actual_landed_cost_per_unit DESC) = 1;

INSERT INTO _fact_order_line_product_cost__stg_actual_cost
(
    order_line_id,
    actual_landed_cost,
    actual_po_cost,
    actual_cmt_cost,
    actual_tariff_duty_cost,
    actual_freight_cost,
    actual_other_cost,
    calculation_type,
    is_fully_landed,
    exchange_rate_date,
    estimated_landed_cost_modified
)
SELECT
    ol.order_line_id,
    lc.actual_landed_cost_per_unit as total_actual_landed_cost,
    lc.po_cost_without_commission as actual_po_cost,
    nvl(lc.cmt_cost, 0)  as actual_cmt_cost,
    nvl(lc.actual_duty_cost_per_unit, 0) + nvl(lc.actual_tariff_cost_per_unit, 0) as actual_tariff_duty_cost,
    nvl(lc.cpu_freight, 0) + nvl(lc.cpu_ocean, 0) + nvl(lc.cpu_air, 0) as actual_freight_cost,
    nvl(lc.cpu_transload, 0) + nvl(lc.cpu_otr, 0) + nvl(lc.cpu_domestic, 0) +nvl(cpu_pierpass,0)+ nvl(agency_cost_and_po_cost,0)-nvl(po_cost_without_commission,0) as actual_other_cost,
    'NEW (PO/SKU)' as calculation_type,
    fully_landed as is_fully_landed,
    lc.year_month_received::DATE as exchange_rate_date,
    ol.estimated_landed_cost_local_amount
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_freight_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_freight_cost_per_unit, 0))
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_duty_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_duty_cost_per_unit, 0))
        + nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(polm.extra_tariff_cost_per_unit, 0),
                                          nvl(cer.exchange_rate, 1) * COALESCE(polm.extra_tariff_cost_per_unit, 0))
            as estimated_landed_cost_modified
FROM _fact_order_line_product_cost__actual_cost_base ol
LEFT JOIN _fact_order_line_product_cost__stg_actual_cost apc
    ON apc.order_line_id = ol.order_line_id
JOIN reference.gsc_landed_cost lc
    ON EQUAL_NULL(UPPER(NVL(ol.receipt_po, ol.alternate_po)) , UPPER(lc.po_number))
    AND EQUAL_NULL(LOWER(ol.sku) , LOWER(lc.sku))
LEFT JOIN reporting_base_prod.reference.po_override_lcosts_modified polm
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po))  =  UPPER(polm.po)
    AND LOWER(ol.sku) = LOWER(polm.sku)
LEFT JOIN stg.fact_order_line AS fol
    ON fol.order_line_id = ol.order_line_id
LEFT JOIN reference.store_currency AS ds
    ON ds.store_id = fol.store_id
    AND fol.order_local_datetime BETWEEN ds.effective_start_date AND ds.effective_end_date
LEFT JOIN reference.currency_exchange_rate_by_date AS cer
    ON cer.rate_date_pst = apc.exchange_rate_date
    AND upper(cer.src_currency) = 'USD'
    AND upper(cer.dest_currency) = upper(ds.currency_code)
WHERE  nvl(ol.receipt_year_month, ol.alternate_year_month) >= '2021-07-01'
    AND lc.history = 'N'
    AND apc.order_line_id IS NULL
    AND lc.total_actual_landed_cost IS NOT NULL
    AND ol.order_completion_local_datetime IS NOT NULL
    AND ol.product_type IN ('Unknown','Normal','Membership Reward Points Item')
    AND NOT lc.is_deleted
QUALIFY row_number() OVER (PARTITION BY ol.order_line_id ORDER BY lc.year_month_received DESC, lc.actual_landed_cost_per_unit DESC) = 1;


INSERT INTO _fact_order_line_product_cost__stg_actual_cost
(
    order_line_id,
    actual_landed_cost,
    actual_po_cost,
    actual_cmt_cost,
    actual_tariff_duty_cost,
    actual_freight_cost,
    actual_other_cost,
    calculation_type,
    is_fully_landed,
    exchange_rate_date
)
SELECT
    ol.order_line_id,
    lc.actual_landed_cost_per_unit as total_actual_landed_cost,
    lc.po_cost_without_commission as actual_po_cost,
    nvl(lc.cmt_cost, 0)  as actual_cmt_cost,
    nvl(lc.actual_duty_cost_per_unit, 0) + nvl(lc.actual_tariff_cost_per_unit, 0) as actual_tariff_duty_cost,
    nvl(lc.cpu_freight, 0) + nvl(lc.cpu_ocean, 0) + nvl(lc.cpu_air, 0) as actual_freight_cost,
    nvl(lc.cpu_transload, 0) + nvl(lc.cpu_otr, 0) + nvl(lc.cpu_domestic, 0) +nvl(cpu_pierpass,0)+ nvl(agency_cost_and_po_cost,0)-nvl(po_cost_without_commission,0) as actual_other_cost,
    'HISTORY (PO/STYLE-COLOR)' as calculation_type,
    TRUE as is_fully_landed,
    lc.year_month_received::DATE as exchange_rate_date
FROM _fact_order_line_product_cost__actual_cost_base ol
LEFT JOIN _fact_order_line_product_cost__stg_actual_cost apc
    ON apc.order_line_id = ol.order_line_id
JOIN reporting_base_prod.gsc.landed_cost_history lc
    ON UPPER(NVL(ol.receipt_po, ol.alternate_po)) = UPPER(lc.po_number)
    AND LOWER(ol.style_color) = LOWER(lc.sku)
WHERE  nvl(ol.receipt_year_month, ol.alternate_year_month) < '2021-07-01'
AND apc.order_line_id IS NULL
AND lc.total_actual_landed_cost IS NOT NULL
AND ol.order_completion_local_datetime IS NOT NULL
AND ol.product_type IN ('Unknown','Normal','Membership Reward Points Item')
QUALIFY row_number() OVER (PARTITION BY ol.order_line_id ORDER BY lc.year_month_received DESC, lc.actual_landed_cost_per_unit DESC) = 1;

----------------------------
-- prepare staging table  --
----------------------------
CREATE OR REPLACE TEMP TABLE _fact_order_line_product_cost__stg AS
SELECT
    base.order_line_id,
    fol.meta_original_order_line_id,
    fol.order_id,
    fol.store_id,
    bca.shipped_local_datetime,
    bca.order_completion_local_datetime,
    coalesce(apc.estimated_landed_cost_modified,bca.estimated_landed_cost_local_amount) AS estimated_landed_cost_local_amount,
    bca.brand,
    bca.sku,
    bca.style_color,
    bca.product_category,
    bca.department,
    bca.category,
    bca.subcategory,
    bca.class,
    bca.receipt_po,
    COALESCE(bca.receipt_po_line_number, -1) AS receipt_po_line_number,
    bca.receipt_year_month,
    bca.alternate_po,
    COALESCE(bca.alternate_po_line_number, -1) AS alternate_po_line_number,
    bca.alternate_year_month,
    bca.receipt_datetime,
    bca.alternate_receipt_datetime,
    bca.is_lpn_found,
    bca.is_po_receipt_found,
    COALESCE(fol.currency_key, -1) AS currency_key,
    fol.is_deleted,
    CASE
        WHEN bca.membership_brand_id IN (10, 11, 12, 13) OR apc.actual_landed_cost > 85
            THEN IFNULL(coalesce(apc.estimated_landed_cost_modified,bca.estimated_landed_cost_local_amount), 0)
        ELSE
                NVL(fol.item_quantity, 1) *
                IFF(cer.src_currency = ds.currency_code, COALESCE(apc.actual_landed_cost, 0),
                    NVL(cer.exchange_rate, 1) *
                    COALESCE(apc.actual_landed_cost, 0)) END AS actual_landed_cost_local_amount,
    nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(apc.actual_po_cost, 0), nvl(cer.exchange_rate, 1) * COALESCE(apc.actual_po_cost, 0)) as actual_po_cost_local_amount,
    nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(apc.actual_cmt_cost, 0), nvl(cer.exchange_rate, 1) * COALESCE(apc.actual_cmt_cost, 0)) as actual_cmt_cost_local_amount,
    nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(apc.actual_tariff_duty_cost, 0), nvl(cer.exchange_rate, 1) * COALESCE(apc.actual_tariff_duty_cost, 0)) as actual_tariff_duty_cost_local_amount,
    nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(apc.actual_freight_cost, 0), nvl(cer.exchange_rate, 1) * COALESCE(apc.actual_freight_cost, 0)) as actual_freight_cost_local_amount,
    nvl(fol.item_quantity, 1) * IFF(cer.src_currency = ds.currency_code, COALESCE(apc.actual_other_cost, 0), nvl(cer.exchange_rate, 1) * COALESCE(apc.actual_other_cost, 0)) as actual_other_cost_local_amount,
    apc.calculation_type,
    IFF(bca.membership_brand_id IN (10, 11, 12, 13), TRUE, apc.is_fully_landed) AS is_fully_landed,
    sc.order_classification_l1,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_order_line_product_cost__order_line_base AS base
    JOIN _fact_order_line_product_cost__actual_cost_base as bca
        ON bca.order_line_id = base.order_line_id
    JOIN stg.fact_order_line AS fol
        ON fol.order_line_id = bca.order_line_id
    LEFT JOIN stg.dim_order_sales_channel AS sc
        ON fol.order_sales_channel_key = sc.order_sales_channel_key
    JOIN reference.store_currency AS ds
        ON ds.store_id = fol.store_id
        AND fol.order_local_datetime BETWEEN ds.effective_start_date AND ds.effective_end_date
    LEFT JOIN _fact_order_line_product_cost__stg_actual_cost AS apc
        ON apc.order_line_id = base.order_line_id
    LEFT JOIN reference.currency_exchange_rate_by_date AS cer
        ON cer.rate_date_pst = apc.exchange_rate_date
        AND upper(cer.src_currency) = 'USD'
        AND upper(cer.dest_currency) = upper(ds.currency_code);


/* for dropship splits, we are mapping the costs from the child order_line_id to the master order_line_id.  For master orders, we apply the MIN shipped local amongst the child.
   The child orders get shipped out at different times or may not have all shipped out yet, so the cost that flows to the master order_line_id is not fully accurate since it's based off of 1 shipped date */

UPDATE _fact_order_line_product_cost__stg AS t
SET
    t.actual_landed_cost_local_amount = s.actual_landed_cost_local_amount,
    t.actual_po_cost_local_amount = s.actual_po_cost_local_amount,
    t.actual_cmt_cost_local_amount = s.actual_cmt_cost_local_amount,
    t.actual_tariff_duty_cost_local_amount = s.actual_tariff_duty_cost_local_amount,
    t.actual_freight_cost_local_amount = s.actual_freight_cost_local_amount,
    t.actual_other_cost_local_amount = s.actual_other_cost_local_amount,
    t.calculation_type = s.calculation_type,
    t.is_fully_landed = s.is_fully_landed
FROM (
    SELECT
        olsm.master_order_line_id AS order_line_id,
        ac.actual_landed_cost_local_amount,
        ac.actual_po_cost_local_amount,
        ac.actual_cmt_cost_local_amount,
        ac.actual_tariff_duty_cost_local_amount,
        ac.actual_freight_cost_local_amount,
        ac.actual_other_cost_local_amount,
        ac.calculation_type,
        ac.is_fully_landed
    FROM _fact_order_line_product_cost__stg AS ac
        JOIN stg.fact_order_line AS fol
            ON fol.order_line_id = ac.order_line_id
        JOIN lake_consolidated.ultra_merchant.order_line_split_map AS olsm
            ON olsm.order_line_id = fol.order_line_id
    WHERE fol.order_status_key = 8
) AS s
WHERE s.order_line_id = t.order_line_id;


INSERT INTO stg.fact_order_line_product_cost_stg (
    order_line_id,
    meta_original_order_line_id,
    currency_key,
    store_id,
    order_id,
    shipped_local_datetime,
    order_completion_local_datetime,
    estimated_landed_cost_local_amount,
    brand,
    sku,
    style_color,
    product_category,
    department,
    category,
    subcategory,
    class,
    receipt_po,
    receipt_year_month,
    receipt_po_line_number,
    alternate_po,
    alternate_year_month,
    alternate_po_line_number,
    receipt_datetime,
    alternate_receipt_datetime,
    is_lpn_found,
    is_po_receipt_found,
    is_deleted,
    actual_landed_cost_local_amount,
    actual_po_cost_local_amount,
    actual_cmt_cost_local_amount,
    actual_tariff_duty_cost_local_amount,
    actual_freight_cost_local_amount,
    actual_other_cost_local_amount,
    calculation_type,
    is_fully_landed,
    reporting_landed_cost_local_amount,
    is_actual_landed_cost,
    fully_landed_conversion_date,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    a.order_line_id,
    a.meta_original_order_line_id,
    a.currency_key,
    a.store_id,
    a.order_id,
    a.shipped_local_datetime,
    a.order_completion_local_datetime,
    a.estimated_landed_cost_local_amount,
    a.brand,
    a.sku,
    a.style_color,
    a.product_category,
    a.department,
    a.category,
    a.subcategory,
    a.class,
    a.receipt_po,
    a.receipt_year_month,
    a.receipt_po_line_number,
    a.alternate_po,
    a.alternate_year_month,
    a.alternate_po_line_number,
    a.receipt_datetime,
    a.alternate_receipt_datetime,
    a.is_lpn_found,
    a.is_po_receipt_found,
    a.is_deleted,
    a.actual_landed_cost_local_amount,
    a.actual_po_cost_local_amount,
    a.actual_cmt_cost_local_amount,
    a.actual_tariff_duty_cost_local_amount,
    a.actual_freight_cost_local_amount,
    a.actual_other_cost_local_amount,
    a.calculation_type,
    a.is_fully_landed,
    IFF(a.order_classification_l1 IN('Exchange','Reship','Product Order') AND NVL(a.is_fully_landed, FALSE) = TRUE AND a.actual_landed_cost_local_amount>0, a.actual_landed_cost_local_amount , a.estimated_landed_cost_local_amount) AS reporting_landed_cost_local_amount,
    IFF(a.order_classification_l1 IN('Exchange','Reship','Product Order')  AND NVL(a.is_fully_landed, FALSE) = TRUE AND a.actual_landed_cost_local_amount>0, 1,0) AS is_actual_landed_cost,
    IFF(
            (
                NVL(b.is_fully_landed, FALSE) = 'FALSE'
                AND b.fully_landed_conversion_date IS NULL
                AND NVL(a.is_fully_landed, FALSE) = 'TRUE'
            )
            OR
            (
                b.order_line_id IS NULL
                AND NVL(a.is_fully_landed, FALSE) = 'TRUE'
            ),
            current_timestamp::timestamp_ltz,
            IFF(NVL(a.is_fully_landed, FALSE) = 'FALSE', NULL, b.fully_landed_conversion_date)
        ) AS fully_landed_conversion_date,
    a.meta_create_datetime,
    a.meta_update_datetime
FROM _fact_order_line_product_cost__stg a
LEFT JOIN stg.fact_order_line_product_cost b
    ON a.order_line_id = b.order_line_id
ORDER BY
    a.order_line_id;
-- SELECT order_line_id, COUNT(1) FROM stg.fact_order_line_product_cost_stg GROUP BY 1 HAVING COUNT(1) > 1;
