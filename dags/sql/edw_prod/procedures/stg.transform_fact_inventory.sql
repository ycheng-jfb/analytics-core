SET target_table = 'stg.fact_inventory';
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
SET wm_lake_ultra_warehouse_company = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.company'));
SET wm_lake_ultra_warehouse_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.item'));
SET wm_lake_ultra_warehouse_item_warehouse = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.item_warehouse'));
SET wm_lake_ultra_warehouse_warehouse = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.warehouse'));
SET wm_edw_reference_gfc_inventory_cost = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.gfc_inventory_cost'));
SET wm_lake_jfb_ultra_partner_fulfillment_partner_item_warehouse = (SELECT stg.udf_get_watermark($target_table, 'lake_jfb.ultra_partner.fulfillment_partner_item_warehouse'));
SET wm_lake_consolidated_ultra_merchant_item = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.item'));
SET wm_lake_centric_ed_sku = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_sku'));
SET wm_lake_centric_ed_style = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_style'));
SET wm_lake_centric_ed_colorway = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_colorway'));
SET wm_lake_centric_tfg_marketplace_costs = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.tfg_marketplace_costs'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_warehouse_company,
    $wm_lake_ultra_warehouse_item,
    $wm_lake_ultra_warehouse_item_warehouse,
    $wm_lake_ultra_warehouse_warehouse,
    $$wm_edw_reference_gfc_inventory_cost;
*/

CREATE OR REPLACE TEMP TABLE _fact_inventory__base (item_id INT, warehouse_id INT);

-- Full Refresh
INSERT INTO _fact_inventory__base (item_id, warehouse_id)
SELECT DISTINCT fr.item_id, fr.warehouse_id
FROM (SELECT iw.item_id, iw.warehouse_id
      FROM lake.ultra_warehouse.item_warehouse AS iw
               JOIN lake.ultra_warehouse.item AS i
                    ON i.item_id = iw.item_id
      WHERE $is_full_refresh
      UNION ALL
      SELECT iw.item_id, iw.warehouse_id
      FROM lake_jfb.ultra_partner.fulfillment_partner_item_warehouse AS iw
               JOIN lake_consolidated.ultra_merchant.item AS i
                    ON i.meta_original_item_id = iw.item_id
                    AND i.partner_item_number IS NOT NULL
      WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
     ) fr
ORDER BY fr.item_id, fr.warehouse_id;

-- Incremental Refresh
INSERT INTO _fact_inventory__base (item_id, warehouse_id)
SELECT DISTINCT incr.item_id, incr.warehouse_id
FROM (
    /* Self-check for manual updates */
    SELECT fi.item_id, fi.warehouse_id
    FROM stg.fact_inventory AS fi
    WHERE fi.meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT iw.item_id, iw.warehouse_id
    FROM lake.ultra_warehouse.item_warehouse AS iw
        JOIN lake.ultra_warehouse.item AS i
            ON i.item_id = iw.item_id
        LEFT JOIN lake.ultra_warehouse.company AS c
            ON c.company_id = i.company_id
        LEFT JOIN lake.ultra_warehouse.warehouse AS w
            ON w.warehouse_id = iw.warehouse_id
        LEFT JOIN reference.gfc_inventory_cost AS gic
            ON gic.warehouse_id = iw.warehouse_id
            AND UPPER(gic.sku) = UPPER(i.item_number)
	WHERE i.hvr_change_time > $wm_lake_ultra_warehouse_item
        OR iw.hvr_change_time > $wm_lake_ultra_warehouse_item_warehouse
        OR c.hvr_change_time > $wm_lake_ultra_warehouse_company
        OR w.hvr_change_time > $wm_lake_ultra_warehouse_warehouse
        OR gic.meta_update_datetime > $wm_edw_reference_gfc_inventory_cost
    UNION ALL
    /* Incorporating miracle mile incremental logic*/
    SELECT iw.item_id, iw.warehouse_id
    FROM lake_jfb.ultra_partner.fulfillment_partner_item_warehouse iw
         JOIN lake_consolidated.ultra_merchant.item AS i
              ON i.meta_original_item_id = iw.item_id
              AND i.partner_item_number IS NOT NULL
         LEFT JOIN lake.centric.ed_sku sku
              ON i.item_number = sku.tfg_code
         LEFT JOIN lake.centric.ed_style eds
              ON sku.the_parent_id = eds.id
         LEFT JOIN lake.centric.ed_colorway edc
              ON eds.id = edc.the_parent_id AND edc.id = sku.realized_color
         LEFT JOIN lake.centric.tfg_marketplace_costs mc
                   ON mc.style = eds.code AND mc.colorway_cnl = edc.the_cnl
    WHERE i.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_item
        OR iw.hvr_change_time > $wm_lake_jfb_ultra_partner_fulfillment_partner_item_warehouse
        OR sku.meta_update_datetime > $wm_lake_centric_ed_sku
        OR eds.meta_update_datetime > $wm_lake_centric_ed_style
        OR edc.meta_update_datetime > $wm_lake_centric_ed_colorway
        OR mc.meta_update_datetime > $wm_lake_centric_tfg_marketplace_costs
    UNION ALL
    /* Previously errored rows */
    SELECT item_id, warehouse_id
    FROM excp.fact_inventory
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.item_id, incr.warehouse_id;
-- SELECT * FROM _fact_inventory__base;
-- SELECT COUNT(1) FROM _fact_inventory__base;

-- landed cost from reference.gfc_inventory_cost
CREATE OR REPLACE TEMP TABLE _fact_inventory__cpu AS
SELECT
       base.warehouse_id,
       COALESCE(w.region_id, 0)        AS region_id,
       UPPER(i.item_number)            AS sku,
       COALESCE(gic.inventory_cost, 0) AS landed_cost_per_unit
FROM _fact_inventory__base AS base
         JOIN lake.ultra_warehouse.item AS i
              ON i.item_id = base.item_id
         LEFT JOIN lake.ultra_warehouse.warehouse AS w
                   ON w.warehouse_id = base.warehouse_id
         LEFT JOIN reference.gfc_inventory_cost AS gic
                   ON gic.warehouse_id = base.warehouse_id
                       AND UPPER(gic.sku) = UPPER(i.item_number)
WHERE NOT gic.is_deleted AND base.warehouse_id != 601 AND NOT i.hvr_is_deleted
--landed cost calculation from centric tables for miracle miles
UNION ALL
SELECT base.warehouse_id,
       COALESCE(w.region_id, 0)    AS region_id,
       UPPER(i.item_number)        AS sku,
       COALESCE(mc.landed_cost, 0) AS landed_cost_per_unit
FROM _fact_inventory__base AS base
         JOIN lake_jfb.ultra_partner.fulfillment_partner_item_warehouse iw
              ON iw.item_id = base.item_id
                  AND iw.warehouse_id = base.warehouse_id
         JOIN lake_consolidated.ultra_merchant.item AS i
              ON i.meta_original_item_id = iw.item_id
         LEFT JOIN lake.ultra_warehouse.warehouse AS w
                   ON w.warehouse_id = base.warehouse_id
         LEFT JOIN lake.centric.ed_sku sku
                   ON i.item_number = sku.tfg_code
         LEFT JOIN lake.centric.ed_style eds
                   ON sku.the_parent_id = eds.id
         LEFT JOIN lake.centric.ed_colorway edc
                   ON eds.id = edc.the_parent_id AND edc.id = sku.realized_color
         LEFT JOIN lake.centric.tfg_marketplace_costs mc
                   ON mc.style = eds.code AND mc.colorway_cnl = edc.the_cnl
WHERE base.warehouse_id = 601
QUALIFY ROW_NUMBER() OVER (PARTITION BY sku.tfg_code ORDER BY sku.the_created_at DESC) = 1;
-- SELECT * FROM _fact_inventory__cpu;
-- SELECT COUNT(1) FROM _fact_inventory__cpu WHERE landed_cost_per_unit = 0;

-- If landed cost per unit = 0, look up alternative cost based on anything from reference.gfc_inventory_cost that is
-- in the same region having the same sku
UPDATE _fact_inventory__cpu AS cpu
SET cpu.landed_cost_per_unit = dc.landed_cost_per_unit
FROM (
    SELECT DISTINCT
        COALESCE(region_id, 0) AS region_id,
        UPPER(sku) AS sku,
        inventory_cost AS landed_cost_per_unit
    FROM reference.gfc_inventory_cost AS gic
    WHERE NOT gic.is_deleted
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(region_id, 0), UPPER(sku) ORDER BY inventory_cost DESC) = 1
    ) AS dc
WHERE cpu.landed_cost_per_unit = 0
    AND cpu.region_id != 9
    AND cpu.region_id = dc.region_id
    AND cpu.sku = dc.sku
    AND cpu.landed_cost_per_unit != dc.landed_cost_per_unit;
-- SELECT * FROM _fact_inventory__cpu;
-- SELECT COUNT(1) FROM _fact_inventory__cpu WHERE landed_cost_per_unit = 0;

-- If landed cost per unit = 0, look up alternative cost based on anything from reference.gfc_inventory_cos that
-- has the same sku and preferably fully landed
UPDATE _fact_inventory__cpu AS cpu
SET cpu.landed_cost_per_unit = dc.landed_cost_per_unit
FROM (
    SELECT DISTINCT
        UPPER(sku) AS sku,
        inventory_cost AS landed_cost_per_unit
    FROM reference.gfc_inventory_cost AS gic
    WHERE NOT gic.is_deleted
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY UPPER(sku) ORDER BY inventory_cost DESC)) = 1
    ) AS dc
WHERE cpu.landed_cost_per_unit = 0
    AND cpu.region_id != 9
    AND cpu.sku = dc.sku
    AND cpu.landed_cost_per_unit != dc.landed_cost_per_unit;
-- SELECT * FROM _fact_inventory__cpu;
-- SELECT COUNT(1) FROM _fact_inventory__cpu WHERE landed_cost_per_unit = 0;

--Pulling in data from reporting_prod.gsc.po_detail_dataset to use in default logic
CREATE OR REPLACE TEMP TABLE _po_detail_dataset  AS
SELECT
    pod.warehouse_id,
    pod.sku,
    DATE_TRUNC(month, COALESCE(r.datetime_received, '1900-01-01')::DATE) AS year_month_received,
    iff(pod.po_status_text != 'Cancelled', 1, 0) AS  is_cancelled,
    pod.show_room,
    IFF(NVL(pod.actual_landed_cost,86) < 85, pod.reporting_landed_cost, pod.landed_cost_estimated) AS landed_cost_per_unit
FROM reference.gsc_po_detail_dataset AS pod
LEFT JOIN lake.ultra_warehouse.receipt AS r
        ON UPPER(r.po_number) = UPPER(pod.po_number)
WHERE nvl(r.label, ' ') NOT ILIKE '%-C' AND NOT pod.is_deleted
QUALIFY ROW_NUMBER() OVER( partition by pod.warehouse_id, pod.sku
                            ORDER BY
                                is_cancelled DESC,
                                year_month_received DESC,
                                show_room DESC,
                                qty DESC,
                                reporting_landed_cost DESC
                         ) = 1;

--Default logic using po_detail_dataset at warehouse_id and sku
UPDATE _fact_inventory__cpu AS cpu
SET cpu.landed_cost_per_unit = dc.landed_cost_per_unit
FROM (
    SELECT
        COALESCE(warehouse_id, 0) AS warehouse_id,
        UPPER(sku) AS sku,
        landed_cost_per_unit AS landed_cost_per_unit
    FROM _po_detail_dataset AS pdd
    ) AS dc
WHERE cpu.landed_cost_per_unit = 0
    AND cpu.region_id != 9
    AND cpu.warehouse_id = dc.warehouse_id
    AND cpu.sku = dc.sku
    AND cpu.landed_cost_per_unit != dc.landed_cost_per_unit;

--Default logic using po_detail_dataset at sku and region_id
UPDATE _fact_inventory__cpu AS cpu
SET cpu.landed_cost_per_unit = dc.landed_cost_per_unit
FROM (
    SELECT DISTINCT
        COALESCE(w.region_id, 0) AS region_id,
        UPPER(sku) AS sku,
        landed_cost_per_unit AS landed_cost_per_unit
    FROM _po_detail_dataset AS pdd
    JOIN lake.ultra_warehouse.warehouse AS w
            ON w.warehouse_id = pdd.warehouse_id
    QUALIFY ROW_NUMBER() over (PARTITION BY COALESCE(w.region_id, 0), UPPER(sku)
                                ORDER BY
                                    is_cancelled DESC,
                                    year_month_received DESC,
                                    show_room DESC,
                                    landed_cost_per_unit DESC
                              ) = 1
    ) AS dc
WHERE cpu.landed_cost_per_unit = 0
    AND cpu.region_id != 9
    AND cpu.region_id = dc.region_id
    AND cpu.sku = dc.sku
    AND cpu.landed_cost_per_unit != dc.landed_cost_per_unit;

--Default logic using po_detail_dataset at sku
UPDATE _fact_inventory__cpu AS cpu
SET cpu.landed_cost_per_unit = dc.landed_cost_per_unit
FROM (
    SELECT DISTINCT
        UPPER(sku) AS sku,
        landed_cost_per_unit AS landed_cost_per_unit
    FROM _po_detail_dataset AS pdd
    QUALIFY ROW_NUMBER() over (PARTITION BY UPPER(sku)
                                ORDER BY
                                    is_cancelled DESC,
                                    year_month_received DESC,
                                    show_room DESC,
                                    landed_cost_per_unit DESC
                              ) = 1
    ) AS dc
WHERE cpu.landed_cost_per_unit = 0
    AND cpu.region_id != 9
    AND cpu.sku = dc.sku
    AND cpu.landed_cost_per_unit != dc.landed_cost_per_unit;

-- soft delete orphan records
UPDATE stg.fact_inventory fi
SET fi.is_deleted = TRUE,
    fi.meta_update_datetime = $execution_start_time
WHERE $is_full_refresh AND NOT is_deleted
AND NOT EXISTS(
    SELECT 1 FROM _fact_inventory__base AS base
    WHERE fi.item_id = base.item_id
    AND fi.warehouse_id = base.warehouse_id
);


CREATE OR REPLACE TEMP TABLE _fact_inventory__stg AS
SELECT
    base.item_id,
    base.warehouse_id,
    cpu.region_id,
    IFF(base.warehouse_id = 601, 'JUSTFAB', UPPER(TRIM(c.label))) AS brand,
    COALESCE(UPPER(i.item_number), UPPER(ium.item_number))        AS sku,
    IFF(base.warehouse_id = 601, fiw.warehouse_available_quantity,
       IFF((NVL(iw.qty_onhand, 0) - NVL(dir.qty_reserved, 0)) <= 0, 0,
           (NVL(iw.qty_onhand, 0) - NVL(dir.qty_reserved, 0))))   AS onhand_quantity,
    NVL(iw.qty_replen, 0)                                         AS replen_quantity,
    NVL(iw.qty_ghost, 0)                                          AS ghost_quantity,
    NVL(iw.qty_order_reserve, 0)                                  AS reserve_quantity,
    NVL(iw.qty_wholesale_reserve, 0)                              AS special_pick_quantity,
    NVL(iw.qty_misc3_reserve, 0)                                  AS manual_stock_reserve_quantity,
    CASE
        WHEN IFF(base.warehouse_id = 601, fiw.warehouse_available_quantity,
             (onhand_quantity + NVL(iw.qty_replen, 0) + NVL(iw.qty_ghost, 0))
             - (NVL(iw.qty_order_reserve, 0) + NVL(iw.qty_wholesale_reserve, 0) + NVL(iw.qty_misc3_reserve, 0))) < 0
            THEN 0
        ELSE IFF(base.warehouse_id = 601, fiw.warehouse_available_quantity,
             (onhand_quantity + NVL(iw.qty_replen, 0) + NVL(iw.qty_ghost, 0))
             - (NVL(iw.qty_order_reserve, 0) + NVL(iw.qty_wholesale_reserve, 0) + NVL(iw.qty_misc3_reserve, 0)))
        END                                                       AS available_to_sell_quantity,
    NVL(iw.qty_ri, 0)                                             AS receipt_inspection_quantity,
    NVL(iw.qty_return, 0)                                         AS return_quantity,
    NVL(iw.qty_mrb, 0)                                            AS damaged_quantity,
    NVL(iw.qty_mrb_returns, 0)                                    AS damaged_returns_quantity,
    NVL(iw.qty_allocated, 0)                                      AS allocated_quantity,
    NVL(iw.qty_intransit, 0)                                      AS intransit_quantity,
    NVL(iw.qty_staging, 0)                                        AS staging_quantity,
    NVL(iw.qty_pick_staging, 0)                                   AS pick_staging_quantity,
    NVL(iw.qty_lost, 0)                                           AS lost_quantity,
    (onhand_quantity + NVL(iw.qty_replen, 0) + NVL(iw.qty_ri, 0)
       + NVL(iw.qty_pick_staging, 0) + NVL(iw.qty_staging, 0))    AS open_to_buy_quantity,
    NVL(cpu.landed_cost_per_unit, 0)                              AS landed_cost_per_unit,
    NVL(dir.qty_reserved, 0)                                      AS dsw_dropship_quantity,
    IFF(i.hvr_is_deleted = 1, TRUE, FALSE)                        AS is_deleted,
    $execution_start_time                                         AS meta_create_datetime,
    $execution_start_time                                         AS meta_update_datetime
FROM _fact_inventory__base AS base
     LEFT JOIN lake.ultra_warehouse.item_warehouse AS iw
        ON iw.item_id = base.item_id
           AND iw.warehouse_id = base.warehouse_id
     LEFT JOIN lake.ultra_warehouse.item AS i
        ON i.item_id = iw.item_id
     LEFT JOIN lake_jfb.ultra_partner.fulfillment_partner_item_warehouse fiw
        ON base.item_id = fiw.item_id
           AND base.warehouse_id = fiw.warehouse_id
     LEFT JOIN lake_consolidated.ultra_merchant.item AS ium
        ON ium.meta_original_item_id = base.item_id
           AND base.warehouse_id = 601
     LEFT JOIN lake.ultra_warehouse.company AS c
        ON c.company_id = i.company_id
     LEFT JOIN _fact_inventory__cpu AS cpu
        ON cpu.warehouse_id = base.warehouse_id
           AND UPPER(cpu.sku) = COALESCE(UPPER(i.item_number), UPPER(ium.item_number))
     LEFT JOIN reporting_prod.gfc.dropship_inventory_reserve AS dir
        ON UPPER(i.item_number) = UPPER(dir.item_number)
           AND UPPER(dir.dropshipretailercode) = 'DSW'
           AND base.warehouse_id = 107
ORDER BY base.item_id,
         base.warehouse_id;
-- SELECT * FROM stg.fact_inventory_stg;



INSERT INTO stg.fact_inventory_stg (
    item_id,
	warehouse_id,
    region_id,
    brand,
    sku,
    onhand_quantity,
    replen_quantity,
    ghost_quantity,
    reserve_quantity,
    special_pick_quantity,
    manual_stock_reserve_quantity,
    available_to_sell_quantity,
    receipt_inspection_quantity,
    return_quantity,
    damaged_quantity,
    damaged_returns_quantity,
    allocated_quantity,
    intransit_quantity,
    staging_quantity,
    pick_staging_quantity,
    lost_quantity,
    open_to_buy_quantity,
    landed_cost_per_unit,
    dsw_dropship_quantity,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    item_id,
	warehouse_id,
    region_id,
    brand,
    sku,
    onhand_quantity,
    replen_quantity,
    ghost_quantity,
    reserve_quantity,
    special_pick_quantity,
    manual_stock_reserve_quantity,
    available_to_sell_quantity,
    receipt_inspection_quantity,
    return_quantity,
    damaged_quantity,
    damaged_returns_quantity,
    allocated_quantity,
    intransit_quantity,
    staging_quantity,
    pick_staging_quantity,
    lost_quantity,
    open_to_buy_quantity,
    landed_cost_per_unit,
    dsw_dropship_quantity,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_inventory__stg
ORDER BY
    item_id,
    warehouse_id;
