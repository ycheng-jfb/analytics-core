SET target_table = 'stg.fact_inventory_history';
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
SET wm_lake_ultra_warehouse_inventory_rollup = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.inventory_rollup'));
SET wm_edw_stg_fact_inventory = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_inventory'));
SET wm_edw_prod_reference_dropship_inventory_log = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.dropship_inventory_log'));
/*
SELECT
    $wm_lake_ultra_warehouse_inventory_rollup,
    $wm_edw_stg_fact_inventory,
    $wm_edw_prod_reference_dropship_inventory_log;
*/

-- Make sure we do not refresh prior to the min historical date (2018-01-01)
SET min_refresh_date = '2018-01-01'::TIMESTAMP_LTZ(3);
SET wm_lake_ultra_warehouse_inventory_rollup = IFF($is_full_refresh, $min_refresh_date, GREATEST($wm_lake_ultra_warehouse_inventory_rollup, $min_refresh_date));
SET is_full_refresh = IFF($wm_lake_ultra_warehouse_inventory_rollup = $min_refresh_date, TRUE, $is_full_refresh);

CREATE OR REPLACE TEMP TABLE _fact_inventory_date__base (
    item_id INT,
    warehouse_id INT,
    local_date date,
    date date
    );

INSERT INTO _fact_inventory_date__base (item_id, warehouse_id, local_date, date)
SELECT DISTINCT
            iw.item_id,
            iw.warehouse_id,
            iw.local_date::DATE AS local_date,
            iw.date::DATE AS date
FROM lake.ultra_warehouse.inventory_rollup AS iw
     JOIN lake.ultra_warehouse.item AS i
          ON i.item_id = iw.item_id
WHERE UPPER(i.item_number) NOT LIKE '*DUP%%'
AND iw.meta_update_datetime > $wm_lake_ultra_warehouse_inventory_rollup
UNION ALL
SELECT DISTINCT
            iw.item_id,
            iw.warehouse_id,
            iw.date::DATE AS local_date,
            iw.date::DATE AS date
FROM reference.dropship_inventory_log AS iw
WHERE iw.meta_update_datetime > $wm_edw_prod_reference_dropship_inventory_log
;

-- minimum hour date extraction at local_date
CREATE OR REPLACE TEMP TABLE _fact_inventory_local_date_history__base (
    item_id INT,
    warehouse_id INT,
    date_hour TIMESTAMP_NTZ,
    local_date DATE,
    date TIMESTAMP_NTZ,
    src VARCHAR
    );

INSERT INTO _fact_inventory_local_date_history__base (item_id, warehouse_id, local_date, date_hour)
SELECT iw.item_id,
       iw.warehouse_id,
       iw.local_date::date AS local_date,
       MIN(iw.local_date) AS date_hour
FROM _fact_inventory_date__base AS base
     JOIN lake.ultra_warehouse.inventory_rollup AS iw
          ON base.item_id = iw.item_id
          AND base.warehouse_id = iw.warehouse_id
          AND base.local_date = iw.local_date::date
WHERE iw.local_date IS NOT NULL
GROUP BY iw.item_id,
         iw.warehouse_id,
         iw.local_date::date;

CREATE OR REPLACE TEMP TABLE _fact_inventory_local_date_history__base_stg AS
SELECT iw.item_id,
       iw.warehouse_id,
       iw.local_date,
       iw.date
 from _fact_inventory_local_date_history__base  base
        JOIN lake.ultra_warehouse.inventory_rollup AS iw
          ON base.item_id = iw.item_id
          AND base.warehouse_id = iw.warehouse_id
          AND iw.local_date = base.date_hour
QUALIFY ROW_NUMBER() OVER (PARTITION BY iw.item_id,iw.warehouse_id, base.date_hour ORDER BY iw.date) = 1;

UPDATE _fact_inventory_local_date_history__base base
SET base.date = iw.date, src = 'local_date'
FROM _fact_inventory_local_date_history__base_stg iw
WHERE iw.local_date IS NOT NULL
    AND base.item_id = iw.item_id
    AND base.warehouse_id = iw.warehouse_id
    AND iw.local_date = base.date_hour;

-- minimum hour date extraction at date when local_date is null
CREATE OR REPLACE TEMP TABLE _fact_inventory_date_history__base (
    item_id INT,
    warehouse_id INT,
    date_hour TIMESTAMP_NTZ,
    local_date DATE,
    date TIMESTAMP_NTZ,
    src VARCHAR
    );


INSERT INTO _fact_inventory_date_history__base (item_id, warehouse_id, local_date, date_hour)
SELECT iw.item_id,
       iw.warehouse_id,
       iw.date::date AS local_date,
       MIN(iw.date) AS date_hour
FROM _fact_inventory_date__base AS base
     JOIN lake.ultra_warehouse.inventory_rollup AS iw
          ON base.item_id = iw.item_id
          AND base.warehouse_id = iw.warehouse_id
          AND base.date = iw.date::date
WHERE iw.local_date IS NULL
GROUP BY iw.item_id,
         iw.warehouse_id,
         iw.date::date;

UPDATE _fact_inventory_date_history__base base
SET base.date = iw.date, src = 'date'
FROM lake.ultra_warehouse.inventory_rollup iw
WHERE iw.local_date IS NULL
    AND base.item_id = iw.item_id
    AND base.warehouse_id = iw.warehouse_id
    AND iw.date = base.date_hour;

--Removes the overlapped records between local_date and date
CREATE OR REPLACE TEMP TABLE _fact_inventory_history__base (
    item_id INT,
    warehouse_id INT,
    local_date DATE,
    date TIMESTAMP_NTZ,
    rollup_datetime_added TIMESTAMP_NTZ,
    src VARCHAR
    );

INSERT INTO _fact_inventory_history__base (item_id, warehouse_id, local_date, date, src)
SELECT item_id, warehouse_id, local_date, date, src
FROM(
    SELECT item_id, warehouse_id, local_date, date, src
    FROM _fact_inventory_local_date_history__base
    UNION ALL
    SELECT item_id, warehouse_id, local_date, date, src
    FROM _fact_inventory_date_history__base
    UNION ALL
    SELECT item_id, warehouse_id, local_date, date, 'Miracle Mile'
    FROM _fact_inventory_date__base WHERE warehouse_id = 601
)
QUALIFY ROW_NUMBER() OVER(PARTITION BY item_id, warehouse_id, local_date ORDER BY date ASC) = 1;

UPDATE _fact_inventory_history__base base
SET base.rollup_datetime_added = iw.datetime_added
FROM lake.ultra_warehouse.inventory_rollup iw
WHERE iw.item_id = base.item_id
    AND iw.warehouse_id = base.warehouse_id
    AND iw.date = base.date;

-- Save historical values from underlying table for reprocessed dates
CREATE OR REPLACE TEMP TABLE _fact_inventory_history__hist AS
SELECT
    src.local_date,
    src.item_id,
    src.warehouse_id,
    src.landed_cost_per_unit
FROM _fact_inventory_history__base AS base
    JOIN stg.fact_inventory_history AS src
        ON src.item_id = base.item_id
        AND src.warehouse_id = base.warehouse_id
        AND src.rollup_datetime_added = base.rollup_datetime_added
ORDER BY
    local_date DESC,
    item_id,
    warehouse_id;
-- SELECT * FROM _fact_inventory_history__hist;

CREATE OR REPLACE TEMP TABLE _fact_inventory_history__region AS
SELECT
    base.item_id,
    base.warehouse_id,
    base.date,
    COALESCE(w.region_id, 0) AS region_id
FROM _fact_inventory_history__base AS base
    LEFT JOIN lake.ultra_warehouse.warehouse AS w
        ON w.warehouse_id = base.warehouse_id;

CREATE OR REPLACE TEMP TABLE _fact_inventory_history__dsw_dropship AS
SELECT
    UPPER(i.item_number) AS item_number,
    base.warehouse_id,
    base.date,
    ifd.total_qty_reserve AS dsw_dropship_quantity
FROM _fact_inventory_history__base AS base
JOIN lake.ultra_warehouse.item AS i
    ON i.item_id = base.item_id
JOIN (
    /* Subquery for creating History Effective timestamps */
    SELECT item_id,
           inventory_fence_id,
           dropship_retailer_id,
           total_qty_reserve,
           hvr_change_time                                AS effective_start_datetime,
           IFNULL(LEAD(hvr_change_time) OVER (PARTITION BY inventory_fence_detail_id ORDER BY hvr_change_time),
                  '9999-12-31 00:00:00.000000000 -08:00') AS effective_end_datetime
    FROM lake_history.ultra_warehouse.inventory_fence_detail) AS ifd
     ON ifd.item_id = i.item_id
    AND base.date BETWEEN ifd.effective_start_datetime::DATE AND ifd.effective_end_datetime::DATE
JOIN lake_view.ultra_warehouse.dropship_retailer dr
    on dr.dropship_retailer_id = ifd.dropship_retailer_id
WHERE base.warehouse_id = 107
    AND ifd.inventory_fence_id = 469
    AND dr.dropship_retailer_id = 1 -- for DSW
    AND IFNULL(ifd.total_qty_reserve, 0) > 0
QUALIFY (ROW_NUMBER() OVER (PARTITION BY UPPER(i.item_number) ,base.warehouse_id,base.date
        ORDER BY ifd.effective_start_datetime DESC)) = 1;

-- soft delete orphan records
UPDATE stg.fact_inventory_history  hist
SET hist.is_deleted = TRUE,
    hist.meta_update_datetime = $execution_start_time
WHERE $is_full_refresh AND NOT is_deleted
AND NOT EXISTS(
    SELECT 1 FROM _fact_inventory_history__base AS base
    WHERE hist.item_id = base.item_id
    AND hist.warehouse_id = base.warehouse_id
    AND hist.rollup_datetime_added = base.rollup_datetime_added
);

CREATE OR REPLACE TEMP TABLE _fact_inventory_history_stg  AS
SELECT base.item_id,
       base.warehouse_id,
       iw.datetime_added                                               AS rollup_datetime_added,
       base.local_date,
       r.region_id                                                     AS region_id,
       UPPER(TRIM(c.label))                                            AS brand,
       UPPER(i.item_number)                                            AS sku,
       IFF((NVL(iw.qty_onhand, 0) - NVL(dd.dsw_dropship_quantity, 0)) <= 0, 0,
           (NVL(iw.qty_onhand, 0) - NVL(dd.dsw_dropship_quantity, 0))) AS onhand_quantity_calc,
       NVL(iw.qty_replen, 0)                                           AS replen_quantity,
       NVL(iw.qty_ghost, 0)                                            AS ghost_quantity,
       NVL(iw.qty_order_reserve, 0)                                    AS reserve_quantity,
       NVL(iw.qty_wholesale_reserve, 0)                                AS special_pick_quantity,
       NVL(iw.qty_misc3_reserve, 0)                                    AS manual_stock_reserve_quantity,
       CASE
           WHEN iw.datetime_added >= '2020-05-01' THEN
               CASE
                   WHEN (NVL(onhand_quantity_calc, 0) + NVL(iw.qty_replen, 0) + NVL(iw.qty_ghost, 0))
                            - (NVL(iw.qty_order_reserve, 0) + NVL(iw.qty_wholesale_reserve, 0)
                           + NVL(iw.qty_misc3_reserve, 0)) <= 0 THEN 0
                   ELSE (NVL(onhand_quantity_calc, 0) + NVL(iw.qty_replen, 0) + NVL(iw.qty_ghost, 0))
                       - (NVL(iw.qty_order_reserve, 0) + NVL(iw.qty_wholesale_reserve, 0)
                           + NVL(iw.qty_misc3_reserve, 0))
                   END
           ELSE
               CASE
                   WHEN (NVL(onhand_quantity_calc, 0) - NVL(iw.qty_misc3_reserve, 0)) <= 0 THEN 0
                   ELSE (NVL(onhand_quantity_calc, 0) - NVL(iw.qty_misc3_reserve, 0))
                   END
           END                                                         AS available_to_sell_quantity,
       NVL(iw.qty_ri, 0)                                               AS receipt_inspection_quantity,
       NVL(iw.qty_return, 0)                                           AS return_quantity,
       NVL(iw.qty_mrb, 0)                                              AS damaged_quantity,
       NVL(iw.qty_mrb_returns, 0)                                      AS damaged_returns_quantity,
       NVL(iw.qty_allocated, 0)                                        AS allocated_quantity,
       NVL(iw.qty_intransit, 0)                                        AS intransit_quantity,
       NVL(iw.qty_staging, 0)                                          AS staging_quantity,
       NVL(iw.qty_pick_staging, 0)                                     AS pick_staging_quantity,
       NVL(iw.qty_lost, 0)                                             AS lost_quantity,
       CASE
           WHEN iw.datetime_added >= '2020-05-01' THEN
               CASE
                   WHEN (NVL(onhand_quantity_calc, 0) + NVL(iw.qty_replen, 0) + NVL(iw.qty_ri, 0)
                       + NVL(iw.qty_pick_staging, 0) + NVL(iw.qty_staging, 0)) <= 0 THEN 0
                   ELSE (NVL(onhand_quantity_calc, 0) + NVL(iw.qty_replen, 0) + NVL(iw.qty_ri, 0)
                       + NVL(iw.qty_pick_staging, 0) + NVL(iw.qty_staging, 0))
                   END
           ELSE
               CASE
                   WHEN NVL(onhand_quantity_calc, 0) <= 0 THEN 0
                   ELSE NVL(onhand_quantity_calc, 0)
                   END
           END                                                         AS open_to_buy_quantity,
       COALESCE(hist.landed_cost_per_unit, fi.landed_cost_per_unit, 0) AS landed_cost_per_unit,
       NVL(dd.dsw_dropship_quantity, 0)                                AS dsw_dropship_quantity,
       IFF(i.hvr_is_deleted = 1, TRUE, FALSE)                          AS is_deleted,
       $execution_start_time                                           AS meta_create_datetime,
       $execution_start_time                                           AS meta_update_datetime
FROM _fact_inventory_history__base AS base
     JOIN lake.ultra_warehouse.inventory_rollup AS iw
          ON iw.item_id = base.item_id
              AND iw.warehouse_id = base.warehouse_id
              AND iw.date = base.date
     JOIN lake.ultra_warehouse.item AS i
          ON i.item_id = base.item_id
     LEFT JOIN lake.ultra_warehouse.company AS c
               ON c.company_id = i.company_id
     LEFT JOIN stg.fact_inventory AS fi /* Use to get current values for normal delta processing */
               ON fi.item_id = base.item_id
                   AND fi.warehouse_id = base.warehouse_id
     LEFT JOIN _fact_inventory_history__hist AS hist /* Use to preserve historic values when reloading data */
               ON hist.item_id = base.item_id
                   AND hist.warehouse_id = base.warehouse_id
                   AND hist.local_date = base.local_date
     LEFT JOIN _fact_inventory_history__dsw_dropship dd
               ON UPPER(dd.item_number) = UPPER(i.item_number)
                   AND dd.warehouse_id = base.warehouse_id
                   AND dd.date = base.date
     LEFT JOIN _fact_inventory_history__region r
               ON r.item_id = base.item_id
                   AND r.warehouse_id = base.warehouse_id
                   AND r.date = base.date
ORDER BY base.local_date DESC,
         base.item_id,
         base.warehouse_id;


----Miracle Mile insertion
INSERT INTO _fact_inventory_history_stg(
    item_id,
    warehouse_id,
    rollup_datetime_added,
    local_date,
    region_id,
    brand,
    sku,
    onhand_quantity_calc,
    available_to_sell_quantity,
    landed_cost_per_unit,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT base.item_id,
       base.warehouse_id,
       iw.date                      AS rollup_datetime_added,
       base.local_date,
       r.region_id                  AS region_id,
       'JUSTFAB'                    AS brand,
       UPPER(i.item_number)         AS sku,
       warehouse_available_quantity AS onhand_quantity_calc,
       warehouse_available_quantity AS available_to_sell_quantity,
       COALESCE(hist.landed_cost_per_unit, fi.landed_cost_per_unit, 0) AS landed_cost_per_unit,
       IFF(i.hvr_is_deleted = 1, TRUE, FALSE) AS is_deleted,
       $execution_start_time        AS meta_create_datetime,
       $execution_start_time        AS meta_update_datetime
FROM _fact_inventory_history__base AS base
     JOIN reference.dropship_inventory_log AS iw
          ON iw.item_id = base.item_id
              AND iw.warehouse_id = base.warehouse_id
              AND iw.date = base.date
     JOIN lake_consolidated.ultra_merchant.item AS i
          ON i.meta_original_item_id = base.item_id
     LEFT JOIN _fact_inventory_history__region r
               ON r.item_id = base.item_id
                   AND r.warehouse_id = base.warehouse_id
                   AND r.date = base.date
    LEFT JOIN stg.fact_inventory AS fi /* Use to get current values for normal delta processing */
        ON fi.item_id = base.item_id
        AND fi.warehouse_id = base.warehouse_id
    LEFT JOIN _fact_inventory_history__hist AS hist /* Use to preserve historic values when reloading data */
        ON hist.item_id = base.item_id
        AND hist.warehouse_id = base.warehouse_id
        AND hist.local_date = base.local_date
ORDER BY base.local_date DESC,
         base.item_id,
         base.warehouse_id;

INSERT INTO stg.fact_inventory_history_stg (
    item_id,
    warehouse_id,
    rollup_datetime_added,
    local_date,
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
    rollup_datetime_added,
    local_date,
    region_id,
    brand,
    sku,
    onhand_quantity_calc,
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
FROM _fact_inventory_history_stg
ORDER BY local_date DESC,
         item_id,
         warehouse_id;
-- SELECT * FROM stg.fact_inventory_history_stg;

/*
--DA-25093 - lATEST BACKFILL LOGIC

CREATE OR REPLACE TABLE work.gsc.master_landed_cost as
SELECT
    warehouse_id,
    sku,
    landed_cost_per_unit,
    ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku ORDER BY local_date desc) AS ws_rno,
    row_number() OVER (PARTITION BY sku ORDER BY local_date desc) AS sku_rno
FROM data_model.fact_inventory_history
WHERE local_date >= '2021-12-01'
    and open_to_buy_quantity + intransit_quantity > 5
    and landed_cost_per_unit > 0;

CREATE OR REPLACE TABLE work.gsc.bad_landed_cost as
SELECT DISTINCT
    h.local_date,
    h.warehouse_id,
    h.sku,
    cast(0.00 as number(38,6)) AS landed_cost_per_unit
FROM work.gsc.master_landed_cost m
LEFT JOIN data_model.fact_inventory_history h
ON m.sku = h.sku
WHERE h.local_date >= '2021-12-01'
AND h.open_to_buy_quantity + h.intransit_quantity > 5
AND h.landed_cost_per_unit = 0;

UPDATE work.gsc.bad_landed_cost a
SET a.landed_cost_per_unit = b.landed_cost_per_unit
FROM work.gsc.master_landed_cost b
    where a.sku = b.sku
    and a.warehouse_id = b.warehouse_id
    and b.ws_rno = 1;

UPDATE work.gsc.bad_landed_cost a
SET a.landed_cost_per_unit = b.landed_cost_per_unit
FROM work.gsc.master_landed_cost b
where  a.sku = b.sku
    and b.sku_rno = 1
    AND a.landed_cost_per_unit = 0;

-- update fact_inventory_history using work.gsc.bad_landed_cost

UPDATE stg.fact_inventory_history t
SET t.landed_cost_per_unit = a.landed_cost_per_unit
FROM work.gsc.bad_landed_cost a
WHERE t.sku = a.sku
and t.warehouse_id = a.warehouse_id
and t. local_date = a.local_date;

*/

/*
-- Mike Backfill Logic
UPDATE stg.fact_inventory_history AS h
SET h.landed_cost_per_unit = i.landed_cost_per_unit
FROM stg.fact_inventory AS i
WHERE h.item_id = i.item_id
    AND h.warehouse_id = i.warehouse_id
    AND h.sku = i.sku
    AND h.landed_cost_per_unit = 0
    AND h.onhand_quantity + h.replen_quantity + h.ghost_quantity + h.reserve_quantity + h.special_pick_quantity +
        h.manual_stock_reserve_quantity + h.available_to_sell_quantity + h.receipt_inspection_quantity + h.return_quantity +
        h.damaged_quantity + h.damaged_returns_quantity + h.allocated_quantity + h.intransit_quantity + h.staging_quantity +
        h.pick_staging_quantity + h.lost_quantity + h.open_to_buy_quantity > 0
    AND h.landed_cost_per_unit != i.landed_cost_per_unit;
*/
