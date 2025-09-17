SET target_table = 'stg.fact_ebs_bulk_shipment';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table,NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET wm_lake_ultra_warehouse_inventory_log = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.inventory_log'));
SET wm_lake_ultra_warehouse_inventory_log_container_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.inventory_log_container_item'));
SET wm_lake_ultra_warehouse_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.item'));
SET wm_lake_ultra_warehouse_lpn = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.lpn'));
SET wm_lake_ultra_warehouse_case = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.case'));
SET wm_lake_ultra_warehouse_location = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.location'));
SET wm_lake_ultra_warehouse_warehouse = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.warehouse'));
SET wm_lake_ultra_warehouse_zone = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.zone'));
SET wm_lake_ultra_warehouse_bulk_shipment_container_detail = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.bulk_shipment_container_detail'));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

CREATE OR REPLACE TEMP TABLE _fact_ebs_bulk_shipment__inventory_log_base (inventory_log_id INT);

INSERT INTO _fact_ebs_bulk_shipment__inventory_log_base (inventory_log_id)
-- Full Refresh
SELECT DISTINCT il.inventory_log_id
FROM lake.ultra_warehouse.inventory_log AS il
WHERE il.type_code_id = 206
    AND il.object ILIKE 'bulk_shipment%%'
    AND $is_full_refresh
UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.inventory_log_id
FROM (
    SELECT il.inventory_log_id
    FROM lake.ultra_warehouse.inventory_log AS il
        LEFT JOIN lake.ultra_warehouse.inventory_log_container_item AS ci
            ON ci.inventory_log_id = il.inventory_log_id
        LEFT JOIN lake.ultra_warehouse.item AS i
            ON i.item_id = ci.item_id
        LEFT JOIN lake.ultra_warehouse.lpn AS lpn
            ON lpn.lpn_id = ci.lpn_id
        LEFT JOIN lake.ultra_warehouse.case AS cs
		    ON cs.case_id = il.case_id
    WHERE (
        il.hvr_change_time > $wm_lake_ultra_warehouse_inventory_log
        OR ci.hvr_change_time > $wm_lake_ultra_warehouse_inventory_log_container_item
        OR i.hvr_change_time > $wm_lake_ultra_warehouse_item
        OR lpn.hvr_change_time > $wm_lake_ultra_warehouse_lpn
        OR cs.hvr_change_time > $wm_lake_ultra_warehouse_case
        )
        AND il.type_code_id = 206
        AND il.object ILIKE 'bulk_shipment%%'
    UNION ALL
    SELECT il.inventory_log_id
    FROM lake.ultra_warehouse.inventory_log AS il
        LEFT JOIN lake.ultra_warehouse.item AS i
            ON i.item_id = il.item_id
        LEFT JOIN lake.ultra_warehouse.lpn AS lpn
            ON lpn.lpn_id = il.lpn_id
        LEFT JOIN lake.ultra_warehouse.location AS l
            ON l.location_id = il.location_id
        LEFT JOIN lake.ultra_warehouse.warehouse AS w
            ON w.warehouse_id = l.warehouse_id
        LEFT JOIN lake.ultra_warehouse.zone AS z
            ON z.zone_id = l.zone_id
        LEFT JOIN lake.ultra_warehouse.bulk_shipment_container_detail AS bscd
            ON bscd.inventory_log_id = il.inventory_log_id
    WHERE (
        i.hvr_change_time > $wm_lake_ultra_warehouse_item
        OR lpn.hvr_change_time > $wm_lake_ultra_warehouse_lpn
        OR l.hvr_change_time > $wm_lake_ultra_warehouse_location
        OR w.hvr_change_time > $wm_lake_ultra_warehouse_warehouse
        OR z.hvr_change_time > $wm_lake_ultra_warehouse_zone
        OR bscd.hvr_change_time > $wm_lake_ultra_warehouse_bulk_shipment_container_detail
        )
        AND il.type_code_id = 206
        AND il.object ILIKE 'bulk_shipment%%'
    UNION ALL
    SELECT transaction_id AS inventory_log_id
    FROM excp.fact_ebs_bulk_shipment
    WHERE meta_is_current_excp AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh;

-- Get the inventory log container item records
CREATE OR REPLACE TEMP TABLE _fact_ebs_bulk_shipment__case_detail AS
SELECT
    il.inventory_log_id,
    i.item_number,
    c.company_code,
    SUM(ci.quantity) AS quantity
FROM _fact_ebs_bulk_shipment__inventory_log_base AS base
    JOIN lake.ultra_warehouse.inventory_log AS il
        ON il.inventory_log_id = base.inventory_log_id
    LEFT JOIN lake.ultra_warehouse.inventory_log_container_item AS ci
        ON ci.inventory_log_id = base.inventory_log_id
	LEFT JOIN lake.ultra_warehouse.item AS i
		ON i.item_id = ci.item_id
	LEFT JOIN lake.ultra_warehouse.lpn AS lpn
		ON lpn.lpn_id = ci.lpn_id
	LEFT JOIN lake.ultra_warehouse.case AS cs
		ON cs.case_id = il.case_id
	LEFT JOIN lake.ultra_warehouse.receipt AS r
		ON r.receipt_id = COALESCE(lpn.receipt_id, cs.receipt_id)
	LEFT JOIN lake.ultra_warehouse.company AS c
		ON c.company_id = COALESCE(r.company_id, i.company_id)
WHERE il.case_id IS NOT NULL
    AND il.lpn_id IS NULL
GROUP BY
	il.inventory_log_id,
	i.item_number,
	c.company_code;
-- SELECT * FROM _fact_ebs_bulk_shipment__case_detail;

CREATE OR REPLACE TEMP TABLE _fact_ebs_bulk_shipment__location AS
SELECT DISTINCT
    l.location_id,
    z.type_code_id,
    CASE
--      WHEN z.type_code_id IN (202) THEN 'RETURN'
--      WHEN z.type_code_id IN (333,449,1158) THEN 'QA'
        WHEN z.type_code_id IN (201, 202, 319, 333, 361, 241, 273, 332, 359, 360, 449,
                                602, 703, 704, 766, 444, 445, 450, 601, 643, 1045, 209,
                                249, 335, 400, 480, 520, 587, 635, 636, 637, 638, 690,
                                691, 693, 694, 695, 696, 1100, 1158, 1196, 1197, 1243,
                                1253, 1306, 1308, 1309) THEN 'FG'
        WHEN z.type_code_id IN (240) AND l.data_2 = '02' THEN 'MRB_RECEIVING'
        WHEN z.type_code_id IN (240) AND l.data_2 IN ('03', '04') THEN 'MRB_WAREHOUSE'
        WHEN z.type_code_id IN (564) THEN 'MRB_RETURN'
--		WHEN z.type_code_id IN (319) THEN 'LOST'
        WHEN z.type_code_id IN (544) THEN 'INTRANSIT'
        ELSE NULL END AS sub_inventory_bucket
FROM _fact_ebs_bulk_shipment__inventory_log_base AS base
    JOIN lake.ultra_warehouse.inventory_log AS il
        ON il.inventory_log_id = base.inventory_log_id
    JOIN lake.ultra_warehouse.location AS l
        ON l.location_id = il.location_id
    JOIN lake.ultra_warehouse.zone AS z
        ON z.zone_id = l.zone_id;
-- SELECT * FROM _fact_ebs_bulk_shipment__location;

CREATE OR REPLACE TEMP TABLE _fact_ebs_bulk_shipment__stg AS
-- Put cases along with inventory log container item records into the temp table
SELECT
	CASE WHEN w.warehouse_id IN (107, 154, 421) THEN 'USKY001' ELSE w.code END AS facility,
	COALESCE(cd.company_code, '') AS business_unit,
	CONCAT('9999', IFF(il.object = 'bulk_shipment_container_detail', bscd.bulk_shipment_container_id, il.object_id)) AS sales_order_id,
	IFF(il.object = 'bulk_shipment_container_detail', il.object_id, 0) AS line_number,
    NULL::INT AS product_id,
	il.lpn_id,
	il.item_id,
	cd.item_number,
	sub.sub_inventory_bucket AS from_sub_inventory,
	COALESCE(cd.quantity, il.quantity) AS shipped_quantity,
	il.datetime_transaction AS system_datetime,
	il.datetime_local_transaction AS shipment_local_datetime,
	il.inventory_log_id AS transaction_id
FROM _fact_ebs_bulk_shipment__inventory_log_base AS base
    JOIN lake.ultra_warehouse.inventory_log AS il
        ON il.inventory_log_id = base.inventory_log_id
    JOIN _fact_ebs_bulk_shipment__case_detail AS cd
        ON cd.inventory_log_id = il.inventory_log_id
    LEFT JOIN lake.ultra_warehouse.location AS l
	    ON l.location_id = il.location_id
	LEFT JOIN lake.ultra_warehouse.warehouse AS w
	    ON w.warehouse_id = l.warehouse_id
	LEFT JOIN _fact_ebs_bulk_shipment__location AS sub
	    ON sub.location_id = il.location_id
	JOIN lake.ultra_warehouse.bulk_shipment_container_detail AS bscd /* Use JOIN to exclude dups deleted from bulk_shipment_container_detail */
	    ON bscd.inventory_log_id = il.inventory_log_id
WHERE il.case_id IS NOT NULL
    AND il.lpn_id IS NULL
UNION ALL
-- Put LPNs added to bulk shipment without a case into the temp table
SELECT
	CASE WHEN w.warehouse_id IN (107, 154, 421) THEN 'USKY001' ELSE w.code END AS facility,
	COALESCE(c.company_code, '') AS business_unit,
	CONCAT('9999', IFF(il.object = 'bulk_shipment_container_detail', bscd.bulk_shipment_container_id, il.object_id)) AS sales_order_id,
	IFF(il.object = 'bulk_shipment_container_detail', il.object_id, 0) AS line_number,
	NULL::INT AS product_id,
	il.lpn_id,
	il.item_id,
	i.item_number,
	sub.sub_inventory_bucket AS from_sub_inventory,
	il.quantity AS shipped_quantity,
	il.datetime_transaction AS system_datetime,
	il.datetime_local_transaction AS shipment_local_datetime,
	il.inventory_log_id AS transaction_id
FROM _fact_ebs_bulk_shipment__inventory_log_base AS base
    JOIN lake.ultra_warehouse.inventory_log AS il
        ON il.inventory_log_id = base.inventory_log_id
    LEFT JOIN lake.ultra_warehouse.location AS l
	    ON l.location_id = il.location_id
	LEFT JOIN lake.ultra_warehouse.warehouse AS w
	    ON w.warehouse_id = l.warehouse_id
	LEFT JOIN _fact_ebs_bulk_shipment__location AS sub
	    ON sub.location_id = il.location_id
	JOIN lake.ultra_warehouse.bulk_shipment_container_detail AS bscd /* Use JOIN to exclude dups deleted from bulk_shipment_container_detail */
	    ON bscd.inventory_log_id = il.inventory_log_id
    JOIN lake.ultra_warehouse.item AS i
	    ON i.item_id = il.item_id
	JOIN lake.ultra_warehouse.lpn AS lpn
	    ON lpn.lpn_id = il.lpn_id
    LEFT JOIN lake.ultra_warehouse.receipt AS r
	    ON r.receipt_id = lpn.receipt_id
        AND lpn.receipt_id IS NOT NULL
    JOIN lake.ultra_warehouse.company AS c
	    ON c.company_id = COALESCE(r.company_id, i.company_id)
WHERE il.lpn_id IS NOT NULL;
-- SELECT * FROM _fact_ebs_bulk_shipment__stg;

-- Unless the item_number is known, we cannot tie the record to a product
DELETE FROM _fact_ebs_bulk_shipment__stg
WHERE item_number IS NULL;

UPDATE _fact_ebs_bulk_shipment__stg AS t
SET t.product_id = s.product_id
FROM (
    SELECT
        stg.transaction_id,
        stg.item_number,
        stg.business_unit,
        stg.item_id AS uw_item_id,
        i.item_id AS um_item_id,
        p.product_id,
        ROW_NUMBER() OVER (PARTITION BY i.item_number ORDER BY p.product_id DESC) AS row_num
    FROM _fact_ebs_bulk_shipment__stg AS stg
        JOIN lake_consolidated.ultra_merchant.item AS i
            ON i.item_number = stg.item_number
        JOIN lake_consolidated.ultra_merchant.product AS p
            ON p.item_id = i.item_id
    QUALIFY row_num = 1
    ) AS s
WHERE t.transaction_id = s.transaction_id
    AND t.item_number = s.item_number
    AND t.business_unit = s.business_unit;
-- SELECT * FROM _fact_ebs_bulk_shipment__stg WHERE transaction_id = 901499559;

INSERT INTO stg.fact_ebs_bulk_shipment_stg (
	transaction_id,
    item_number,
    business_unit,
    item_id,
    product_id,
    facility,
    sales_order_id,
    line_number,
    from_sub_inventory,
    shipped_quantity,
    system_datetime,
    shipment_local_datetime,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    transaction_id,
    item_number,
    business_unit,
    item_id,
    product_id,
    facility,
    sales_order_id,
    line_number,
    from_sub_inventory,
    shipped_quantity,
    system_datetime,
    shipment_local_datetime,
    $execution_start_time AS meta_create_datetime,
	$execution_start_time AS meta_update_datetime
FROM _fact_ebs_bulk_shipment__stg;
