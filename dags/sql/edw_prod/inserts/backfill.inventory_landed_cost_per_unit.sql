/*
This script does not populate a table, but should used to backfill the landing_cost_per_unit field
in stg.fact_inventory_history any time the table gets truncated, (i.e. an initial load).  History for
this field is based on capturing each end of day value from stg.fact_inventory, which is only possible
going forward. This script is not needed for full refresh, since the existing data will be preserved.
*/

-- STEP 1. Grab all current inventory by FC/SKU/PO/year-month received
CREATE OR REPLACE TEMP TABLE _hist_sku_date AS
SELECT
    warehouse_id,
    sku,
    local_date
FROM data_model.fact_inventory_history
WHERE TRUE
    --AND sku = 'LG1933447-0001-38040' /* This is example criteria that demonstrates how to backfill. */
    --AND landed_cost_per_unit = 0
    AND local_date < '2022-05-15';
-- SELECT COUNT(1) FROM _hist_sku_date;

-- STEP 2.1. Grab "most recent" PO the SKU would have come from based on the inventory / showroom date
CREATE OR REPLACE TEMP TABLE _fc_sku_po_date_1 AS
SELECT DISTINCT
    hsd.warehouse_id,
    hsd.sku,
    hsd.local_date,
    FIRST_VALUE(h.po_number) OVER (PARTITION BY hsd.warehouse_id, d.sku, hsd.local_date ORDER BY h.show_room || '-01' DESC) AS po_number
FROM _hist_sku_date AS hsd
    JOIN lake_view.jf_portal.po_dtl AS d
        ON d.sku = hsd.sku
    JOIN lake_view.jf_portal.po_hdr AS h
        ON h.po_id = d.po_id
        AND h.show_room || '-01' < hsd.local_date
WHERE h.warehouse_id = CASE
        WHEN hsd.warehouse_id IN (107,154,421) THEN 1 --US
        WHEN hsd.warehouse_id = 109 THEN 2 --CA
        WHEN hsd.warehouse_id IN (108,221) THEN 3 --EU
        WHEN hsd.warehouse_id = 231 THEN 4 --LA
        WHEN hsd.warehouse_id = 366 THEN 5 --UK
        WHEN hsd.warehouse_id = 465 THEN 6 --MX
        WHEN hsd.warehouse_id = 467 THEN 7 --SXU
        WHEN hsd.warehouse_id = 466 THEN 8 --MX2
        ELSE 1 END
    AND NOT d.is_cancelled;

-- STEP 2.2. Grab "earliest" PO the SKU would have come from if not found
CREATE OR REPLACE TEMP TABLE _fc_sku_po_date_2 AS
SELECT DISTINCT
    hsd.warehouse_id,
    hsd.sku,
    hsd.local_date,
    FIRST_VALUE(h.po_number) OVER (PARTITION BY hsd.warehouse_id, d.sku, hsd.local_date ORDER BY h.show_room || '-01' ASC) AS po_number
FROM _hist_sku_date AS hsd
    LEFT JOIN _fc_sku_po_date_1 AS po
        ON po.warehouse_id = hsd.warehouse_id
        AND po.sku = hsd.sku
        AND po.local_date = hsd.local_date
    JOIN lake_view.jf_portal.po_dtl AS d
        ON d.sku = hsd.sku
    JOIN lake_view.jf_portal.po_hdr AS h
        ON h.po_id = d.po_id
WHERE po.po_number IS NULL
    AND h.warehouse_id = CASE
        WHEN hsd.warehouse_id IN (107, 154, 421) THEN 1 --US
        WHEN hsd.warehouse_id = 109 THEN 2 --CA
        WHEN hsd.warehouse_id IN (108, 221) THEN 3 --EU
        WHEN hsd.warehouse_id = 231 THEN 4 --LA
        WHEN hsd.warehouse_id = 366 THEN 5 --UK
        WHEN hsd.warehouse_id = 465 THEN 6 --MX
        WHEN hsd.warehouse_id = 467 THEN 7 --SXU
        WHEN hsd.warehouse_id = 466 THEN 8 --MX2
        ELSE 1 END
    AND NOT d.is_cancelled;

-- STEP 2.3. Merge _fc_sku_po_date
CREATE OR REPLACE TEMP TABLE _fc_sku_po_date AS
SELECT
    warehouse_id,
    sku,
    local_date,
    po_number
FROM _fc_sku_po_date_1
UNION
SELECT
    warehouse_id,
    sku,
    local_date,
    po_number
FROM _fc_sku_po_date_2;

-- STEP 2.4. Last ditch effort to identify PO
CREATE OR REPLACE TEMP TABLE _fc_sku_po_date_3 AS
SELECT DISTINCT
    hsd.warehouse_id,
    hsd.sku,
    hsd.local_date,
    FIRST_VALUE(h.po_number) OVER (PARTITION BY hsd.warehouse_id, d.sku, hsd.local_date ORDER BY h.show_room || '-01' ASC) AS po_number
FROM _hist_sku_date AS hsd
    LEFT JOIN _fc_sku_po_date AS po
        ON po.warehouse_id = hsd.warehouse_id
        AND po.sku = hsd.sku
        AND po.local_date = hsd.local_date
    JOIN lake_view.jf_portal.po_dtl AS d
        ON d.sku = hsd.sku
    JOIN lake_view.jf_portal.po_hdr AS h
        ON h.po_id = d.po_id
WHERE po.po_number IS NULL
    AND NOT d.is_cancelled;

-- STEP 2.5. Insert last results back into main table
INSERT INTO _fc_sku_po_date (warehouse_id, sku, local_date, po_number)
SELECT warehouse_id, sku, local_date, po_number FROM _fc_sku_po_date_3;

-- STEP 3. Grab estimated cost calculation from Merlin (reporting.gsc.po_detail_dataset)
CREATE OR REPLACE TEMP TABLE _fc_po_sku_cost_est AS
SELECT
    s.warehouse_id,
    s.po_number,
    ZEROIFNULL(po.cost) + ZEROIFNULL(po.commission) + ZEROIFNULL(po.inspection) + ZEROIFNULL(po.cmt) + ZEROIFNULL(po.duty) + (ZEROIFNULL(po.primary_tariff) * ZEROIFNULL(po.cost)) AS est_cost,
    s.sku,
    s.local_date
FROM _fc_sku_po_date AS s
    JOIN reporting.gsc.po_detail_dataset AS po
        ON po.po_number = s.po_number
        AND po.sku = s.sku;

-- STEP 4. Get old and new landed cost per unit amounts
CREATE OR REPLACE TEMP TABLE _fc_po_sku_cost_per_unit AS
SELECT
    fih.warehouse_id,
    fih.sku,
    fih.local_date,
    fih.landed_cost_per_unit AS old_landed_cost_per_unit,
    e.est_cost AS new_landed_cost_per_unit
FROM _fc_po_sku_cost_est AS e
    JOIN data_model.fact_inventory_history AS fih
        ON fih.warehouse_id = e.warehouse_id
        AND fih.sku = e.sku
        AND fih.local_date = e.local_date;
-- SELECT * FROM _fc_po_sku_cost_per_unit WHERE old_landed_cost_per_unit != new_landed_cost_per_unit;

-- STEP 5. Update history (Applied filter prevents UPDATE)
UPDATE stg.fact_inventory_history AS t
SET t.landed_cost_per_unit = s.new_landed_cost_per_unit
FROM _fc_po_sku_cost_per_unit AS s
WHERE s.warehouse_id = t.warehouse_id
    AND s.sku = t.sku
    AND s.local_date = t.local_date
    AND s.new_landed_cost_per_unit != t.landed_cost_per_unit
    AND TRUE = FALSE;
