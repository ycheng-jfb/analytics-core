/*
This script is to pull the historical data till August, 2023(sourcing from Merlin db).
From September, 2024 we are sourcing the data from reporting_prod.gsc.po_detail_dataset(GSC team owns this view)

This script is using reference.po_sku_cost_archive(cloned copy of reference.po_sku_cost),
this is the only source of historical data sourcing data from Merlin.

CREATE OR REPLACE TABLE reference.po_sku_cost_archive
CLONE  reference.po_sku_cost;

Please do not drop or truncate this object, doing so, we will lose all the data
*/

INSERT INTO reference.po_sku_cost_history
SELECT
    a.po_sku_cost_id,
    a.po_number,
    a.sku,
    a.brand,
    a.po_status,
    a.start_date,
    a.currency,
    a.quantity,
    a.freight,
    a.duty,
    a.cmt,
    a.cost,
    a.landed_cost,
    a.meta_row_hash,
    a.meta_create_datetime,
    a.meta_update_datetime
FROM reference.po_sku_cost_archive a
WHERE a.start_date < '2023-09-01'
OR EXISTS (
            SELECT 1 FROM reporting_prod.gsc.po_detail_dataset d
            WHERE to_date(show_room, 'YYYY-MM') < '2023-09-01'
            AND d.qty > 0
            AND d.po_status_id IN (3,4,8,12)
            AND d.po_number NOT ILIKE '%CS'
            AND a.po_number = d.po_number
            AND UPPER(a.sku) = UPPER(d.sku)
);
