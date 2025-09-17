TRUNCATE TABLE validation.landed_cost_zero;
INSERT INTO validation.landed_cost_zero
WITH cte AS(
SELECT
    sku,
    SUM(ZEROIFNULL(cost) + ZEROIFNULL(commission) + ZEROIFNULL(inspection) + ZEROIFNULL(cmt) + ZEROIFNULL(duty)
        + (ZEROIFNULL(primary_tariff) * ZEROIFNULL(cost))) AS sum_of_estimated_cost_per_unit
FROM reporting_prod.gsc.po_detail_dataset
GROUP BY sku
HAVING sum_of_estimated_cost_per_unit::int > 0)
SELECT
       f.item_id,
       f.warehouse_id,
       f.brand,
       f.region,
       f.is_retail,
       f.sku,
       f.product_sku,
       f.open_to_buy_quantity,
       f.intransit_quantity,
       f.landed_cost_per_unit
FROM data_model.fact_inventory f
JOIN lake_view.ultra_warehouse.item i
    ON f.item_id = i.item_id
WHERE
    f.sku IN(SELECT DISTINCT sku FROM cte)
    AND open_to_buy_quantity + intransit_quantity > 5
    AND IFNULL(wms_class,'') != 'CONSUMABLE'
    AND i.datetime_added > dateadd("year",-2,getdate())
    AND f.landed_cost_per_unit =0
    AND f.region_id !=9;
