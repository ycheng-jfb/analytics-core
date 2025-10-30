insert into fact_inventory_in_transit_from
select
  od.fromPhysicalWarehouseCode,
  od.toPhysicalWarehouseCode,
  od.sku,
  sum(
    od.expectedInboundQuantity - od.actualInboundQuantity
  )
from
  lake.mmt.ODS_IMS_T_ON_WAY_INFO_DETAIL_DF od
where od.PT_DAY = current_date
group by od.fromPhysicalWarehouseCode,
  od.toPhysicalWarehouseCode,
  od.sku;


CREATE OR REPLACE TABLE fact_inventory_in_transit_from (
    fromPhysicalWarehouseCode VARCHAR COMMENT '来源仓库代码',
    toPhysicalWarehouseCode VARCHAR COMMENT '目标仓库代码',
    sku VARCHAR COMMENT 'SKU',
    sum_expected_minus_actual NUMBER COMMENT '预期入库数量与实际入库数量差值总和'
) COMMENT = '库存来源在途事实表';


select * from fact_inventory_in_transit_from;




CREATE OR REPLACE VIEW data_model_jfb.fact_inventory_in_transit_from (
    item_id,
    sku,
    from_warehouse_id,
    total_units
    ) AS
SELECT
    item_id,
    sku,
    from_warehouse_id,
    SUM(units) AS total_units
FROM stg.fact_inventory_in_transit
WHERE NOT is_deleted
GROUP BY
    item_id,
    sku,
    from_warehouse_id

union  all
SELECT
    item_id,
    sku,
    from_warehouse_id,
    SUM(units) AS total_units
FROM NEW_STG.fact_inventory_in_transit
GROUP BY
    item_id,
    sku,
    from_warehouse_id



select * from EDW_PROD.data_model_jfb.fact_inventory_in_transit_from;