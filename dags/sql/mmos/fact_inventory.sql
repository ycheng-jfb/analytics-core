
truncate table EDW_PROD.NEW_STG.fact_inventory;

insert into EDW_PROD.NEW_STG.fact_inventory
select
    t.SKU AS item_id,
    t.WAREHOUSENAME,
    t.BRAND_NAME ,
    case when t.BRAND_NAME in ('JUSTFAB','FABKIDS','SHOEDAZZLE') then 'US' else null end as REGION,
    false as IS_RETAIL,
    t.SKU AS sku,
    UPPER(TRIM(SUBSTRING(t.sku, 1, CHARINDEX('-', t.sku, CHARINDEX('-', t.sku) + 1) - 1))) AS product_sku,
     t.availableQty + t.damagedQty + t.lockQty as onhand_quantity,
     t2.totalOnWayQty AS replen_quantity,
     0 AS ghost_quantity,
     null as reserve_quantity,
     0 as special_pick_quantity,
     null manual_stock_reserve_quantity,
     t.AVAILABLEQTY,
     t.AVAILABLEQTY ecom_available_to_sell_quantity,
     0 receipt_inspection_quantity,
     null return_quantity,
     t.DAMAGEDQTY AS damaged_quantity,
     null as damaged_returns_quantity,
     null allocated_quantity,
     t2.TOTALONWAYQTY AS intransit_quantity,
     null staging_quantity,
     null pick_staging_quantity,
     null lost_quantity,
     -- onhand_quantity + replen_quantity + receipt_inspection_quantity + pick_staging_quantity + staging_quantity
     t.availableQty + t.damagedQty + t.lockQty -- onhand_quantity
   + t2.totalOnWayQty -- replen_quantity
   + 0 + 0 + 0 as open_to_buy_quantity,
     null as landed_cost_per_unit, -- 成本补充
     null as total_onhand_landed_cost, -- 成本补充
     null as dsw_dropship_quantity
from LAKE.MMT.ODS_IMS_INVS_DF t
left join LAKE.MMT.ODS_IMS_ONWAY_DF t2
on t.sku = t2.sku and t.PHYSICALWAREHOUSECODE = t2.PHYSICALWAREHOUSECODE
       and t.channelId = t2.channelId and t.PT_DAY = t2.PT_DAY and t2.SNAPSHOTDATE = DATEADD(day, -1, CURRENT_DATE)
where t.PT_DAY = CURRENT_DATE;





















