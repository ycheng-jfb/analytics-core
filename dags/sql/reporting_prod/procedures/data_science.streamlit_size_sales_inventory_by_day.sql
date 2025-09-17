-- inventory and sales by day by size by mpid_variation
create or replace temporary table _products as select product_id, MASTER_PRODUCT_ID, sku, product_name, department,
replace(replace(replace(replace(replace(size, '/', ''),'|',''),'  ',' '),'  ',' '),' ','_') as size
from EDW_PROD.DATA_MODEL_FL.DIM_PRODUCT where store_id=52 and master_product_id != -1
and department in ('Womens', 'Mens', 'Shapewear - Womens', 'Scrubs Womens', 'Scrubs Mens');

create or replace temp table _inventory as (
select sku, local_date as date, sum(AVAILABLE_TO_SELL_QUANTITY) as available_to_sell_quantity
from EDW_PROD.DATA_MODEL_FL.FACT_INVENTORY_HISTORY
where SKU in (select sku from _products) and not is_retail and region in ('US-WC', 'US-EC', 'CA', 'MX')
group by local_date, sku);

create or replace temp table _sales as (select ol.product_id, ol.ORDER_LOCAL_DATETIME::date as date, sum(ITEM_QUANTITY) as sales
from EDW_PROD.DATA_MODEL_FL.FACT_ORDER_LINE ol
join edw_prod.data_model_fl.fact_order o on ol.order_id=o.order_id
join EDW_PROD.DATA_MODEL_FL.DIM_ORDER_STATUS as dos on dos.ORDER_STATUS_KEY = o.ORDER_STATUS_KEY
join EDW_PROD.DATA_MODEL_FL.DIM_ORDER_LINE_STATUS as os on os.ORDER_LINE_STATUS_KEY = ol.ORDER_LINE_STATUS_KEY
join EDW_PROD.DATA_MODEL_FL.DIM_ORDER_SALES_CHANNEL as sc on sc.ORDER_SALES_CHANNEL_KEY = o.ORDER_SALES_CHANNEL_KEY
left join edw_prod.DATA_MODEL.FACT_RETURN_LINE frl on frl.ORDER_LINE_ID = ol.ORDER_LINE_ID
where ol.PRODUCT_ID in (select product_id from _products)
    and ol.store_id=52 and sc.ORDER_SALES_CHANNEL_L1 = 'Online Order'   -- these two clauses seem to do the same thing
    -- and sc.ORDER_CLASSIFICATION_L1 = 'Product Order'                 -- all deplete inventory, e.g. exchange
    and dos.order_status = 'Success'                                    -- only successful orders deplete inventory
    and os.order_line_status != 'Cancelled'
    and frl.return_line_id is null                                      -- returns go back into inventory
group by ol.ORDER_LOCAL_DATETIME::date, ol.PRODUCT_ID);

create or replace temp table _sales_inventory_by_day as
select i.date, i.available_to_sell_quantity, coalesce(s.sales,0) as sales, p.product_id, p.MASTER_PRODUCT_ID,
lower(tags.value) as subclass, p.sku, product_name, department,
split_part(size, '_', 1) as size, split_part(size, '_', 1) as size_original, split_part(size, '_', 2) as variation, split_part(size, '_', 3) as length,
replace(concat(MASTER_PRODUCT_ID, '_', split_part(size, '_', 2), '_', split_part(size, '_', 3)), '__','') as mpid_variation,
ty_unit_count_activating,ty_bop_available_to_sell_qty,ty_eop_available_to_sell_qty
from _products p join _inventory i on i.sku = p.sku
left join _sales s on i.date=s.date and s.product_id = p.product_id
left join reporting_base_prod.data_science.product_recommendation_tags tags on tags.mpid=p.MASTER_PRODUCT_ID
left join EDW_PROD.REPORTING.MERCH_PLANNING_ITEM_SKU mpis on mpis.sku=p.sku and i.date=mpis.date
where mpis.store_id=52 and mpis.date_type='Inventory' having sales>0
and LOWER(tags.name)='subclass' and tag_type='merch' and mpis.store_id=52 and mpis.date_type='Inventory';

-- normalize sizes, these are the same sizes: choose XXL-1X
-- TODO: normalize sizes for departments other than Womans
update _sales_inventory_by_day set size='XXL-1X' where size in ('XXL', '1X');

create or replace temp table _total_sales as select mpid_variation, date, sum(sales) as total_sales
from _sales_inventory_by_day group by mpid_variation, date;

create or replace temp table _broke_inventory as select mpid_variation, date,
(min(available_to_sell_quantity-sales)<1 or min(ty_eop_available_to_sell_qty)<1 or min(ty_bop_available_to_sell_qty-sales)<1) as broke_inventory
from _sales_inventory_by_day group by mpid_variation, date;

-- to be safe, mark any day before a broke day as broke
create or replace temp table _day_before_broke as
select distinct mpid_variation, dateadd('day', -1, date) as date, TRUE as day_before_broke from _broke_inventory where broke_inventory=True;

-- TODO: update non_standard_sizes for deparments other than Womans
create or replace temp table _non_standard_sizes as select mpid_variation, TRUE as non_standard_sizes from _sales_inventory_by_day
where size not in ('4X', '3X', '2X', 'XXL-1X', 'XL', 'L', 'M', 'S', 'XS', 'XXS') group by mpid_variation;

create or replace temp table _sales_inventory_by_day_merge as
select si.*, ts.total_sales, broke_inventory, day_before_broke, non_standard_sizes
from _sales_inventory_by_day si
left join _total_sales ts on ts.mpid_variation=si.mpid_variation and ts.date=si.date
left join _broke_inventory bi on bi.mpid_variation=si.mpid_variation and bi.date=si.date
left join _day_before_broke dbb on dbb.mpid_variation=si.mpid_variation and dbb.date=si.date
left join _non_standard_sizes nss on nss.mpid_variation=si.mpid_variation;

update _sales_inventory_by_day_merge set day_before_broke=FALSE where day_before_broke is null;
update _sales_inventory_by_day_merge set broke_inventory=FALSE where broke_inventory is null;
update _sales_inventory_by_day_merge set non_standard_sizes=FALSE where non_standard_sizes is null;
create or replace transient table reporting_prod.data_science.streamlit_size_sales_inventory_by_day as select * from _sales_inventory_by_day_merge;

create or replace transient table reporting_prod.data_science.streamlit_size_sales_by_day as select *,
as_double(coalesce(R4X, 0) + coalesce(R3X, 0) + coalesce(R2X, 0) + coalesce(RXXL1X, 0) + coalesce(RXL, 0) + coalesce(RL, 0) + coalesce(RM, 0)
+ coalesce(RS, 0) + coalesce(RXS, 0) + coalesce(RXXS, 0)) as standard_size_total_sales from
    (select date, master_product_id, total_sales, product_name, variation, length, mpid_variation, subclass, department,
    broke_inventory, day_before_broke, non_standard_sizes, sales, size
    from reporting_prod.data_science.streamlit_size_sales_inventory_by_day)
        pivot(sum(sales) for size in ('4X', '3X', '2X', 'XXL-1X', 'XL', 'L', 'M', 'S', 'XS', 'XXS'))
        as p (date, master_product_id, total_sales, product_name, variation, length, mpid_variation, subclass, department,
        broke_inventory, day_before_broke, non_standard_sizes,
        r4x, r3x, r2x, rxxl1x, rxl, rl, rm, rs, rxs, rxxs);

create or replace transient table reporting_prod.data_science.streamlit_size_summary as select mpid_variation, department, subclass,
concat(iff(sum(R4X)>0, 'T', 'F'), iff(sum(R3X)>0, 'T', 'F'), iff(sum(R2X)>0, 'T', 'F'), iff(sum(RXXL1X)>0, 'T', 'F'), iff(sum(RXL)>0, 'T', 'F'),
iff(sum(RL)>0, 'T', 'F'), iff(sum(RM)>0, 'T', 'F'), iff(sum(RS)>0, 'T', 'F'), iff(sum(RXS)>0, 'T', 'F'), iff(sum(RXXS)>0, 'T', 'F')) as nonzero_sizes
from reporting_prod.data_science.streamlit_size_sales_by_day group by mpid_variation, department, subclass;
