set start_date = cast('2019-01-01' as date);


create or replace temporary table _sales_info as
select
    ol.BUSINESS_UNIT as BUSINESS_UNIT
    ,ol.REGION as REGION
    ,ol.COUNTRY as COUNTRY
    ,ol.ORDER_DATE as order_date

    --Sales
    ,sum(ol.TOTAL_QTY_SOLD) as total_qty_sold
    ,count(DISTINCT ol.ORDER_ID) as orders
    ,sum(ol.TOTAL_DISCOUNT) as TOTAL_DISCOUNT
    ,sum(ol.TOTAL_PRODUCT_REVENUE) as total_product_revenue
    ,sum(ol.TOTAL_COGS) as total_cogs
    ,avg(mdp.OG_RETAIL) as avg_og_vip_price
    ,avg(mdp.CURRENT_VIP_RETAIL) as avg_current_vip_price

    --activating
    ,sum(case
            when ol.ORDER_TYPE = 'vip activating'
            then ol.TOTAL_QTY_SOLD else 0 end) as activating_total_qty_sold
    ,count(DISTINCT case
            when ol.ORDER_TYPE = 'vip activating'
            then ol.ORDER_ID end) as activating_orders
    ,sum(case
            when ol.ORDER_TYPE = 'vip activating'
            then ol.TOTAL_DISCOUNT else 0 end) as activating_TOTAL_DISCOUNT
    ,sum(case
            when ol.ORDER_TYPE = 'vip activating'
            then ol.TOTAL_PRODUCT_REVENUE else 0 end) as activating_total_product_revenue
    ,sum(case
            when ol.ORDER_TYPE = 'vip activating'
            then ol.TOTAL_COGS else 0 end) as activating_total_cogs

    --repeat
    ,sum(case
            when ol.ORDER_TYPE != 'vip activating'
            then ol.TOTAL_QTY_SOLD else 0 end) as repeat_total_qty_sold
    ,count(DISTINCT case
            when ol.ORDER_TYPE != 'vip activating'
            then ol.ORDER_ID end) as repeat_orders
    ,sum(case
            when ol.ORDER_TYPE != 'vip activating'
            then ol.TOTAL_DISCOUNT else 0 end) as repeat_TOTAL_DISCOUNT
    ,sum(case
            when ol.ORDER_TYPE != 'vip activating'
            then ol.TOTAL_PRODUCT_REVENUE else 0 end) as repeat_total_product_revenue
    ,sum(case
            when ol.ORDER_TYPE != 'vip activating'
            then ol.TOTAL_COGS else 0 end) as repeat_total_cogs
from reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE ol
join REPORTING_PROD.GFB.MERCH_DIM_PRODUCT mdp
    on mdp.BUSINESS_UNIT = ol.BUSINESS_UNIT
    and mdp.REGION = ol.REGION
    and mdp.COUNTRY = ol.COUNTRY
    and mdp.PRODUCT_SKU = ol.PRODUCT_SKU
where
    ol.ORDER_CLASSIFICATION = 'product order'
    and ol.ORDER_DATE >= $start_date
group by
    ol.BUSINESS_UNIT
    ,ol.REGION
    ,ol.COUNTRY
    ,ol.ORDER_DATE;


create or replace temporary table _inventory_info as
select
    id.BUSINESS_UNIT
    ,id.REGION as region
    ,id.COUNTRY as country
    ,id.INVENTORY_DATE as inventory_date

    ,sum(id.QTY_ONHAND) as QTY_ONHAND
    ,sum(id.QTY_AVAILABLE_TO_SELL) as QTY_AVAILABLE_TO_SELL
    ,sum(id.QTY_OPEN_TO_BUY) as QTY_OPEN_TO_BUY
from REPORTING_PROD.GFB.gfb_inventory_data_set id
where
    id.INVENTORY_DATE >= $start_date
group by
    id.BUSINESS_UNIT
    ,id.REGION
    ,id.COUNTRY
    ,id.INVENTORY_DATE;


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb033_merch_kpi_over_time as
select
    coalesce(si.BUSINESS_UNIT, ii.BUSINESS_UNIT) as BUSINESS_UNIT
    ,coalesce(si.REGION, ii.region) as region
    ,coalesce(si.COUNTRY, ii.country) as country
    ,coalesce(si.order_date, ii.inventory_date) as date

    ,coalesce(si.total_qty_sold, 0) as total_qty_sold
    ,coalesce(si.orders, 0) as orders
    ,coalesce(si.TOTAL_DISCOUNT, 0) as TOTAL_DISCOUNT
    ,coalesce(si.total_product_revenue, 0) as total_product_revenue
    ,coalesce(si.total_cogs, 0) as total_cogs
    ,coalesce(si.avg_og_vip_price, 0) as avg_og_vip_price
    ,coalesce(si.avg_current_vip_price, 0) as avg_current_vip_price
    ,coalesce(ii.QTY_AVAILABLE_TO_SELL, 0) as QTY_AVAILABLE_TO_SELL

    --activating
    ,coalesce(si.activating_total_qty_sold, 0) as activating_total_qty_sold
    ,coalesce(si.activating_orders, 0) as activating_orders
    ,coalesce(si.activating_TOTAL_DISCOUNT, 0) as activating_TOTAL_DISCOUNT
    ,coalesce(si.activating_total_product_revenue, 0) as activating_total_product_revenue
    ,coalesce(si.activating_total_cogs, 0) as activating_total_cogs

    --repeat
    ,coalesce(si.repeat_total_qty_sold, 0) as repeat_total_qty_sold
    ,coalesce(si.repeat_orders, 0) as repeat_orders
    ,coalesce(si.repeat_TOTAL_DISCOUNT, 0) as repeat_TOTAL_DISCOUNT
    ,coalesce(si.repeat_total_product_revenue, 0) as repeat_total_product_revenue
    ,coalesce(si.repeat_total_cogs, 0) as repeat_total_cogs
from _sales_info si
full join _inventory_info ii
    on ii.BUSINESS_UNIT = si.BUSINESS_UNIT
    and ii.region = si.REGION
    and ii.country = si.COUNTRY
    and ii.inventory_date = si.order_date
