set start_date = cast('2021-01-01' as date);
set end_date = current_date();


create or replace temporary table _store as
select
    *
from EDW_PROD.DATA_MODEL_JFB.DIM_STORE st
where
    st.STORE_BRAND_ABBR in ('JF', 'SD', 'FK')
    and st.STORE_FULL_NAME not like '%(DM)%'
    and st.STORE_FULL_NAME not like '%Wholesale%'
    and st.STORE_FULL_NAME not like '%Heels.com%'
    and st.STORE_FULL_NAME not like '%Retail%'
    and st.STORE_FULL_NAME not like '%Sample%'
    and st.STORE_FULL_NAME not like '%SWAG%'
    and st.STORE_FULL_NAME not like '%PS%';


create or replace temporary table _daily_inventory as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.SKU
    ,a.INVENTORY_DATE
    ,sum(a.QTY_ONHAND) as QTY_ONHAND
from REPORTING_PROD.GFB.GFB_INVENTORY_DATA_SET a
group by
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.SKU
    ,a.INVENTORY_DATE
having
    sum(a.QTY_ONHAND) > 0;


create or replace temporary table _days_on_hand as
select
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.SKU
    ,min(a.INVENTORY_DATE) as earliest_inventory_date
    ,max(a.INVENTORY_DATE) as latest_inventory_date
    ,datediff(day, earliest_inventory_date, latest_inventory_date) as days_on_onhand
from _daily_inventory a
group by
    a.BUSINESS_UNIT
    ,a.REGION
    ,a.COUNTRY
    ,a.SKU;


create or replace temporary table _inventory_info as
select distinct
    mdp.BUSINESS_UNIT
    ,mdp.REGION
    ,mdp.COUNTRY
    ,mdp.PRODUCT_SKU
    ,id.SKU
    ,psm.CLEAN_SIZE as size
    ,psm.core_size_flag
    ,(case
        when mdp.LATEST_LAUNCH_DATE > date_trunc(month, dateadd(day, -1, current_date()))
        then coalesce(mdp.PREVIOUS_LAUNCH_DATE, mdp.LATEST_LAUNCH_DATE)
        else mdp.LATEST_LAUNCH_DATE end) as current_showroom
    ,mdp.DEPARTMENT
    ,mdp.SUBCATEGORY
    ,mdp.WW_WC
    ,mdp.SHARED
    ,mdp.PRODUCT_STYLE_NUMBER
    ,mdp.COLOR
    ,id.WAREHOUSE
    ,mdp.DEPARTMENT_DETAIL
    ,mdp.IMAGE_URL
    ,mdp.LARGE_IMG_URL
    ,mdp.STYLE_NAME
    ,mdp.CORESB_REORDER_FASHION
    ,mdp.QTY_PENDING as product_sku_qty_pending
    ,coalesce(sps.IS_ACTIVE, 'Inactive') as IS_ACTIVE
    ,mdp.SEASON_CODE
    ,(case
        when mdp.IS_BASE_SKU = 1 then 'Yes'
        else 'No' end) as IS_BASE_SKU
    ,(case
        when mcsl.SKU is not null then 'clearance'
        when st.STORE_BRAND_ABBR in ('JF', 'SD') and round(pph.SALE_PRICE) - pph.SALE_PRICE = 0.03 then 'clearance'
        when st.STORE_BRAND_ABBR in ('FK') and round(pph.SALE_PRICE) - pph.SALE_PRICE = 0.01 then 'clearance'
        when pph.SALE_PRICE > 0 then 'markdown'
        else 'regular' end) as clearance_flag

    ,coalesce(id.QTY_AVAILABLE_TO_SELL, 0) as total_inventory
from reporting_prod.GFB.MERCH_DIM_PRODUCT mdp
join reporting_prod.GFB.GFB_INVENTORY_DATA_SET_CURRENT id
    on mdp.BUSINESS_UNIT = id.BUSINESS_UNIT
    and mdp.REGION = id.REGION
    and mdp.COUNTRY = id.COUNTRY
    and mdp.PRODUCT_SKU = id.PRODUCT_SKU
join _store st
    on lower(st.STORE_BRAND) = lower(mdp.BUSINESS_UNIT)
    and lower(st.STORE_REGION) = lower(mdp.REGION)
    and lower(st.STORE_COUNTRY) = lower(mdp.COUNTRY)
left join REPORTING_PROD.GFB.GFB_SKU_PRODUCT_STATUS sps
    on sps.BUSINESS_UNIT = mdp.BUSINESS_UNIT
    and sps.REGION = mdp.REGION
    and sps.SKU = id.SKU
left join REPORTING_PROD.GFB.VIEW_PRODUCT_SIZE_MAPPING psm
    on lower(psm.SIZE) = lower(id.DP_SIZE)
    and psm.product_sku = mdp.PRODUCT_SKU
    and psm.REGION = mdp.REGION
left join LAKE_VIEW.sharepoint.GFB_MERCH_CLEARANCE_SKU_LIST mcsl
    on lower(mcsl.BUSINESS_UNIT) = lower(id.BUSINESS_UNIT)
    and lower(mcsl.region) = lower(id.REGION)
    and lower(mcsl.SKU) = lower(id.PRODUCT_SKU)
    and (current_date() between mcsl.START_DATE and coalesce(mcsl.END_DATE, current_date()))
left join (
    SELECT DISTINCT
        MASTER_PRODUCT_ID,
        EFFECTIVE_START_DATETIME,
        EFFECTIVE_END_DATETIME,
        SALE_PRICE
    FROM EDW_PROD.DATA_MODEL_JFB.DIM_PRODUCT_PRICE_HISTORY
    ) AS pph
    on pph.MASTER_PRODUCT_ID = mdp.MASTER_PRODUCT_ID
    and (current_date() between pph.EFFECTIVE_START_DATETIME and pph.EFFECTIVE_END_DATETIME)
where
    id.COUNTRY != 'CA'; /* since we do not have Canada warehouse any more. */


create or replace temporary table _product_sales_days as
select
    ol.BUSINESS_UNIT
    ,ol.REGION
    ,ol.COUNTRY
    ,ol.PRODUCT_SKU
    ,min(ol.ORDER_DATE) as first_sale_date
    ,max(ol.ORDER_DATE) as last_sale_date
    ,datediff(day, min(ol.ORDER_DATE), max(ol.ORDER_DATE)) as days_of_selling
from reporting_prod.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE ol
where
    ol.ORDER_CLASSIFICATION = 'product order'
group by
    ol.BUSINESS_UNIT
    ,ol.REGION
    ,ol.COUNTRY
    ,ol.PRODUCT_SKU;


create or replace temporary table _out_of_stock_by_size as
select
    ci.BUSINESS_UNIT
    ,ci.REGION
    ,ci.COUNTRY
    ,ci.PRODUCT_SKU
    ,ci.SKU
    ,ci.size
    ,ci.current_showroom
    ,ci.DEPARTMENT
    ,ci.SUBCATEGORY
    ,ci.PRODUCT_STYLE_NUMBER
    ,ci.COLOR
    ,ci.DEPARTMENT_DETAIL
    ,ci.IMAGE_URL
    ,ci.LARGE_IMG_URL
    ,ci.STYLE_NAME
    ,ci.CORESB_REORDER_FASHION
    ,coalesce(psd.first_sale_date, current_date()) as first_sale_date
    ,ci.product_sku_qty_pending
    ,ci.core_size_flag
    ,ci.IS_ACTIVE
    ,ci.SEASON_CODE
    ,ci.IS_BASE_SKU
    ,ci.clearance_flag

    ,sum(ci.total_inventory) as total_inventory
    ,sum(case
            when ci.core_size_flag = 'core' then ci.total_inventory
        end) as core_inventory
from _inventory_info ci
left join _product_sales_days psd
    on psd.BUSINESS_UNIT = ci.BUSINESS_UNIT
    and psd.REGION = ci.REGION
    and psd.COUNTRY = ci.COUNTRY
    and psd.PRODUCT_SKU = ci.PRODUCT_SKU
group by
    ci.BUSINESS_UNIT
    ,ci.REGION
    ,ci.COUNTRY
    ,ci.PRODUCT_SKU
    ,ci.SKU
    ,ci.size
    ,ci.current_showroom
    ,ci.DEPARTMENT
    ,ci.SUBCATEGORY
    ,ci.PRODUCT_STYLE_NUMBER
    ,ci.COLOR
    ,ci.DEPARTMENT_DETAIL
    ,ci.IMAGE_URL
    ,ci.LARGE_IMG_URL
    ,ci.STYLE_NAME
    ,ci.CORESB_REORDER_FASHION
    ,psd.first_sale_date
    ,ci.product_sku_qty_pending
    ,ci.core_size_flag
    ,ci.IS_ACTIVE
    ,ci.SEASON_CODE
    ,ci.IS_BASE_SKU
    ,ci.clearance_flag;


create or replace temporary table _core_size_broken as
select
    oss.BUSINESS_UNIT
    ,oss.REGION
    ,oss.COUNTRY
    ,oss.PRODUCT_SKU
    ,count(case
            when oss.core_inventory = 0 then oss.PRODUCT_SKU end) as core_size_broken_count
    ,sum(oss.total_inventory) as product_sku_inventory
from _out_of_stock_by_size oss
group by
    oss.BUSINESS_UNIT
    ,oss.REGION
    ,oss.COUNTRY
    ,oss.PRODUCT_SKU;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb015_out_of_stock_by_size as
select
    oss.*

    ,(case
        when csb.product_sku_inventory = 0 then 'Sold Out In All Sizes'
        when csb.core_size_broken_count = 1 then 'Broken In 1 Core Size'
        when csb.core_size_broken_count > 1 then 'Broken In 2+ Core Sizes'
        else 'Not Broken In Core Size' end) as core_size_broken_flag
    ,doh.earliest_inventory_date
    ,doh.latest_inventory_date
    ,(case
        when oss.total_inventory = 0 then null
        else doh.days_on_onhand end) as days_on_onhand
    ,wlp.PRODUCT_WAIT_LIST_TYPE
    ,wlp.waitlist_with_known_eta_flag
    ,wlp.waitlist_start_datetime
    ,wlp.waitlist_end_datetime
    ,coalesce(wlp.WW_WC, 'Regular') as ww_wc_flag
from _out_of_stock_by_size oss
left join _core_size_broken csb
    on csb.BUSINESS_UNIT = oss.BUSINESS_UNIT
    and csb.REGION = oss.REGION
    and csb.COUNTRY = oss.COUNTRY
    and csb.PRODUCT_SKU = oss.PRODUCT_SKU
left join _days_on_hand doh
    on doh.BUSINESS_UNIT = oss.BUSINESS_UNIT
    and doh.REGION = oss.REGION
    and doh.COUNTRY = oss.COUNTRY
    and doh.SKU = oss.SKU
left join REPORTING_PROD.GFB.MERCH_DIM_PRODUCT wlp
    on wlp.business_unit = oss.BUSINESS_UNIT
    and wlp.region = oss.REGION
    and wlp.country = oss.COUNTRY
    and wlp.PRODUCT_SKU = oss.PRODUCT_SKU;
