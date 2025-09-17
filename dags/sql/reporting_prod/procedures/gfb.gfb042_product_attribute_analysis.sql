create or replace temporary table _product_sales as
select
    mds.BUSINESS_UNIT
    ,mds.REGION
    ,mds.COUNTRY
    ,date_trunc(month, mds.DATE) as month_date
    ,mds.PRODUCT_SKU
    ,mds.DEPARTMENT_DETAIL
    ,mds.SUBCATEGORY
    ,mds.COLOR_FAMILY_CONSOLE
    ,mds.FIT_CONSOLE
    ,mds.HEEL_SIZE_CONSOLE
    ,mds.HEEL_SHAPE_CONSOLE
    ,mds.TOE_TYPE_CONSOLE
    ,mds.MATERIAL_TYPE_CONSOLE
    ,mds.BOOT_SHAFT_HEIGHT_CONSOLE
    ,(case
        when month(mds.DATE) between 3 and 5 then 'spring'
        when month(mds.DATE) between 6 and 8 then 'summer'
        when month(mds.DATE) between 9 and 11 then 'fall'
        else 'winter' end) as season
    ,mds.LARGE_IMG_URL
    ,mds.CURRENT_SHOWROOM
    ,day(last_day(month_date)) as days_in_month
    ,mds.STYLE_NAME
    ,mds.CLEARANCE_FLAG
    ,mds.AVG_OVERALL_RATING
    ,mds.MASTER_COLOR
    ,mds.COLOR

    ,sum(mds.TOTAL_QTY_SOLD) as TOTAL_QTY_SOLD
    ,sum(mds.ACTIVATING_QTY_SOLD) as ACTIVATING_QTY_SOLD
    ,sum(mds.REPEAT_QTY_SOLD) as REPEAT_QTY_SOLD
    ,sum(mds.TOTAL_PRODUCT_REVENUE) as TOTAL_PRODUCT_REVENUE
    ,sum(mds.ACTIVATING_PRODUCT_REVENUE) as ACTIVATING_PRODUCT_REVENUE
    ,sum(mds.REPEAT_PRODUCT_REVENUE) as REPEAT_PRODUCT_REVENUE
    ,sum(mds.TOTAL_COGS) as TOTAL_COGS
    ,sum(mds.ACTIVATING_COGS) as ACTIVATING_COGS
    ,sum(mds.REPEAT_COGS) as REPEAT_COGS
    ,sum(mds.TOTAL_RETURN_UNIT) as TOTAL_RETURN_UNIT
    ,sum(case
            when mds.DATE = month_date then mds.QTY_AVAILABLE_TO_SELL
            else 0 end) as bop_QTY_AVAILABLE_TO_SELL
    ,sum(case
            when mds.DATE = last_day(month_date) then mds.QTY_AVAILABLE_TO_SELL
            else 0 end) as eop_QTY_AVAILABLE_TO_SELL
from REPORTING_PROD.gfb.DOS_107_MERCH_DATA_SET_BY_PLACE_DATE mds
where
    mds.DEPARTMENT_DETAIL = 'FOOTWEAR'
group by
    mds.BUSINESS_UNIT
    ,mds.REGION
    ,mds.COUNTRY
    ,date_trunc(month, mds.DATE)
    ,mds.PRODUCT_SKU
    ,mds.DEPARTMENT_DETAIL
    ,mds.SUBCATEGORY
    ,mds.COLOR_FAMILY_CONSOLE
    ,mds.FIT_CONSOLE
    ,mds.HEEL_SIZE_CONSOLE
    ,mds.HEEL_SHAPE_CONSOLE
    ,mds.TOE_TYPE_CONSOLE
    ,mds.MATERIAL_TYPE_CONSOLE
    ,mds.BOOT_SHAFT_HEIGHT_CONSOLE
    ,(case
        when month(mds.DATE) between 3 and 5 then 'spring'
        when month(mds.DATE) between 6 and 8 then 'summer'
        when month(mds.DATE) between 9 and 11 then 'fall'
        else 'winter' end)
    ,mds.LARGE_IMG_URL
    ,mds.CURRENT_SHOWROOM
    ,day(last_day(month_date))
    ,mds.STYLE_NAME
    ,mds.CLEARANCE_FLAG
    ,mds.AVG_OVERALL_RATING
    ,mds.MASTER_COLOR
    ,mds.COLOR
having
    sum(mds.TOTAL_QTY_SOLD) > 0;


create or replace temporary table _product_out_of_stock as
select
    oos.BUSINESS_UNIT
    ,oos.REGION
    ,oos.COUNTRY
    ,oos.PRODUCT_SKU
    ,date_trunc(month, oos.INVENTORY_DATE) as month_date

    ,count(distinct case
            when oos.CORE_SIZE_BROKEN_FLAG like 'Broken In%' and oos.CORE_SIZE_BROKEN_FLAG like '%Core Size%'
            then oos.INVENTORY_DATE end) as days_core_size_broken
from REPORTING_PROD.GFB.GFB015_01_OUT_OF_STOCK_BY_SIZE_HIST oos
where
    oos.DEPARTMENT_DETAIL = 'FOOTWEAR'
group by
    oos.BUSINESS_UNIT
    ,oos.REGION
    ,oos.COUNTRY
    ,oos.PRODUCT_SKU
    ,date_trunc(month, oos.INVENTORY_DATE);


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.GFB042_PRODUCT_ATTRIBUTE_ANALYSIS as
select
    ps.*
    ,oos.days_core_size_broken
from _product_sales ps
left join _product_out_of_stock oos
    on oos.BUSINESS_UNIT = ps.BUSINESS_UNIT
    and oos.REGION = ps.REGION
    and oos.COUNTRY = ps.COUNTRY
    and oos.PRODUCT_SKU = ps.PRODUCT_SKU
    and oos.month_date = ps.month_date;


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb042_product_attribute_subcategory as
select distinct
    a.SUBCATEGORY
from REPORTING_PROD.GFB.GFB042_PRODUCT_ATTRIBUTE_ANALYSIS a

union select 'All';


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb042_product_attribute_color_family as
select distinct
    a.COLOR_FAMILY_CONSOLE
from REPORTING_PROD.GFB.GFB042_PRODUCT_ATTRIBUTE_ANALYSIS a
where
    a.COLOR_FAMILY_CONSOLE not like '%,%'

union select 'All';


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb042_product_attribute_fit as
select distinct
    a.FIT_CONSOLE
from REPORTING_PROD.GFB.GFB042_PRODUCT_ATTRIBUTE_ANALYSIS a
where
    a.FIT_CONSOLE not like '%,%'

union select 'All';


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb042_product_attribute_heel_shape as
select distinct
    a.HEEL_SHAPE_CONSOLE
from REPORTING_PROD.GFB.GFB042_PRODUCT_ATTRIBUTE_ANALYSIS a
where
    a.HEEL_SHAPE_CONSOLE not like '%,%'

union select 'All';


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb042_product_attribute_heel_size as
select distinct
    a.HEEL_SIZE_CONSOLE
from REPORTING_PROD.GFB.GFB042_PRODUCT_ATTRIBUTE_ANALYSIS a
where
    a.HEEL_SIZE_CONSOLE not like '%,%'

union select 'All';


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb042_product_attribute_material_type as
select distinct
    a.MATERIAL_TYPE_CONSOLE
from REPORTING_PROD.GFB.GFB042_PRODUCT_ATTRIBUTE_ANALYSIS a
where
    a.MATERIAL_TYPE_CONSOLE not like '%,%'

union select 'All';


CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.GFB.gfb042_product_attribute_toe_type as
select distinct
    a.TOE_TYPE_CONSOLE
from REPORTING_PROD.GFB.GFB042_PRODUCT_ATTRIBUTE_ANALYSIS a
where
    a.TOE_TYPE_CONSOLE not like '%,%'

union select 'All';
