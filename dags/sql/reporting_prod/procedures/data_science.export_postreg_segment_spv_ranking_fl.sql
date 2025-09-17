set data_update_days = 3;
set start_date = dateadd(day, -$data_update_days, current_timestamp())::DATE;
set BRAND_ABBR = 'FL';
set GRID_NAME = 'spv_all';

CREATE OR REPLACE TEMPORARY TABLE _identity as (select
user_id, memb_start, membership_status, test_account, memb_end from(
    select DISTINCT userid as user_id, Timestamp as memb_start,
        case when lower(TRAITS_MEMBERSHIP_STATUS) = 'lead' then 'lead'
            when lower(TRAITS_MEMBERSHIP_STATUS) in ('elite', 'vip') then 'vip'
            else 'other' end as membership_status,
    	IFF(TRAITS_EMAIL like '%@test.com' or TRAITS_EMAIL like '%@example.com', 'test', '') as test_account,
    	coalesce(lead(TIMESTAMP) over (partition by userid order by TIMESTAMP), '9999-12-31') as memb_end
from (select distinct userid, timestamp, TRAITS_MEMBERSHIP_STATUS, TRAITS_EMAIL
      from lake.segment_fl.javascript_fabletics_identify)) where memb_end > dateadd(day, -1, $start_date));

-- segment add to cart
create or replace table _segment_add_to_cart as (
   select IFF(properties_is_bundle=TRUE, properties_bundle_product_id, properties_product_id) as add_mpid,
        properties_is_bundle as add_is_bundle,
        convert_timezone('UTC','America/Los_Angeles',TIMESTAMP)::Date as add_date,
        coalesce(membership_status, 'other') as add_membership,
        count(distinct(so.properties_session_ID)) as adds
FROM lake.segment_fl.java_fabletics_product_added so
    left join _identity i on so.USERID=i.user_id
    where convert_timezone('UTC','America/Los_Angeles',TIMESTAMP)::DATE >= $start_date
    -- the following membership bracket clause line is in UTC
    and ((TIMESTAMP>memb_start and TIMESTAMP<memb_end) or membership_status is null)
    and properties_AUTOMATED_TEST=FALSE and ifnull(i.test_account,'')<>'test'
    group by add_mpid, add_is_bundle, add_date, membership_status);

-- segment PDP impressions, only counting one impression/mpid/session
create or replace temporary table _segment_pdp_impressions as (
   select IFF(properties_is_bundle=TRUE, properties_bundle_product_id, properties_product_id) as impression_mpid,
        properties_is_bundle as impression_is_bundle,
        convert_timezone('America/Los_Angeles',TIMESTAMP)::Date as impression_date,
        coalesce(membership_status, 'other') as impression_membership,
        count(distinct(so.properties_session_ID)) as impressions
    from lake.segment_fl.javascript_fabletics_product_viewed so
    left join _identity i on so.USERID=i.user_id
    where convert_timezone('America/Los_Angeles',TIMESTAMP)::DATE >= $start_date
    -- the following membership bracket clause line is in UTC
    and ((TIMESTAMP>memb_start and TIMESTAMP<memb_end) or membership_status is null)
    and properties_AUTOMATED_TEST=FALSE and ifnull(i.test_account,'')<>'test'
    group by impression_mpid, impression_is_bundle, impression_date, membership_status);

-- order item quantities
CREATE OR REPLACE temporary table _snowflake_orders AS (
    select store_group,iff(ty.PRODUCT_TYPE_NAME = 'Bundle Component', f.BUNDLE_PRODUCT_ID, f.MASTER_PRODUCT_ID) as order_mpid,
    o.ORDER_LOCAL_DATETIME::timestamp::DATE AS order_date,
    case when MEMBERSHIP_ORDER_TYPE_L2 = 'Activating VIP' then 'lead'
        when MEMBERSHIP_ORDER_TYPE_L2 = 'Repeat VIP' then 'vip'
        else 'other' end AS order_membership,
    count(distinct o.order_id) as orders, sum(f.ITEM_QUANTITY) as quantity,
    iff(PRODUCT_TYPE_NAME='Bundle Component', true, false) as order_is_bundle
    from EDW_PROD.DATA_MODEL_FL.FACT_ORDER as o
    join EDW_PROD.DATA_MODEL_FL.DIM_ORDER_STATUS as dos on dos.ORDER_STATUS_KEY = o.ORDER_STATUS_KEY
    join EDW_PROD.DATA_MODEL_FL.DIM_ORDER_MEMBERSHIP_CLASSIFICATION as ch
      on ch.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = o.ORDER_MEMBERSHIP_CLASSIFICATION_KEY
    join EDW_PROD.DATA_MODEL_FL.DIM_ORDER_SALES_CHANNEL as sc on sc.ORDER_SALES_CHANNEL_KEY = o.ORDER_SALES_CHANNEL_KEY
    join EDW_PROD.DATA_MODEL_FL.DIM_STORE as ds on ds.STORE_ID = o.STORE_ID
    join EDW_PROD.DATA_MODEL_FL.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = o.CUSTOMER_ID --FL only
    -- order line
    join EDW_PROD.DATA_MODEL_FL.FACT_ORDER_LINE as f on f.ORDER_ID = o.ORDER_ID
    join EDW_PROD.DATA_MODEL_FL.DIM_PRODUCT_TYPE as ty on ty.PRODUCT_TYPE_KEY = f.PRODUCT_TYPE_KEY
    join EDW_PROD.DATA_MODEL_FL.DIM_ORDER_LINE_STATUS as os on os.ORDER_LINE_STATUS_KEY = f.ORDER_LINE_STATUS_KEY
    where ORDER_SALES_CHANNEL_L1 = 'Online Order'
    and ORDER_CLASSIFICATION_L1 = 'Product Order'
    and dos.order_status in ('Success', 'Pending')
    and os.ORDER_LINE_STATUS <> 'Cancelled'
    and ty.PRODUCT_TYPE_NAME in ('Normal', 'Bundle Component')
    and ds.store_type = 'Online'
    and order_date >= $start_date
    and ds.STORE_BRAND_ABBR in ('FL', 'YTY')
    group by order_mpid, order_date, order_membership, PRODUCT_TYPE_NAME, store_group);

-- pivot impressions and sales for membership types from rows to columns
create or replace temp table _pivot_impressions as (
    select * from (select impression_mpid, impression_date, impressions, impression_membership
    from _segment_pdp_impressions)
    pivot(sum(impressions) for impression_membership in ('other', 'lead', 'vip'))
    as p (mpid, date, total_impressions, lead_impressions, vip_impressions)
);
create or replace temp table _pivot_sales as (
    select * from (select order_mpid, order_date, quantity, order_membership from _snowflake_orders)
        pivot(sum(quantity) for order_membership in ('other', 'lead', 'vip'))
        as p (mpid, date, total_sales, lead_sales, vip_sales)
);
create or replace temp table _pivot_adds as (
    select * from (select add_mpid, add_date, adds, add_membership from _segment_add_to_cart)
    pivot(sum(adds) for add_membership in ('other', 'lead', 'vip'))
    as p (mpid, date, total_adds, lead_adds, vip_adds) where mpid!=''
);

-- join impressions and sales
create or replace temp table _pivot_all as (
    select coalesce(i.mpid, s.mpid) as mpid, coalesce(i.date, s.date) as date,
           zeroifnull(i.total_impressions) as total_impressions, zeroifnull(i.lead_impressions) as lead_impressions,
           zeroifnull(i.vip_impressions) as vip_impressions, zeroifnull(s.total_sales) as total_sales,
           zeroifnull(s.lead_sales) as lead_sales, zeroifnull(s.vip_sales) as vip_sales
    from _pivot_impressions i full outer join _pivot_sales s on i.mpid=s.mpid and i.date=s.date
);
-- join views and adds
create or replace temp table _pivot_all_adds as (
    select coalesce(i.mpid, s.mpid) as mpid, coalesce(i.date, s.date) as date,
           zeroifnull(i.total_impressions) as total_impressions, zeroifnull(i.lead_impressions) as lead_impressions,
           zeroifnull(i.vip_impressions) as vip_impressions, zeroifnull(s.total_adds) as total_sales,
           zeroifnull(s.lead_adds) as lead_sales, zeroifnull(s.vip_adds) as vip_sales
    from _pivot_impressions i full outer join _pivot_adds s on i.mpid=s.mpid and i.date=s.date
);

-- update total columns which only has membership=other counts at this point
update _pivot_all set total_impressions = total_impressions + lead_impressions + vip_impressions;
update _pivot_all set total_sales = total_sales + lead_sales + vip_sales;
update _pivot_all_adds set total_impressions = total_impressions + lead_impressions + vip_impressions;
update _pivot_all_adds set total_sales = total_sales + lead_sales + vip_sales;

-- structure data to match REPORTING.FABLETICS.POSTREG_SEGMENT_SPI_RANKING
create or replace temporary table _merge_with_department as (
    select $BRAND_ABBR as brand_abbr, s.store_country as country_abbr, p.department, m.mpid, m.date,
    $GRID_NAME as grid_name, p.image_url, p.category, sum(m.total_impressions) as total_impressions,
    sum(m.lead_impressions) as lead_impressions, sum(m.vip_impressions) as vip_impressions,
    sum(m.total_sales) as total_sales, sum(m.lead_sales) as lead_sales, sum(m.vip_sales) as vip_sales
    from _pivot_all m join EDW_PROD.DATA_MODEL_FL.DIM_PRODUCT p on m.mpid=p.PRODUCT_ID
    join EDW_PROD.DATA_MODEL_FL.dim_store s on s.store_id=p.store_id
    where p.DEPARTMENT in ('Mens', 'Womens', 'Shapewear - Womens', 'Scrubs Mens', 'Scrubs Womens')
    group by brand_abbr, country_abbr, department, mpid, date, grid_name, IMAGE_URL, category);

create or replace temporary table _merge_with_department_adds as (
    select $BRAND_ABBR as brand_abbr, s.store_country as country_abbr, p.department, m.mpid, m.date,
    'pdp_view_add' as grid_name, p.image_url, p.category, sum(m.total_impressions) as total_impressions,
    sum(m.lead_impressions) as lead_impressions, sum(m.vip_impressions) as vip_impressions,
    sum(m.total_sales) as total_sales, sum(m.lead_sales) as lead_sales, sum(m.vip_sales) as vip_sales
    from _pivot_all_adds m join EDW_PROD.DATA_MODEL_FL.DIM_PRODUCT p on m.mpid=p.PRODUCT_ID
    join EDW_PROD.DATA_MODEL_FL.dim_store s on s.store_id=p.store_id
    where p.DEPARTMENT in ('Mens', 'Womens', 'Shapewear - Womens', 'Scrubs Mens', 'Scrubs Womens')
    group by brand_abbr, country_abbr, department, mpid, date, grid_name, IMAGE_URL, category);

-- get rid of a few records not in required departments
create or replace temporary table _final_merge as (select * from _merge_with_department
union select * from _merge_with_department_adds);

/*
-- check totals
-- totals by country
select country_abbr,count(distinct(mpid)) as mpids,sum(lead_sales) as lead_sales,sum(vip_sales) as vip_sales,sum(total_sales) as total_sales,
       sum(lead_impressions) as lead_impressions,sum(vip_impressions) as vip_impressions,sum(total_impressions) as total_impression,
        div0(sum(lead_sales),sum(lead_impressions)) as lead_spv,div0(sum(vip_sales),sum(vip_impressions)) as vip_spv,div0(sum(total_sales),sum(total_impressions)) as total_spv
from _merge_with_department group by country_abbr;
-- initial impression totals
select sum(impressions), impression_membership from _segment_pdp_impressions group by impression_membership
union select sum(impressions), 'total' from _segment_pdp_impressions;
-- initial order item quantity totals
select sum(quantity), order_membership from _snowflake_orders group by order_membership
union select sum(quantity), 'total' from _snowflake_orders;
-- final totals
select sum(total_impressions), sum(lead_impressions), sum(vip_impressions),
sum(total_sales), sum(lead_sales), sum(vip_sales) from _merge_with_department;
-- breakout totals
select department, date, sum(lead_impressions) as lead_impressions, sum(lead_sales) as lead_sales,
to_double(sum(lead_sales))/to_double(sum(lead_impressions)) as lead_spv,
sum(vip_impressions) as vip_impressions, sum(vip_sales) as vip_sales,
to_double(sum(vip_sales))/to_double(sum(vip_impressions)) as vip_spv,
sum(total_impressions) as total_impressions, sum(total_sales) as total_sales,
to_double(sum(total_sales))/to_double(sum(total_impressions)) as total_spv
from _final_merge group by department, date order by department,date;
*/

-- merge into snowflake table
merge into REPORTING_PROD.DATA_SCIENCE.EXPORT_POSTREG_SEGMENT_SPI_RANKING t
using _final_merge m
on t.mpid = m.mpid and t.impressions_date = m.date and t.grid_name = m.grid_name
when not matched then
    insert (brand_abbr, country_abbr, department,
            mpid, impressions_date, grid_name,
            total_impressions, lead_impressions, vip_impressions,
            total_sales, lead_sales, vip_sales,  META_CREATE_DATETIME, META_UPDATE_DATETIME, META_COMPANY_ID)
    values (BRAND_ABBR, SUBSTR(COUNTRY_ABBR, 1, 2), DEPARTMENT,
            mpid, date, grid_name,
            total_impressions, lead_impressions, vip_impressions,
            total_sales, lead_sales, vip_sales, current_timestamp(), current_timestamp(), 20)
when matched and
    (
        not equal_null(t.total_impressions, m.total_impressions) OR
        not equal_null(t.lead_impressions, m.lead_impressions) OR
        not equal_null(t.vip_impressions, m.vip_impressions) OR
        not equal_null(t.total_sales, m.total_sales) OR
        not equal_null(t.lead_sales, m.lead_sales) OR
        not equal_null(t.vip_sales , m.vip_sales)
    )
    then update
    set t.total_impressions = m.total_impressions,
        t.lead_impressions = m.lead_impressions,
        t.vip_impressions = m.vip_impressions,
        t.total_sales = m.total_sales,
        t.lead_sales = m.lead_sales,
        t.vip_sales = m.vip_sales,
        t.meta_update_datetime = current_timestamp();
