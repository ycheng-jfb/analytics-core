set data_update_days = 3;
set start_date = dateadd(day, -$data_update_days, current_timestamp())::DATE;
set BRAND_ABBR = 'JF';

-- membership_status history to bracket impression/order time for appropriate membership_status at time
CREATE OR REPLACE TEMPORARY TABLE _identity as (
    select
        userid as user_id,
        to_timestamp(Timestamp) as memb_start,
        case when lower(TRAITS_MEMBERSHIP_STATUS) = 'lead' then 'lead'
            when lower(TRAITS_MEMBERSHIP_STATUS) in ('vip', 'vip elite', 'sd classic vip', 'vip classic','elite') then 'vip'
            else 'other' end as membership_status,
        IFF(TRAITS_EMAIL like '%@test.com' or TRAITS_EMAIL like '%@example.com', 'test', '') as test_account,
        coalesce(lead(TIMESTAMP) over (partition by userid order by TIMESTAMP), '9999-12-31') as memb_end
    from lake.segment_gfb.JAVASCRIPT_JUSTFAB_IDENTIFY
    WHERE user_id IS NOT NULL
        AND user_id > 0);


-- seems to take really long time if joins are in here
create or replace temporary table _raw_impressions as (
    select distinct iff(properties_products_is_bundle=True,
    cast(IFF(properties_products_bundle_product_id::string = '', '0', properties_products_bundle_product_id::string) as bigint),
    cast(IFF(properties_products_product_id::string = '', '0', properties_products_product_id::string) as bigint)) as impression_mpid,
    properties_products_is_bundle as is_bundle,
    iff(context_page_url ilike '%boutique_post_registration%', 'boutique_post_registration', properties_list_id) as list_id,
    convert_timezone('UTC','America/Los_Angeles',to_timestamp(TIMESTAMP)) as impression_timestamp,
    properties_session_id as session_id, USERID as user_id
    from lake.segment_gfb.javascript_justfab_PRODUCT_LIST_VIEWED so
    where properties_AUTOMATED_TEST=FALSE and impression_timestamp::date >= $start_date);

-- bracket membership information and rollup totals
create or replace temporary table _breakout_impressions_brand_id as (
    select impression_mpid, impression_timestamp::date as impression_date, count(*) as impressions,
    coalesce(membership_status, 'other') as impression_membership, list_id
    from _raw_impressions ri
    left join _identity i on ri.USER_ID=i.user_id
    and ((impression_timestamp>convert_timezone('UTC','America/Los_Angeles',memb_start)
    and impression_timestamp<convert_timezone('UTC','America/Los_Angeles',memb_end)) or membership_status is null)
    and ifnull(i.test_account,'')<>'test'
    group by impression_mpid, impression_date, list_id, membership_status);

-- these take up a lot of RAM
drop table _raw_impressions;
drop table _identity;

-- collapse membership_brand_id
create or replace temporary table _breakout_impressions as (
    select impression_mpid, impression_date, sum(impressions) as impressions, impression_membership,
           coalesce(list_id, '') as list_id
    from _breakout_impressions_brand_id group by impression_mpid, impression_date, impression_membership, list_id);

-- order item quantities
CREATE OR REPLACE temporary table _snowflake_orders AS (
    select iff(ty.PRODUCT_TYPE_NAME = 'Bundle Component', f.BUNDLE_PRODUCT_ID, f.MASTER_PRODUCT_ID) as order_mpid,
    o.ORDER_LOCAL_DATETIME::timestamp::DATE AS order_date,
    coalesce(ps.value, '') as psource,
    case when MEMBERSHIP_ORDER_TYPE_L2 = 'Activating VIP' then 'lead'
        when MEMBERSHIP_ORDER_TYPE_L2 = 'Repeat VIP' then 'vip'
        else 'other' end AS order_membership,
    count(distinct o.order_id) as orders, sum(f.ITEM_QUANTITY) as sales,
    iff(PRODUCT_TYPE_NAME='Bundle Component', true, false) as order_is_bundle
    from EDW_PROD.DATA_MODEL_JFB.FACT_ORDER as o
    join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_STATUS as dos on dos.ORDER_STATUS_KEY = o.ORDER_STATUS_KEY
    join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_MEMBERSHIP_CLASSIFICATION as ch
      on ch.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = o.ORDER_MEMBERSHIP_CLASSIFICATION_KEY
    join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_SALES_CHANNEL as sc on sc.ORDER_SALES_CHANNEL_KEY = o.ORDER_SALES_CHANNEL_KEY
    join EDW_PROD.DATA_MODEL_JFB.DIM_STORE as ds on ds.STORE_ID = o.STORE_ID
    join EDW_PROD.DATA_MODEL_JFB.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = o.CUSTOMER_ID --FL only
    -- order line
    join EDW_PROD.DATA_MODEL_JFB.FACT_ORDER_LINE as f on f.ORDER_ID = o.ORDER_ID
    join EDW_PROD.DATA_MODEL_JFB.DIM_PRODUCT_TYPE as ty on ty.PRODUCT_TYPE_KEY = f.PRODUCT_TYPE_KEY
    join EDW_PROD.DATA_MODEL_JFB.DIM_ORDER_LINE_STATUS as os on os.ORDER_LINE_STATUS_KEY = f.ORDER_LINE_STATUS_KEY
    left join LAKE_JFB_VIEW.ULTRA_MERCHANT.ORDER_PRODUCT_SOURCE as ps on ps.ORDER_ID = f.ORDER_ID
        and ps.PRODUCT_ID=iff(ty.PRODUCT_TYPE_NAME = 'Bundle Component', f.BUNDLE_PRODUCT_ID, f.PRODUCT_ID)
    where ORDER_SALES_CHANNEL_L1 = 'Online Order'
    and ORDER_CLASSIFICATION_L1 = 'Product Order'
    and dos.order_status in ('Success', 'Pending')
    and os.ORDER_LINE_STATUS <> 'Cancelled'
    and ty.PRODUCT_TYPE_NAME in ('Normal', 'Bundle Component')
    and ds.store_type = 'Online'
    and order_date >= $start_date
    and ds.STORE_BRAND_ABBR in ('JF')
    group by order_mpid, order_date, order_membership, PRODUCT_TYPE_NAME, psource);


-- pivot impressions and sales for membership types from rows to columns
create or replace temp table _pivot_impressions as (
    select * from (select impression_mpid, impression_date, list_id, impressions, impression_membership
    from _breakout_impressions)
    pivot(sum(impressions) for impression_membership in ('other', 'lead', 'vip'))
    as p (impressions_mpid, impressions_date, list_id, total_impressions, lead_impressions, vip_impressions));

create or replace temp table _pivot_sales as (
    select * from (select order_mpid, order_date, psource, sales, order_membership from _snowflake_orders)
        pivot(sum(sales) for order_membership in ('other', 'lead', 'vip'))
        as p (order_mpid, order_date, psource, total_sales, lead_sales, vip_sales));

-- join impressions and sales
create or replace temp table _pivot_all as (
    select coalesce(i.impressions_mpid, s.order_mpid) as mpid, coalesce(i.impressions_date, s.order_date) as date,
           coalesce(i.list_id, s.psource, '') as grid_name,
           zeroifnull(i.total_impressions) as total_impressions, zeroifnull(i.lead_impressions) as lead_impressions,
           zeroifnull(i.vip_impressions) as vip_impressions, zeroifnull(s.total_sales) as total_sales,
           zeroifnull(s.lead_sales) as lead_sales, zeroifnull(s.vip_sales) as vip_sales
    from _pivot_impressions i full outer join _pivot_sales s
    on i.impressions_mpid=s.order_mpid and i.impressions_date=s.order_date and i.list_id = s.psource);

-- update total columns which only has membership=other counts at this point
update _pivot_all set total_impressions = total_impressions + lead_impressions + vip_impressions;
update _pivot_all set total_sales = total_sales + lead_sales + vip_sales;

-- get rid of grids not to be counted: upsell, interstitials, pre-order modules
-- how do we keep this list updated?!

-- Need to figure out what grid_name will be and what we categories count as upsell, interstitials, pre-order modules
create or replace temp table _pivot_all_filtered as (
select * from _pivot_all where NOT grid_name ILIKE ANY ('productdetail_relateditems', 'productdetail_shoptheoutfit',
'browse_cart_offer%', 'myaccount_favorites', 'browse:sale%', 'skip_request')
and grid_name!='' and not contains(grid_name, 'interstitial'));

-- create grid_name all, postreg
create or replace temp table _grid_postreg as(
select mpid, date, 'postreg' as grid_name, sum(total_impressions) as total_impressions,
sum(lead_impressions) as lead_impressions, sum(vip_impressions) as vip_impressions, sum(total_sales) as total_sales,
sum(lead_sales) as lead_sales, sum(vip_sales) as vip_sales from _pivot_all_filtered
where contains(grid_name, 'boutique_post_registration')
group by mpid, date);

create or replace temp table _grid_all as(
select mpid, date, 'all' as grid_name, sum(total_impressions) as total_impressions,
sum(lead_impressions) as lead_impressions, sum(vip_impressions) as vip_impressions, sum(total_sales) as total_sales,
sum(lead_sales) as lead_sales, sum(vip_sales) as vip_sales from _pivot_all_filtered
group by mpid, date);

create or replace temporary table _merge as ((select * from _grid_postreg) union (select * from _grid_all));

create or replace temporary table _merge_with_department as (
    select $BRAND_ABBR as brand_abbr, s.STORE_COUNTRY as country_abbr, p.department, m.mpid, m.date, m.grid_name,p.image_url, p.category,
           sum(m.total_impressions) as total_impressions, sum(m.lead_impressions) as lead_impressions,
           sum(m.vip_impressions) as vip_impressions, sum(m.total_sales) as total_sales,
           sum(m.lead_sales) as lead_sales, sum(m.vip_sales) as vip_sales
    from _merge m join EDW_PROD.DATA_MODEL_JFB.DIM_PRODUCT p on m.mpid=p.PRODUCT_ID
    left join EDW_PROD.DATA_MODEL_JFB.dim_store s on s.store_id=p.store_id
    where p.DEPARTMENT in ('Shoes', 'Bags & Accessories', 'Apparel', 'Clothing')
    group by s.store_country, p.department, m.mpid, m.date, m.grid_name,p.image_url, p.category);

--SELECT DISTINCT DEPARTMENT FROM edw.DATA_MODEL.DIM_PRODUCT p WHERE STORE_ID = 26 LIMIT 100;
--SELECT * FROM edw.DATA_MODEL.DIM_STORE DS WHERE ds.STORE_BRAND_ABBR in ('JF') LIMIT 100;

--Filter out the fabkids entries we were getting
create or replace temporary table _merge_with_department_filtered as(
    SELECT * FROM _merge_with_department WHERE NOT image_url ILIKE ('%fabkids%')
);

merge into REPORTING_PROD.DATA_SCIENCE.EXPORT_POSTREG_SEGMENT_SPI_RANKING t
using _merge_with_department_filtered m
on t.mpid = m.mpid and t.impressions_date = m.date and t.grid_name = m.grid_name
when not matched then
    insert (brand_abbr, country_abbr, department,
            mpid, impressions_date, grid_name,
            total_impressions, lead_impressions, vip_impressions,
            total_sales, lead_sales, vip_sales,  META_CREATE_DATETIME, META_UPDATE_DATETIME, META_COMPANY_ID)
    values (BRAND_ABBR, COUNTRY_ABBR, DEPARTMENT,
            mpid, date, grid_name,
            total_impressions, lead_impressions, vip_impressions,
            total_sales, lead_sales, vip_sales, current_timestamp(), current_timestamp(), 10)
when matched and
    (
        not equal_null(t.total_impressions, m.total_impressions) OR
        not equal_null(t.lead_impressions, m.lead_impressions) OR
        not equal_null(t.vip_impressions, m.vip_impressions) OR
        not equal_null(t.total_sales, m.total_sales) OR
        not equal_null(t.lead_sales, m.lead_sales) OR
        not equal_null(t.vip_sales , m.vip_sales)
    )
    then
    update
    set t.total_impressions = m.total_impressions,
        t.lead_impressions = m.lead_impressions,
        t.vip_impressions = m.vip_impressions,
        t.total_sales = m.total_sales,
        t.lead_sales = m.lead_sales,
        t.vip_sales = m.vip_sales,
        t.meta_update_datetime = current_timestamp();
