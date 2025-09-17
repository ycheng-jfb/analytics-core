
--- real-time acquisition and order metrics
-- two different reporting tables: one for acquisition and one for order metrics
-- historical data is pulled from edw. real-time is pulled from source for the last two days

set low_watermark_date = dateadd(day, -45, dateadd(year, -2, current_date()));
set low_watermark_date_rt = dateadd(day,-1,current_date());

------------------------------------------------------------------------------------
-- real-time customer mappings (used across acquisition and order metrics) --

create or replace temporary table _fl_gender as
select distinct cd.customer_id
from lake_fl_view.ultra_merchant.customer_detail cd
join lake_fl_view.ultra_merchant.membership m
    on m.customer_id = cd.customer_id
join edw_prod.data_model.dim_store st
    on st.store_id = m.store_id
where
    name ILIKE 'gender'
    and value ILIKE 'm'
    and st.store_brand in ('Fabletics','Yitty')
    and m.datetime_added >= '2020-01-01';

create or replace temp table _is_fl_scrubs_customer_base as
select
    cd.customer_detail_id,
    cd.customer_id,
    cd.name,
    cd.value,
    cd.datetime_modified,
    cd.datetime_added
from lake_fl.ultra_merchant.customer_detail as cd
where cd.name ILIKE 'isscrubs';

create or replace temp table _is_fl_scrubs_customer as
select cd.customer_id,
       cd.datetime_added as scrubs_registration_datetime
from _is_fl_scrubs_customer_base  cd
where cd.name ILIKE 'isscrubs'
  and value = 1
qualify row_number() over (partition by cd.customer_id order by cd.datetime_added desc) = 1;

create or replace temporary table _fk_free_trial as
select distinct cd.customer_id
from lake_jfb_view.ultra_merchant.customer_detail cd
join lake_jfb_view.ultra_merchant.membership m on m.customer_id = cd.customer_id
join edw_prod.data_model.dim_store st on st.store_id = m.store_id
where
    (name ilike 'origin'
    and value ilike '%free%'
    and st.store_brand = 'FabKids')
or
    (name = 'funnel'
    and lower(value) = 'freetrialcrosspromo'
    and st.store_brand = 'FabKids');

create or replace temp table _orders AS
select o.order_id,
       processing_statuscode,
       payment_statuscode,
       date_shipped,
       datetime_shipped,
       o.datetime_added,
       customer_id,
       subtotal,
       od.product_discount AS discount,
       iff(token_count>0,1,0) as token_order_flag,
       shipping,
       shipping_address_id,
       store_id,
       membership_brand_id
from lake_fl.ultra_merchant."ORDER" o
left join (select order_id, sum(amount) as product_discount from lake_fl.ultra_merchant.order_discount where applied_to = 'subtotal' group by 1) od on o.order_id = od.order_id
left join (select order_id, count(*) as token_count from lake_fl.ULTRA_MERCHANT.order_line_token group by 1) olt on o.order_id = olt.order_id
where o.datetime_added >= $low_watermark_date_rt
union all
select o.order_id,
       processing_statuscode,
       payment_statuscode,
       date_shipped,
       datetime_shipped,
       o.datetime_added,
       customer_id,
       subtotal,
       od.product_discount as discount,
       iff(token_count>0,1,0) as token_order_flag,
       shipping,
       shipping_address_id,
       store_id,
       membership_brand_id
from lake_jfb.ultra_merchant."ORDER" o
left join (select order_id, sum(amount) as product_discount from lake_jfb.ultra_merchant.order_discount where applied_to = 'subtotal' group by 1) od on o.order_id = od.order_id
left join (select order_id, count(*) as token_count from lake_jfb.ultra_merchant.order_line_token group by 1) olt on o.order_id = olt.order_id
where datetime_added >= $low_watermark_date_rt
union all
select o.order_id,
       processing_statuscode,
       payment_statuscode,
       date_shipped,
       datetime_shipped,
       o.datetime_added,
       customer_id,
       subtotal,
       od.product_discount as discount,
       iff(token_count>0,1,0) as token_order_flag,
       shipping,
       shipping_address_id,
       store_id,
       membership_brand_id
from lake_sxf.ultra_merchant."ORDER" o
left join (select order_id, sum(amount) as product_discount from lake_sxf.ultra_merchant.order_discount where applied_to = 'subtotal' group by 1) od on o.order_id = od.order_id
left join (select order_id, count(*) as token_count from lake_sxf.ultra_merchant.order_line_token group by 1) olt on o.order_id = olt.order_id
where datetime_added >= $low_watermark_date_rt;

create or replace temp table _order_detail as
select order_id,
   try_to_number(value) as retail_store_id,
   name
from lake_fl.ultra_merchant.order_detail
where name = 'retail_store_id'
    AND datetime_added >= $low_watermark_date_rt
UNION ALL
select order_id,
   try_to_number(value) as retail_store_id,
   name
from lake_sxf.ultra_merchant.order_detail
where name = 'retail_store_id'
    AND datetime_added >= $low_watermark_date_rt
UNION ALL
select order_id,
   try_to_number(value) as retail_store_id,
   name
from lake_jfb.ultra_merchant.order_detail
where name = 'retail_store_id'
    AND datetime_added >= $low_watermark_date_rt;

create or replace temporary table _retail_vips as
WITH membership AS (
    SELECT
        customer_id,
        datetime_activated,
        order_id
    from lake_fl.ultra_merchant_history.membership
    where datetime_activated >= $low_watermark_date_rt
    UNION ALL
        SELECT
        customer_id,
        datetime_activated,
        order_id
    from lake_jfb.ultra_merchant_history.membership
    where datetime_activated >= $low_watermark_date_rt
    UNION ALL
    SELECT
        customer_id,
        datetime_activated,
        order_id
    from lake_sxf.ultra_merchant_history.membership
    where datetime_activated >= $low_watermark_date_rt
    )
select distinct
    m.customer_id,
    m.datetime_activated
from membership m
join _orders o
    on o.order_id = m.order_id
join _order_detail od
    on od.order_id = o.order_id;

create or replace temporary table _membership as
SELECT customer_id,
    datetime_added
from lake_fl_view.ultra_merchant.membership

UNION ALL
SELECT customer_id,
    datetime_added
from lake_jfb_view.ultra_merchant.membership

UNION ALL
SELECT customer_id,
    datetime_added
from lake_sxf_view.ultra_merchant.membership;

create or replace temporary table _customer as
select
    customer_id,
    email,
    datetime_added
from lake_fl_view.ultra_merchant.customer

UNION ALL
select
    customer_id,
    email,
    datetime_added
from lake_jfb_view.ultra_merchant.customer

UNION ALL
select
    customer_id,
    email,
    datetime_added
from lake_sxf_view.ultra_merchant.customer;

create or replace temp table _customer_detail as
select
    customer_id,
    name,
    value
from lake_fl_view.ultra_merchant.customer_detail
where name ILIKE 'retail_location'
    and value ILIKE 'retail'
UNION ALL
select
    customer_id,
    name,
    value
from lake_jfb_view.ultra_merchant.customer_detail
where name ILIKE 'retail_location'
    and value ILIKE 'retail'
UNION ALL
select
    customer_id,
    name,
    value
from lake_sxf_view.ultra_merchant.customer_detail
where (name ILIKE 'retail_location' and value ILIKE 'retail')
    or (name ILIKE 'country_code' and value ILIKE 'CA')
;


create or replace temporary table _retail_leads as
select distinct
    cd.customer_id,
    c.email
from _customer_detail cd
join _customer c
    on cd.customer_id = c.customer_id
join _membership m
    on m.customer_id = cd.customer_id
where cd.name ILIKE 'retail_location'
    and cd.value ILIKE 'retail'
    and not (email like any ('%%@retail.fabletics%%', '%%@retail.savagex%%'));

-- ACQUISITION
------------------------------------------------------------------------------------
-- historical acquisition metrics from edw --

create or replace temporary table _edw_leads as
select
    st.store_brand as brand,
    st.store_region as region,
    IFF(st.store_brand_abbr='SX' and dc.finance_specialty_store='CA', 'CA', st.store_country) as country,
    st.store_brand || ' ' || country as store_name,

    case when st.store_brand = 'Fabletics' and dc.gender = 'M' then 'Male'
         else 'Female' end as gender,
    cast(iff(store_brand ILIKE 'fabletics' and lower(is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,
    case when store_brand = 'FabKids' and dc.is_cross_promo = true then 'FK Free Trial'
        when store_brand = 'FabKids' then 'Not FK Free Trial' else 'N/A' end as fk_free_trial,
    cast(iff(store_brand ILIKE 'yitty',0,iff(is_retail_registration = true, 1, 0)) as boolean) as retail_lead,
    cast(0 as boolean) as retail_vip,

    date_trunc(hour,fr.registration_local_datetime::timestamp_ntz) as date_hour,
    count(*) as leads,
    count(iff(is_secondary_registration = false,1,null)) as primary_leads,
    count(iff(is_secondary_registration = true,1,null)) as secondary_leads
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.dim_customer dc
    on dc.customer_id = fr.customer_id
join edw_prod.data_model.dim_store st
    on st.store_id = fr.store_id
where fr.registration_local_datetime::timestamp_ntz < $low_watermark_date_rt
    and fr.registration_local_datetime::timestamp_ntz >= $low_watermark_date
    and ifnull(fr.is_fake_retail_registration, false) = false
group by 1,2,3,4,5,6,7,8,9,10;


create or replace temporary table _edw_vips as
select
    st.store_brand as brand,
    st.store_region as region,
    IFF(st.store_brand_abbr='SX' and dc.finance_specialty_store='CA', 'CA', st.store_country) as country,
    st.store_brand || ' ' || country as store_name,

    case when st.store_brand = 'Fabletics' and dc.gender = 'M' then 'Male'
         else 'Female' end as gender,
    cast(iff(store_brand ILIKE 'fabletics' and lower(is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,
    case when store_brand = 'FabKids' and dc.is_cross_promo = true then 'FK Free Trial'
        when store_brand = 'FabKids' then 'Not FK Free Trial' else 'N/A' end as fk_free_trial,
    cast(iff(store_brand ILIKE 'yitty', 0, iff(l.is_retail_registration = true, 1, 0)) as boolean) as retail_lead,
    cast(iff(is_retail_vip = true, 1,0) as boolean) as retail_vip,

    date_trunc(hour,fa.activation_local_datetime::timestamp_ntz) as date_hour,
    count(*) as vips
from edw_prod.data_model.fact_activation fa
join edw_prod.data_model.fact_registration l
    on fa.customer_id = l.customer_id
    and ifnull(is_fake_retail_registration, false) = false
    and ifnull(l.is_secondary_registration, false) = false
join edw_prod.data_model.dim_customer dc
    on dc.customer_id = fa.customer_id
join edw_prod.data_model.dim_store st
    on st.store_id = fa.store_id
where fa.activation_local_datetime::timestamp_ntz < $low_watermark_date_rt
    and fa.activation_local_datetime::timestamp_ntz >= $low_watermark_date
group by 1,2,3,4,5,6,7,8,9,10;

create or replace temporary table _edw_d1_60m_vips_from_leads as
select
    st.store_brand as brand,
    st.store_region as region,
    IFF(st.store_brand_abbr='SX' and dc.finance_specialty_store='CA', 'CA', st.store_country) as country,
    st.store_brand || ' ' || country as store_name,

    case when st.store_brand = 'Fabletics' and dc.gender = 'M' then 'Male'
         else 'Female' end as gender,
    cast(iff(store_brand ILIKE 'fabletics' and lower(is_scrubs_customer) = true, 1, 0) as boolean) as is_fl_scrubs_customer,
    case when store_brand = 'FabKids' and dc.is_cross_promo = true then 'FK Free Trial'
        when store_brand = 'FabKids' then 'Not FK Free Trial' else 'N/A' end as fk_free_trial,
    cast(iff(store_brand ILIKE 'yitty', 0, iff(fr.is_retail_registration = true, 1, 0)) as boolean) as retail_lead,
    cast(iff(is_retail_vip = true, 1,0) as boolean) as retail_vip,

    date_trunc(hour,fa.activation_local_datetime::timestamp_ntz) as date_hour,
    count(iff(datediff(day,dc.registration_local_datetime,fa.activation_local_datetime) = 0,1,null)) as d1_vips_from_leads_at_lead_hour,
    count(iff(datediff(minute,dc.registration_local_datetime,fa.activation_local_datetime) < 60,1,null)) as m60_vips_from_leads_at_lead_hour
from edw_prod.data_model.fact_registration fr
join edw_prod.data_model.fact_activation fa
    on fa.customer_id = fr.customer_id
    and fa.store_id = fr.store_id
join edw_prod.data_model.dim_customer dc
    on dc.customer_id = fa.customer_id
join edw_prod.data_model.dim_store st
    on st.store_id = fa.store_id
where
    ifnull(fr.is_secondary_registration, false) = false
    and fr.registration_local_datetime::timestamp_ntz < $low_watermark_date_rt
    and fr.registration_local_datetime::timestamp_ntz >= $low_watermark_date
    and (datediff(day,dc.registration_local_datetime,activation_local_datetime) = 0
        or datediff(minute,dc.registration_local_datetime,activation_local_datetime) < 60)
    and ifnull(fr.is_fake_retail_registration, false) = false
group by 1,2,3,4,5,6,7,8,9,10;

------------------------------------------------------------------------------------
-- real-time acquisition metrics --

create or replace temporary table _realtime_vips as
select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    'Female' as gender,
    FALSE as is_fl_scrubs_customer,
    case when fk.customer_id is not null then 'FK Free Trial'
        when ds.store_brand = 'FabKids' and fk.customer_id is null then 'Not FK Free Trial'
    else 'N/A' end as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(iff(rv.customer_id is not null, 1,0) as boolean) as retail_vip,
    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_activated)),date_trunc('hour',m.datetime_activated)) date_hour,
    count(1) as vips,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_jfb_view.ultra_merchant.membership m
join lake_jfb_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _fk_free_trial fk
    on fk.customer_id = c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _retail_vips rv
    on rv.customer_id = c.customer_id
    and rv.datetime_activated=m.datetime_activated
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_activated)),date_trunc('hour',m.datetime_activated)
        ) >= $low_watermark_date_rt
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_brand in ('JustFab', 'ShoeDazzle','FabKids')
group by 1,2,3,4,5,6,7,8,9,10

UNION ALL
select
    ds.store_brand as brand,
    ds.store_region as region,
    IFF(cd.value='CA', 'CA', ds.store_country) as country,
    ds.store_brand || ' ' || country as store_name,

    'Female' as gender,
    FALSE as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(iff(rv.customer_id is not null, 1,0) as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_activated)),date_trunc('hour',m.datetime_activated)) date_hour,
    count(1) as vips,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_sxf_view.ultra_merchant.membership m
join lake_sxf_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _retail_vips rv
    on rv.customer_id = c.customer_id
    and rv.datetime_activated=m.datetime_activated
left join _customer_detail cd on cd.customer_id = c.customer_id and cd.name='country_code'
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_activated)),date_trunc('hour',m.datetime_activated)
        ) >= $low_watermark_date_rt
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_brand = 'Savage X'
group by 1,2,3,4,5,6,7,8,9,10

UNION ALL
select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    case when fg.customer_id is not null then 'Male' else 'Female' end as gender,
    cast(iff(sc.customer_id is not null
        and cast(scrubs_registration_datetime as date) = cast(coalesce(m.datetime_added, c.datetime_added) as date),1,0) as boolean)
    as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(iff(rv.customer_id is not null, 1,0) as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_activated)),date_trunc('hour',m.datetime_activated)) date_hour,
    count(1) as vips,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_fl_view.ultra_merchant.membership m
join lake_fl_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _fl_gender fg
    on fg.customer_id = c.customer_id
left join _is_fl_scrubs_customer sc
    on sc.customer_id = c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _retail_vips rv
    on rv.customer_id = c.customer_id
    and rv.datetime_activated=m.datetime_activated
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_activated)),date_trunc('hour',m.datetime_activated)
        ) >= $low_watermark_date_rt
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_name not in ('Fabletics US','Yitty US')
    and ds.store_brand = 'Fabletics'
group by 1,2,3,4,5,6,7,8,9,10

union all
select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    case when fg.customer_id is not null then 'Male' else 'Female' end as gender,
    cast(iff(sc.customer_id is not null
        and cast(scrubs_registration_datetime as date) = cast(coalesce(me.datetime_added, c.datetime_added) as date),1,0) as boolean)
    as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(store_brand ILIKE 'yitty', 0, iff(rl.customer_id is not null,1,0)) as boolean) as retail_lead,
    cast(iff(rv.customer_id is not null, 1,0) as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_activated)),date_trunc('hour',m.datetime_activated)) date_hour,
    count(1) as vips,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_fl_view.ultra_merchant.membership_brand_activation m
join lake_fl_view.ultra_merchant.membership me
    on me.membership_id = m.membership_id
join lake_fl_view.ultra_merchant.customer c
    on c.customer_id = me.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = case
        when m.membership_brand_id = 1 then 52
        when m.membership_brand_id = 2 then 241 end
left join _fl_gender fg
    on fg.customer_id = c.customer_id
left join _is_fl_scrubs_customer sc
    on sc.customer_id = c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _retail_vips rv
    on rv.customer_id = c.customer_id
    and rv.datetime_activated = m.datetime_activated
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_activated)),date_trunc('hour',m.datetime_activated)
        ) >= $low_watermark_date_rt
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_name in ('Fabletics US','Yitty US')
group by 1,2,3,4,5,6,7,8,9,10;


create or replace temporary table _realtime_d1_vips_from_leads_at_lead_hour as
select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    'Female' as gender,
    FALSE as is_fl_scrubs_customer,
    case when fk.customer_id is not null then 'FK Free Trial'
        when ds.store_brand = 'FabKids' and fk.customer_id is null then 'Not FK Free Trial'
    else 'N/A' end as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(iff(rv.customer_id is not null, 1,0) as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)) date_hour, -- lead registration time
    count(iff(datediff('day', m.datetime_added, m.datetime_activated) = 0,1,null)) as d1_vips_from_leads_at_lead_hour,
    count(iff(datediff('minute', m.datetime_added, m.datetime_activated) + 1 <= 60,1,null)) as m60_vips_from_leads_at_lead_hour,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_jfb_view.ultra_merchant.membership m
join lake_jfb_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _fk_free_trial fk
    on fk.customer_id = c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _retail_vips rv
    on rv.customer_id = c.customer_id
    and rv.datetime_activated = m.datetime_activated
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)
        ) >= $low_watermark_date_rt
    and (datediff('day',m.datetime_added,m.datetime_activated) = 0
        or datediff('minute',m.datetime_added,m.datetime_activated)+1 <= 60)
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_brand in ('JustFab', 'ShoeDazzle','FabKids')
group by 1,2,3,4,5,6,7,8,9,10

union all

select
    ds.store_brand as brand,
    ds.store_region as region,
    IFF(cd.value='CA', 'CA', ds.store_country) as country,
    ds.store_brand || ' ' || country as store_name,

    'Female' as gender,
    FALSE as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(iff(rv.customer_id is not null, 1,0) as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)) date_hour, -- lead registration time
    count(iff(datediff('day', m.datetime_added, m.datetime_activated) = 0,1,null)) as d1_vips_from_leads_at_lead_hour,
    count(iff(datediff('minute', m.datetime_added, m.datetime_activated) + 1 <= 60,1,null)) as m60_vips_from_leads_at_lead_hour,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_sxf_view.ultra_merchant.membership m
join lake_sxf_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _retail_vips rv
    on rv.customer_id = c.customer_id
    and rv.datetime_activated = m.datetime_activated
left join _customer_detail cd on cd.customer_id = c.customer_id and cd.name='country_code'
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)
        ) >= $low_watermark_date_rt
    and (datediff('day',m.datetime_added,m.datetime_activated) = 0
        or datediff('minute',m.datetime_added,m.datetime_activated)+1 <= 60)
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_brand = 'Savage X'
group by 1,2,3,4,5,6,7,8,9,10

union all

select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    case when fg.customer_id is not null then 'Male' else 'Female' end as gender,
    FALSE as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(iff(rv.customer_id is not null, 1,0) as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)) date_hour, -- lead registration time
    count(iff(datediff('day', m.datetime_added, m.datetime_activated) = 0,1,null)) as d1_vips_from_leads_at_lead_hour,
    count(iff(datediff('minute', m.datetime_added, m.datetime_activated) + 1 <= 60,1,null)) as m60_vips_from_leads_at_lead_hour,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_fl_view.ultra_merchant.membership m
join lake_fl_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _fl_gender fg
    on fg.customer_id = c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _retail_vips rv
    on rv.customer_id = c.customer_id
    and rv.datetime_activated = m.datetime_activated
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)
        ) >= $low_watermark_date_rt
    and (datediff('day',m.datetime_added,m.datetime_activated) = 0
        or datediff('minute',m.datetime_added,m.datetime_activated)+1 <= 60)
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and store_name not in ('Fabletics US','Yitty US')
    and ds.store_brand = 'Fabletics'
group by 1,2,3,4,5,6,7,8,9,10

union all

select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    case when fg.customer_id is not null then 'Male' else 'Female' end as gender,
    cast(iff(sc.customer_id is not null
        and cast(scrubs_registration_datetime as date) = cast(coalesce(m.datetime_added, c.datetime_added) as date),1,0) as boolean)
    as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(store_brand ILIKE 'yitty', 0, iff(rl.customer_id is not null,1,0)) as boolean) as retail_lead,
    cast(iff(rv.customer_id is not null, 1,0) as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',ms.datetime_signup)),date_trunc('hour',ms.datetime_signup)) date_hour, -- lead registration time
    count(iff(datediff('day', ms.datetime_signup, mb.datetime_activated) = 0,1,null)) as d1_vips_from_leads_at_lead_hour,
    count(iff(datediff('minute', ms.datetime_signup, mb.datetime_activated) + 1 <= 60,1,null)) as m60_vips_from_leads_at_lead_hour,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_fl_view.ultra_merchant.membership_brand_signup ms
join lake_fl_view.ultra_merchant.membership m
    on m.membership_id = ms.membership_id
left join lake_fl_view.ultra_merchant.membership_brand_activation mb
    on ms.membership_id = mb.membership_id
    and ms.membership_brand_id = mb.membership_brand_id
join lake_fl_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = case
        when ms.membership_brand_id = 1 then 52
        when ms.membership_brand_id = 2 then 241 end
left join _fl_gender fg
    on fg.customer_id = c.customer_id
left join _is_fl_scrubs_customer sc
    on sc.customer_id = c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _retail_vips rv
    on rv.customer_id = c.customer_id
    and rv.datetime_activated = m.datetime_activated
where
    ms.membership_signup_id is not null
    and iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',ms.datetime_signup)),date_trunc('hour',ms.datetime_signup)
        ) >= $low_watermark_date_rt
    and (datediff('day', ms.datetime_signup, mb.datetime_activated) = 0
        or datediff('minute', ms.datetime_signup, mb.datetime_activated) + 1 <= 60)
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and store_name in ('Fabletics US','Yitty US')
group by 1,2,3,4,5,6,7,8,9,10;


create or replace temporary table _realtime_leads as
select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    'Female' as gender,
    FALSE as is_fl_scrubs_customer,
    case when fk.customer_id is not null then 'FK Free Trial'
        when ds.store_brand = 'FabKids' and fk.customer_id is null then 'Not FK Free Trial'
    else 'N/A' end as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(0 as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)) date_hour,
    count(1) as leads,
    0 as primary_leads,
    0 as secondary_leads,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_jfb_view.ultra_merchant.membership m
join lake_jfb_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _fk_free_trial fk
    on fk.customer_id=c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)
        ) >= $low_watermark_date_rt
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_brand in ('JustFab', 'ShoeDazzle','FabKids')
group by 1,2,3,4,5,6,7,8,9,10

union all
select
    ds.store_brand as brand,
    ds.store_region as region,
    IFF(cd.value='CA', 'CA', ds.store_country) as country,
    ds.store_brand || ' ' || country as store_name,

    'Female' as gender,
    FALSE as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(0 as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)) date_hour,
    count(1) as leads,
    0 as primary_leads,
    0 as secondary_leads,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_sxf_view.ultra_merchant.membership m
join lake_sxf_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
left join _customer_detail cd on cd.customer_id = c.customer_id and cd.name='country_code'
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)
        ) >= $low_watermark_date_rt
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_brand = 'Savage X'
group by 1,2,3,4,5,6,7,8,9,10

union all
select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    case when fg.customer_id is not null then 'Male' else 'Female' end as gender,
    cast(iff(sc.customer_id is not null
        and cast(scrubs_registration_datetime as date) = cast(coalesce(m.datetime_added, c.datetime_added) as date),1,0) as boolean)
    as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(rl.customer_id is not null,1,0) as boolean) as retail_lead,
    cast(0 as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)) date_hour,
    count(1) as leads,
    0 as primary_leads,
    0 as secondary_leads,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from lake_fl_view.ultra_merchant.membership m
join lake_fl_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = m.store_id
left join _fl_gender fg
    on fg.customer_id=c.customer_id
left join _is_fl_scrubs_customer sc
    on sc.customer_id = c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_added)),date_trunc('hour',m.datetime_added)
        ) >= $low_watermark_date_rt
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_name not in ('Fabletics US','Yitty US')
    and ds.store_brand = 'Fabletics'
group by 1,2,3,4,5,6,7,8,9,10

union all
select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,

    case when fg.customer_id is not null then 'Male' else 'Female' end as gender,
    cast(iff(sc.customer_id is not null
        and cast(scrubs_registration_datetime as date) = cast(coalesce(m.datetime_added, c.datetime_added) as date),1,0) as boolean)
    as is_fl_scrubs_customer,
    'N/A' as fk_free_trial,
    cast(iff(store_brand ILIKE 'yitty', 0, iff(rl.customer_id is not null,1,0)) as boolean) as retail_lead,
    cast(0 as boolean) as retail_vip,

    iff(ds.store_region = 'EU', dateadd(hour,9,date_trunc('hour',m.datetime_signup)),date_trunc('hour',m.datetime_signup)) date_hour,
    count(1) as leads,
    count(iff(sequence = 1,1,null)) as primary_leads,
    count(iff(sequence > 1,1,null)) as secondary_leads,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(m.datetime_added))),max(m.datetime_added)) as max_refresh_time
from (
    select
        ms.*,
        m.customer_id,
        row_number() over (partition by ms.membership_id order by datetime_signup) as sequence
        from lake_fl_view.ultra_merchant.membership_brand_signup ms
        join lake_fl_view.ultra_merchant.membership m
            on m.membership_id = ms.membership_id
        ) m
join lake_fl_view.ultra_merchant.customer c
    on c.customer_id = m.customer_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = case
        when m.membership_brand_id = 1 then 52
        when m.membership_brand_id = 2 then 241 end
left join _fl_gender fg
    on fg.customer_id=c.customer_id
left join _is_fl_scrubs_customer sc
    on sc.customer_id = c.customer_id
left join _retail_leads rl
    on rl.customer_id = c.customer_id
where
    iff(ds.store_region = 'EU', dateadd(hour, 9, date_trunc('hour',m.datetime_signup)),date_trunc('hour',m.datetime_signup)
        ) >= $low_watermark_date_rt
    and c.email not like '%@test%'
    and c.email not like '%@example%'
    and c.email not like '%%test%@email%%'
    and c.email not like '%@fkqa'
    and c.email not like '%%@retail.fabletics%%'
    and c.email not like '%%@retail.savagex%%'
    and ds.store_name in ('Fabletics US','Yitty US')
group by 1,2,3,4,5,6,7,8,9,10;

------------------------------------------------------------------------------------
-- hourly fb spend --

create or replace temporary table _facebook_spend AS
select
    store_brand as brand,
    store_region as region,
    case when campaign_name ilike any ('%flmus_canada%', '%flmca_%') then 'CA'
        when st.store_brand_abbr = 'SX' and am.specialty_store = 'CA' then 'CA'
        else store_country
    end as country,
    brand || ' ' || country as store_name,
    case when mens_account_flag = 0 then 'Female'
       when mens_account_flag = 1 then 'Male'
       end as gender,
    coalesce(is_scrubs_flag,0) as is_fl_scrubs_customer,
    case when store_brand = 'FabKids' then 'Not FK Free Trial'
       else 'N/A'
    end as fk_free_trial,
    cast(0 as boolean) as retail_lead,
    cast(0 as boolean) as retail_vip,

    case when account_name = 'Fabletics_CA' then dateadd(hour, -3, cast(concat(date,' ', left(hourly_stats_aggregated_by_advertiser_time_zone,8)) as datetime)) --adjust to pst time
       else cast(concat(date,' ', left(hourly_stats_aggregated_by_advertiser_time_zone,8)) as datetime)
    end as date_hour,
    iff(store_region = 'EU',dateadd(hour,8,current_timestamp()),current_timestamp()) as timestamp,
    iff(store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', fb.meta_update_datetime)),fb.meta_update_datetime) as fb_meta_update_datetime_pst,
    iff(campaign_name ilike '%afterpay%' or adset_name ilike '%afterpay%', 0, (spend * coalesce(lkpusd.exchange_rate, 1))) as fb_spend, -- convert to USD
    iff(campaign_name ilike '%afterpay%' or adset_name ilike '%afterpay%', 0, (spend * coalesce(lkplocal.exchange_rate, 1))) as fb_spend_local
from lake_view.facebook.ad_insights_by_hour fb
join lake_view.sharepoint.med_account_mapping_media am on lower(am.source_id) = lower(fb.account_id)
    and am.source ilike '%facebook%'
join edw_prod.data_model.dim_store st on st.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and fb.date = lkpusd.rate_date_pst
    and lkpusd.dest_currency = 'USD'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and fb.date = lkplocal.rate_date_pst
    and lkplocal.dest_currency = st.store_currency
where date_hour >= $low_watermark_date
    and date_hour <= timestamp;

create or replace temporary table _facebook_spend_agg as
select
    brand,
    region,
    country,
    store_name,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    retail_lead,
    retail_vip,
    date_hour,
    max(fb_meta_update_datetime_pst) as fb_meta_update_datetime_pst,
    sum(fb_spend) as fb_spend,
    sum(fb_spend_local) as fb_spend_local
from _facebook_spend
group by 1,2,3,4,5,6,7,8,9,10;

------------------------------------------------------------------------------------
-- scaffold to house all combinations from edw and source for acquisition metrics

create or replace temporary table _scaffold_acq as
select distinct
    brand,
    region,
    country,
    store_name,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    retail_lead,
    retail_vip,
    date_hour
from _edw_leads
union
select distinct
    brand,
    region,
    country,
    store_name,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    retail_lead,
    retail_vip,
    date_hour
from _edw_vips
union
select distinct
    brand,
    region,
    country,
    store_name,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    retail_lead,
    retail_vip,
    date_hour
from _edw_d1_60m_vips_from_leads
union
select distinct
    brand,
    region,
    country,
    store_name,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    retail_lead,
    retail_vip,
    date_hour
from _realtime_leads
union
select distinct
    brand,
    region,
    country,
    store_name,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    retail_lead,
    retail_vip,
    date_hour
from _realtime_vips
union
select distinct
    brand,
    region,
    country,
    store_name,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    retail_lead,
    retail_vip,
    date_hour
from _realtime_d1_vips_from_leads_at_lead_hour
union
select distinct
    brand,
    region,
    country,
    store_name,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    retail_lead,
    retail_vip,
    date_hour
from _facebook_spend_agg;

-- final acq reporting table
create or replace transient table reporting_base_prod.shared.med61_realtime_acquisition as
select
    edw.brand,
    edw.region,
    edw.country,
    edw.store_name,
    edw.gender,
    edw.is_fl_scrubs_customer,
    edw.fk_free_trial,
    edw.retail_lead,
    edw.retail_vip,
    cast(iff(edw.retail_lead = true or edw.retail_vip = true, 1, 0) as boolean) as retail_customer,
    edw.date_hour,
    coalesce(edw_l.leads,rt_l.leads,0) as leads,
    coalesce(edw_l.primary_leads,rt_l.primary_leads,0) as primary_leads,
    coalesce(edw_l.secondary_leads,rt_l.secondary_leads,0) as secondary_leads,
    coalesce(edw_v.vips,rt_v.vips,0) as vips,
    coalesce(edw_d1.d1_vips_from_leads_at_lead_hour,rt_d1.d1_vips_from_leads_at_lead_hour,0) as d1_vips_from_leads_at_lead_hour,
    coalesce(edw_d1.m60_vips_from_leads_at_lead_hour,rt_d1.m60_vips_from_leads_at_lead_hour,0) as m60_vips_from_leads_at_lead_hour,
    coalesce(fb_spend,0) as fb_spend,
    coalesce(fb_spend_local,0) as fb_spend_local,
    rt_l.max_refresh_time as max_refresh_timestamp_leads,
    rt_v.max_refresh_time as max_refresh_timestamp_vips,
    f.fb_meta_update_datetime_pst
from _scaffold_acq edw
left join _edw_leads edw_l
    on edw.brand = edw_l.brand
    and edw.region = edw_l.region
    and edw.country = edw_l.country
    and edw.store_name = edw_l.store_name
    and edw.gender = edw_l.gender
    and edw.is_fl_scrubs_customer = edw_l.is_fl_scrubs_customer
    and edw.fk_free_trial = edw_l.fk_free_trial
    and edw.retail_lead = edw_l.retail_lead
    and edw.retail_vip = edw_l.retail_vip
    and edw.date_hour = edw_l.date_hour
left join _realtime_leads rt_l
    on edw.brand = rt_l.brand
    and edw.region = rt_l.region
    and edw.country = rt_l.country
    and edw.store_name = rt_l.store_name
    and edw.gender = rt_l.gender
    and edw.is_fl_scrubs_customer = rt_l.is_fl_scrubs_customer
    and edw.fk_free_trial = rt_l.fk_free_trial
    and edw.retail_lead = rt_l.retail_lead
    and edw.retail_vip = rt_l.retail_vip
    and edw.date_hour = rt_l.date_hour
left join _edw_vips edw_v
    on edw.brand = edw_v.brand
    and edw.region = edw_v.region
    and edw.country = edw_v.country
    and edw.store_name = edw_v.store_name
    and edw.gender = edw_v.gender
    and edw.is_fl_scrubs_customer = edw_v.is_fl_scrubs_customer
    and edw.fk_free_trial = edw_v.fk_free_trial
    and edw.retail_lead = edw_v.retail_lead
    and edw.retail_vip = edw_v.retail_vip
    and edw.date_hour = edw_v.date_hour
left join _realtime_vips rt_v
    on edw.brand = rt_v.brand
    and edw.region = rt_v.region
    and edw.country = rt_v.country
    and edw.store_name = rt_v.store_name
    and edw.gender = rt_v.gender
    and edw.is_fl_scrubs_customer = rt_v.is_fl_scrubs_customer
    and edw.fk_free_trial = rt_v.fk_free_trial
    and edw.retail_lead = rt_v.retail_lead
    and edw.retail_vip = rt_v.retail_vip
    and edw.date_hour = rt_v.date_hour
left join _edw_d1_60m_vips_from_leads edw_d1
    on edw.brand = edw_d1.brand
    and edw.region = edw_d1.region
    and edw.country = edw_d1.country
    and edw.store_name = edw_d1.store_name
    and edw.gender = edw_d1.gender
    and edw.is_fl_scrubs_customer = edw_d1.is_fl_scrubs_customer
    and edw.fk_free_trial = edw_d1.fk_free_trial
    and edw.retail_lead = edw_d1.retail_lead
    and edw.retail_vip = edw_d1.retail_vip
    and edw.date_hour = edw_d1.date_hour
left join _realtime_d1_vips_from_leads_at_lead_hour rt_d1
    on edw.brand = rt_d1.brand
    and edw.region = rt_d1.region
    and edw.country = rt_d1.country
    and edw.store_name = rt_d1.store_name
    and edw.gender = rt_d1.gender
    and edw.is_fl_scrubs_customer = rt_d1.is_fl_scrubs_customer
    and edw.fk_free_trial = rt_d1.fk_free_trial
    and edw.retail_lead = rt_d1.retail_lead
    and edw.retail_vip = rt_d1.retail_vip
    and edw.date_hour = rt_d1.date_hour
left join _facebook_spend_agg f
    on edw.brand = f.brand
    and edw.region = f.region
    and edw.country = f.country
    and edw.store_name = f.store_name
    and edw.gender = f.gender
    and edw.is_fl_scrubs_customer = f.is_fl_scrubs_customer
    and edw.fk_free_trial = f.fk_free_trial
    and edw.retail_lead = f.retail_lead
    and edw.retail_vip = f.retail_vip
    and edw.date_hour = f.date_hour;

alter table reporting_base_prod.shared.med61_realtime_acquisition set data_retention_time_in_days = 0;

-- ORDER
------------------------------------------------------------------------------------
-- historical order metrics data from edw --

create or replace temp table _vip_store_id as
select coalesce(b.store_id, a.store_id) as vip_store_id,
       a.store_id
from edw_prod.data_model.dim_store as a
left join edw_prod.data_model.dim_store as b
    on b.store_brand = a.store_brand
    and b.store_country = a.store_country
    and b.store_type = 'Online'
    and a.store_type <> 'Online'
    and b.is_core_store = True
    and b.store_full_name not in ('JustFab - Wholesale', 'PS by JustFab')
where a.is_core_store = true;

create or replace temporary table _orders_base as
select
    o.store_id,
    st.store_region as region,
    iff(fa.sub_store_id is not null and fa.sub_store_id <> -1, fa.sub_store_id, vsi.vip_store_id) as vip_store_id,
    order_local_datetime as order_datetime,
    case when is_cross_promo = false and store_brand ILIKE 'fabkids' then 'Not FK Free Trial'
        when is_cross_promo = true and store_brand ILIKE 'fabkids' then 'FK Free Trial'
    else 'N/A' end as fk_free_trial,
    gender,
    is_scrubs_customer,
    oc.order_classification_l1,
    membership_order_type_l3 as order_type,
    o.token_count,
    o.order_id,
    o.unit_count,
    o.reporting_usd_conversion_rate,
    o.product_gross_revenue_local_amount,
    o.product_subtotal_local_amount,
    o.product_discount_local_amount
from edw_prod.data_model.fact_order o
join edw_prod.data_model.fact_activation fa
    on fa.activation_key = o.activation_key
join edw_prod.data_model.dim_store st
    on st.store_id = o.store_id
    and st.is_core_store = True
join edw_prod.data_model.dim_customer dc
    on o.customer_id = dc.customer_id
join edw_prod.data_model.dim_order_membership_classification mc
    on o.order_membership_classification_key = mc.order_membership_classification_key
left join _vip_store_id vsi
    on vsi.store_id = st.store_id
join edw_prod.data_model.dim_order_sales_channel oc
    on oc.order_sales_channel_key = o.order_sales_channel_key
join edw_prod.data_model.dim_order_status os
    on os.order_status_key = o.order_status_key
left join edw_prod.data_model.fact_order_credit foc
    on foc.order_id = o.order_id
where
    is_test_order = 0
    and os.order_status ILIKE ANY ('success', 'pending')
    and order_local_datetime < $low_watermark_date_rt
    and order_local_datetime >= $low_watermark_date;

create or replace temporary table _edw_orders as
select
    o.store_id,
    o.vip_store_id,
    to_timestamp(date_trunc('hour',o.order_datetime)) as date_hour,
    cast(iff(ds.store_brand ILIKE 'fabletics', o.is_scrubs_customer, false) as boolean) as is_fl_scrubs_customer,
    iff(ds.store_brand ILIKE 'fabletics' and gender ILIKE 'm','Male','Female') as gender,
    fk_free_trial,
    order_type,
    sum(iff(o.order_classification_l1 ILIKE 'product order',1,0)) as order_count,
    sum(iff(token_count>0 and order_classification_l1 ilike 'product order',1,0)) as token_order_count,
    sum(iff(o.order_classification_l1 ILIKE 'product order',unit_count,0)) as units,
    sum(product_gross_revenue_local_amount * reporting_usd_conversion_rate) as revenue,
    sum(product_subtotal_local_amount) as subtotal,
    sum(product_discount_local_amount) as discount
from _orders_base as o
left join edw_prod.data_model.dim_store ds
    on ds.store_id = o.vip_store_id
group by 1,2,3,4,5,6,7;

------------------------------------------------------------------------------------
-- real-time order metric data from source --

create or replace temp table _vip_store_id_base as
select coalesce(try_to_number(od.value), o.store_id) as vip_store_id,
       o.order_id,
       try_to_number(od.value) odval,
       o.store_id,
       o.membership_brand_id
from lake_fl.ultra_merchant."ORDER" o
join _is_fl_scrubs_customer s
    on s.customer_id = o.customer_id
left join lake_fl.ultra_merchant.order_detail as od
    on o.order_id = od.order_id
    and od.name = 'retail_store_id';

create or replace temp table _vip_store_id_source as
select
    case
        when odval is not null then odval
        when membership_brand_id = 1 then 52
        when membership_brand_id = 2 then 241 end as store_id,
    order_id
from _vip_store_id_base v
join edw_prod.stg.dim_store ds
    on v.store_id = ds.store_id;

create or replace temp table _retail_store_id as
select od.order_id,
       retail_store_id
from _order_detail as od
where retail_store_id is not null;

CREATE OR REPLACE TEMP TABLE _order_classification__mobile_app_store AS
SELECT sc.store_id
FROM lake_fl.ultra_merchant.store_classification AS sc
    JOIN lake_fl.ultra_merchant.store_type AS st
        ON st.store_type_id = sc.store_type_id
WHERE st.store_type_id = 8
union all
SELECT sc.store_id
FROM lake_jfb.ultra_merchant.store_classification AS sc
    JOIN lake_fl.ultra_merchant.store_type AS st
        ON st.store_type_id = sc.store_type_id
WHERE st.store_type_id = 8
union all
SELECT sc.store_id
FROM lake_sxf.ultra_merchant.store_classification AS sc
    JOIN lake_fl.ultra_merchant.store_type AS st
        ON st.store_type_id = sc.store_type_id
WHERE st.store_type_id = 8;


CREATE OR REPLACE TEMP TABLE _order_classification AS
SELECT
    order_id,
    order_type_id
FROM lake_fl.ultra_merchant.order_classification
UNION ALL
SELECT
    order_id,
    order_type_id
FROM lake_sxf.ultra_merchant.order_classification
UNION ALL
SELECT
    order_id,
    order_type_id
FROM lake_jfb.ultra_merchant.order_classification
;

CREATE OR REPLACE TEMP TABLE _order_line AS
SELECT
    order_id,
    product_type_id
FROM lake_fl.ultra_merchant.order_line
UNION ALL
SELECT
    order_id,
    product_type_id
FROM lake_sxf.ultra_merchant.order_line
UNION ALL
SELECT
    order_id,
    product_type_id
FROM lake_jfb.ultra_merchant.order_line
;

-- Classifying the orders sales channel so that we only consider Product Orders
CREATE OR REPLACE TEMP TABLE _order_classification__sales_channel (
    order_id number,
    is_billing_order boolean,
    is_membership_fee boolean,
    is_exchange boolean,
    is_reship boolean,
    is_retail_order boolean,
    is_bops_order boolean,
    is_test_order boolean,
    is_membership_token boolean,
    is_gift_certificate boolean,
    is_mobile_app_order boolean,
    order_sales_channel varchar,
    order_classification_name varchar
    );
INSERT INTO _order_classification__sales_channel (
    order_id,
    is_billing_order,
    is_membership_fee,
    is_exchange,
    is_reship,
    is_retail_order,
    is_bops_order,
    is_test_order,
    is_membership_token,
    is_gift_certificate,
    is_mobile_app_order,
    order_sales_channel,
    order_classification_name
    )
    select * from(
SELECT
    o.order_id as order_id,
    MAX(IFF(oc.order_type_id IN (10,39), 1, 0)) AS is_billing_order,
    MAX(IFF(oc.order_type_id = 9, 1, 0)) AS is_membership_fee,
    MAX(IFF(oc.order_type_id IN (11, 26), 1, 0)) AS is_exchange,
    MAX(IFF(oc.order_type_id = 6, 1, 0)) AS is_reship,
    MAX(IFF(oc.order_type_id = 19, 1, 0)) AS is_retail_order,
    MAX(IFF(oc.order_type_id = 40, 1, 0)) AS is_bops_order,
    MAX(IFF(o.processing_statuscode = 2335 OR IFF(tc.customer_id IS NOT NULL, TRUE, FALSE), 1, 0)) AS is_test_order,
    MAX(IFF(oc.order_type_id = 39, 1, 0)) AS is_membership_token,
    MAX(IFF(ol.order_id IS NOT NULL, 1, 0)) AS is_gift_certificate,
    MAX(IFF(mas.store_id IS NOT NULL, 1, 0)) AS is_mobile_app_order,
    CASE
        WHEN is_billing_order = 1 THEN 'Billing Order'
        WHEN is_membership_fee = 1 THEN 'Billing Order'
        WHEN is_gift_certificate = 1 THEN 'Billing Order'
        WHEN is_retail_order = 1 THEN 'Retail Order'
        WHEN is_mobile_app_order = 1 THEN 'Mobile App Order'
        ELSE 'Web Order'
    END AS order_sales_channel,
    CASE
        WHEN is_membership_token = 1 THEN 'Token Billing'
        WHEN is_gift_certificate = 1 THEN 'Gift Certificate'
        WHEN is_billing_order = 1 THEN 'Credit Billing'
        WHEN is_exchange = 1 THEN 'Exchange'
        WHEN is_reship = 1 THEN 'Reship'
        WHEN is_membership_fee = 1 THEN 'Membership Fee'
        ELSE 'Product Order'
    END AS order_classification_name
FROM _orders AS o
    LEFT JOIN edw_prod.reference.test_customer tc
        ON o.customer_id = tc.customer_id
    LEFT JOIN _order_classification AS oc
        ON oc.order_id = o.order_id
        AND oc.order_type_id IN (6, 9, 10, 11, 19, 25, 26, 32, 35, 36, 39, 40, 43, 48, 49)
    LEFT JOIN _order_line ol
        ON ol.order_id = o.order_id
        AND ol.product_type_id IN (5, 20)
    LEFT JOIN _order_classification__mobile_app_store AS mas
        ON mas.store_id = o.store_id
GROUP BY 1) where order_classification_name = 'Product Order';


CREATE OR REPLACE TEMP TABLE _order__bops_store AS
WITH cte AS (
SELECT sc.order_id, o.store_id
FROM _order_classification__sales_channel AS sc
      JOIN _orders AS o
           ON o.order_id = sc.order_id
WHERE sc.is_bops_order = TRUE
UNION
/* This UNION captures child order_ids in split BOPS that were shipped to customer */
SELECT base.order_id, o.store_id
FROM _order_classification__sales_channel AS base
      JOIN _orders AS o
           ON o.order_id = base.order_id
      JOIN _order_classification AS oc
           ON oc.order_id = o.order_id
WHERE oc.order_type_id = 40
)
SELECT
    cte.order_id,
    retail_store_id AS bops_original_store_id,
    cte.store_id AS bops_store_id
FROM cte
LEFT JOIN _order_detail AS od
    ON od.order_id = cte.order_id
    AND od.name = 'original_store_id'
    AND retail_store_id IS NOT NULL;


CREATE OR REPLACE TEMP TABLE _order_classification__is_activating AS
SELECT
    base.order_id,
    CASE
        WHEN (post.is_membership_conversion_order = 1 OR post.is_first_online_retail_order = 1)
            THEN 1
        ELSE 0
    END AS is_activating
FROM _order_classification__sales_channel AS base
    LEFT JOIN
    (SELECT
        base.order_id,
        COALESCE(MAX(IFF(oc.order_type_id = 23, 1, 0)), 0) AS is_membership_conversion_order,
        COALESCE(MAX(IFF(oc.order_type_id = 33, 1, 0)), 0) AS is_first_online_retail_order
    FROM _order_classification__sales_channel AS base
    JOIN _order_classification AS oc
        ON oc.order_id = base.order_id
    WHERE order_type_id IN (23, 33)
    GROUP BY 1) AS post
        ON post.order_id = base.order_id;

CREATE OR REPLACE TEMP TABLE _order_classification__is_guest as
SELECT
    base.order_id,
    CASE
        WHEN ia.is_activating = 1 THEN 0
        ELSE 1
    END AS is_guest
 FROM _order_classification__sales_channel AS base
   LEFT JOIN _order_classification__is_activating AS ia
        ON ia.order_id = base.order_id
    ;

-- update online retail orders to is_guest
UPDATE _order_classification__is_guest AS g
SET g.is_guest = 1
FROM _order_classification AS oc
WHERE oc.order_id = g.order_id
    AND oc.order_type_id IN (33, 32);


CREATE OR REPLACE TEMP TABLE _reship AS
SELECT reship_order_id,
       original_order_id
FROM lake_fl.ultra_merchant.reship
UNION ALL
SELECT reship_order_id,
       original_order_id
FROM lake_sxf.ultra_merchant.reship
UNION ALL
SELECT reship_order_id,
       original_order_id
FROM lake_jfb.ultra_merchant.reship;

CREATE OR REPLACE TEMP TABLE _exchange AS
SELECT exchange_order_id,
       original_order_id
FROM lake_fl.ultra_merchant.exchange
UNION ALL
SELECT exchange_order_id,
       original_order_id
FROM lake_sxf.ultra_merchant.exchange
UNION ALL
SELECT exchange_order_id,
       original_order_id
FROM lake_jfb.ultra_merchant.exchange;


 CREATE OR REPLACE TEMP TABLE _order_classification__original_order (
    order_id number,
    original_order_id number,
    is_exchange boolean,
    is_reship boolean,
    is_activating boolean,
    is_guest boolean
    );
INSERT INTO _order_classification__original_order (
    order_id,
    original_order_id,
    is_exchange,
    is_reship,
    is_activating,
    is_guest
    )
SELECT
    base.order_id,
    r.original_order_id,
    FALSE AS is_exchange,
    TRUE AS is_reship,
    COALESCE(ia.is_activating,  FALSE) AS is_activating,
    COALESCE(ig.is_guest,  TRUE) AS is_guest
FROM _order_classification__sales_channel AS base
    JOIN _reship AS r
        ON r.reship_order_id = base.order_id
    LEFT JOIN _order_classification__is_activating AS ia
        ON ia.order_id = r.original_order_id
    LEFT JOIN _order_classification__is_guest AS ig
        ON ig.order_id = r.original_order_id

UNION ALL
SELECT
    base.order_id,
    e.original_order_id,
    TRUE AS is_exchange,
    FALSE AS is_reship,
    COALESCE(ia.is_activating, FALSE) AS is_activating,
    COALESCE(ig.is_guest,  TRUE) AS is_guest
FROM _order_classification__sales_channel AS base
    JOIN _exchange AS e
        ON e.exchange_order_id = base.order_id
    LEFT JOIN _order_classification__is_activating AS ia
        ON ia.order_id = e.original_order_id
    LEFT JOIN _order_classification__is_guest AS ig
        ON ig.order_id = e.original_order_id;

CREATE OR REPLACE TEMP TABLE _all_orders  as
select base.order_id,
IFF((sc.is_exchange OR sc.is_reship), COALESCE(oo.is_activating, ia.is_activating), ia.is_activating) AS is_activating,
    IFF((sc.is_exchange OR sc.is_reship), COALESCE(oo.is_guest, ig.is_guest), ig.is_guest) AS is_guest

from _order_classification__sales_channel base
LEFT JOIN _order_classification__is_activating AS ia
        ON ia.order_id = base.order_id
    LEFT JOIN _order_classification__is_guest AS ig
        ON ig.order_id = base.order_id
    LEFT JOIN _order_classification__sales_channel AS sc
        ON sc.order_id = base.order_id
    LEFT JOIN _order_classification__original_order AS oo
        ON oo.order_id = base.order_id

        where sc.is_test_order = 0;

create or replace temp table _pre_orders as
select base.order_id
from _order_classification__sales_channel AS base
    JOIN _order_classification  oc
    on base.order_id =oc.order_id
where order_type_id = 36;

create or replace temp table _order_status as
select
    o.order_id,
    case when po.order_id is not null then true else false end as is_pre_order,
    dopr.order_processing_status,
    dopy.order_payment_status,
    case
        when o.processing_statuscode = 2335 or tc.customer_id is not null then 'Test Order'
        when coalesce(o.datetime_shipped, o.date_shipped) is not null then 'Success'
        when dopy.order_payment_status ILIKE ANY ('failed authorization', 'authorization expired') then 'Failure'
        when dopr.order_processing_status ILIKE 'placement failed' then 'Failure'
        when dopr.order_processing_status ILIKE ANY ('cancelled', 'cancelled (incomplete auth redirect)') then 'Cancelled'
        when dopr.order_processing_status ILIKE ANY
        ('placed', 'fulfillment (batching)', 'fulfillment (in progress)', 'ready for pickup',
         'hold (manual review - group 1)')
            or (po.order_id is not null and dopr.order_processing_status ILIKE 'hold (preorder)') then 'Pending'
        when dopr.order_processing_status ILIKE 'hold%' then 'On Hold'
        when dopr.order_processing_status ILIKE ANY ('authorizing payment', 'initializing') then 'On Hold'
        when dopr.order_processing_status ILIKE ANY ('shipped', 'complete') then 'Success'
        when dopr.order_processing_status_code = 2130 then 'Pre-order Split'
        when dopr.order_processing_status ILIKE 'split (for bops)' then 'BOPS Split'
        when dopr.order_processing_status ILIKE 'fulfillment (failed inventory check)' then 'Cancelled'
    else 'Unclassified'
    end as order_status
from _orders o
left join edw_prod.stg.dim_order_processing_status as dopr
    on dopr.order_processing_status_code = o.processing_statuscode
    and o.datetime_added between dopr.effective_start_datetime and dopr.effective_end_datetime
left join edw_prod.stg.dim_order_payment_status as dopy
    on dopy.order_payment_status_code = o.payment_statuscode
    and o.datetime_added between dopy.effective_start_datetime and dopy.effective_end_datetime
left join _pre_orders po on po.order_id = o.order_id
left join edw_prod.reference.test_customer tc
    on o.customer_id = tc.meta_original_customer_id;

create or replace temporary table _address as
SELECT address_id,
       country_code
from lake_fl.ultra_merchant.address
UNION
SELECT address_id,
       country_code
from lake_jfb.ultra_merchant.address
UNION
SELECT address_id,
       country_code
from lake_sxf.ultra_merchant.address;

CREATE OR REPLACE TEMP TABLE _membership_plan_membership_brand AS
select membership_brand_id,
    store_id
from lake_fl.ultra_merchant.membership_plan_membership_brand
UNION ALL
select membership_brand_id,
    store_id
from lake_sxf.ultra_merchant.membership_plan_membership_brand
UNION ALL
select membership_brand_id,
    store_id
from lake_jfb.ultra_merchant.membership_plan_membership_brand
;

CREATE OR REPLACE TEMP TABLE _order__brand_store_id_mapping AS
SELECT
    o.order_id,
    CASE
        WHEN o.membership_brand_id IS NULL THEN o.store_id
        WHEN st.store_type = 'Retail' THEN o.store_id
        WHEN st.is_core_store = FALSE THEN o.store_id
        WHEN lower(st.store_brand) = lower(mbst.store_brand) THEN o.store_id
        WHEN o.membership_brand_id = 2 AND st.store_type = 'Mobile App' THEN 24101
        WHEN o.membership_brand_id IS NOT NULL THEN mpmb.store_id
    ELSE o.store_id
    END AS store_id
from _orders o
join _all_orders ao
on o.order_id = ao.order_id
    JOIN edw_prod.stg.dim_store AS st
        ON st.store_id = o.store_id
    LEFT JOIN _membership_plan_membership_brand AS mpmb
        ON mpmb.membership_brand_id = o.membership_brand_id
    LEFT JOIN edw_prod.stg.dim_store AS mbst
        ON mbst.store_id = mpmb.store_id;

update _orders ob
set ob.store_id = obm.store_id
from _order__brand_store_id_mapping obm
where ob.order_id = obm.order_id;

create or replace temporary table _realtime_orders_base as
select distinct
    ds.store_id,
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name as store_name,
    ds.store_type as store_type,
    ds.store_currency,
    case when fg.customer_id is not null and ds.store_brand ilike 'fabletics' then 'Male' else 'Female' end as gender,
    cast(iff(sc.customer_id is not null and ds.store_brand ilike 'fabletics'
        and cast(scrubs_registration_datetime as date) = cast(coalesce(m.datetime_added, c.datetime_added) as date),1,0) as boolean)
    as is_fl_scrubs_customer,
    case when fk.customer_id is not null then 'FK Free Trial'
        when ds.store_brand ILIKE 'fabkids' and fk.customer_id is null then 'Not FK Free Trial'
    else 'N/A' end as fk_free_trial,
    case
        when is_activating and is_guest then 'First Guest'
        when is_activating and not is_guest then 'Activating VIP'
        when not is_activating and is_guest then 'Repeat Guest'
        when not is_activating and not is_guest then 'Repeat VIP'
    end as order_type,
    o.order_id,
    o.customer_id
from _orders o
join _all_orders ao
on o.order_id = ao.order_id
join _order_status os
    on o.order_id = os.order_id
    and os.order_status ILIKE ANY ('success', 'pending')
left join _vip_store_id_source vip
    on vip.order_id = o.order_id
left join _retail_store_id rsi
    on rsi.order_id = o.order_id
LEFT JOIN _order__bops_store AS bops
        ON bops.order_id = o.order_id
join edw_prod.data_model.dim_store ds
    on ds.store_id = coalesce(bops.bops_original_store_id,rsi.retail_store_id, o.store_id)
join _customer c on
    c.customer_id = o.customer_id
left join _membership m
    on m.customer_id = c.customer_id
left join _fl_gender fg
    on fg.customer_id = c.customer_id
left join _is_fl_scrubs_customer sc
    on sc.customer_id = c.customer_id
left join _fk_free_trial fk
    on fk.customer_id = c.customer_id
left join (
    select distinct oc.order_id
    from lake_fl.ultra_merchant.order_classification oc
  join _all_orders ao
  on oc.order_id = ao.order_id
    where order_type_id = 3
    ) as oct
    on oct.order_id = o.order_id
and oct.order_id is null;

create or replace temp table _units_set as
select
    ol.order_id,
    count(*) as units
from lake_fl_view.ultra_merchant.order_line ol
join lake_fl_view.ultra_merchant.product_type pt
    on pt.product_type_id = ol.product_type_id
join _orders o
    on o.order_id = ol.order_id
join lake_fl_view.ultra_merchant.statuscode sc
    on sc.statuscode = ol.statuscode
where ol.product_type_id != 14
    and pt.is_free = 0
    and sc.label != 'Cancelled'
group by 1

UNION ALL
select
    ol.order_id,
    count(*) as units
from lake_jfb_view.ultra_merchant.order_line ol
join lake_jfb_view.ultra_merchant.product_type pt
    on pt.product_type_id = ol.product_type_id
join _orders o on o.order_id = ol.order_id
join lake_jfb_view.ultra_merchant.statuscode sc
    on sc.statuscode = ol.statuscode
where ol.product_type_id != 14
    and pt.is_free = 0
    and sc.label != 'Cancelled'
group by 1

UNION ALL
select
    ol.order_id,
    count(*) as units
from lake_sxf_view.ultra_merchant.order_line ol
join lake_sxf_view.ultra_merchant.product_type pt
    on pt.product_type_id = ol.product_type_id
join _orders o
    on o.order_id = ol.order_id
join lake_sxf_view.ultra_merchant.statuscode sc
    on sc.statuscode = ol.statuscode
where ol.product_type_id != 14
    and pt.is_free = 0
    and sc.label != 'Cancelled'
group by 1;

create or replace temp table _realtime_orders as
select
    ob.store_id,
    brand,
    region,
    country,
    store_name,
    store_type,
    gender,
    is_fl_scrubs_customer,
    fk_free_trial,
    order_type,
    iff(region = 'EU', dateadd(hour, 9, date_trunc('hour', o.datetime_added)),date_trunc('hour', o.datetime_added)) as date_hour,
    sum(coalesce(o.subtotal, 0) / (1 + coalesce(vrh.rate, 0))) * ifnull(avg(ocr.exchange_rate), 1) as subtotal_net_vat,
    sum(coalesce(o.discount, 0) / (1 + coalesce(vrh.rate, 0))) * ifnull(avg(ocr.exchange_rate), 1)  as discount_net_vat,
    sum(coalesce(o.shipping, 0) / (1 + coalesce(vrh.rate, 0))) * ifnull(avg(ocr.exchange_rate), 1) as shipping_net_vat,
    (subtotal_net_vat - discount_net_vat + shipping_net_vat) as revenue,
    count(distinct o.order_id)  as order_count,
    sum(token_order_flag) as token_order_count,
    sum(coalesce(u.units, 0)) as units,
    max(o.datetime_added) as max_refresh_time
from _realtime_orders_base ob
join _orders o
    on ob.order_id = o.order_id
left join edw_prod.reference.currency_exchange_rate_by_date ocr
    on ocr.src_currency = ob.store_currency
    and to_date(ocr.rate_date_pst) = date_trunc('day', to_date(o.datetime_added))
    and ocr.dest_currency = 'usd'
left join _address as a
    on a.address_id = o.shipping_address_id
left join edw_prod.reference.vat_rate_history as vrh
    on replace(vrh.country_code, 'GB', 'UK') = replace(a.country_code, 'GB', 'UK')
    and vrh.expires_date > to_date(o.datetime_added)
join _customer c
    on c.customer_id = o.customer_id
left join _units_set u
    on u.order_id = o.order_id
group by 1,2,3,4,5,6,7,8,9,10,11;

------------------------------------------------------------------------------------
-- scaffold to house all combinations from edw and source

create or replace temporary table _scaffold_orders as
select distinct store_id, gender, is_fl_scrubs_customer, fk_free_trial, order_type, date_hour
from _edw_orders
union
select distinct store_id, gender, is_fl_scrubs_customer, fk_free_trial, order_type, date_hour
from _realtime_orders;

-- final order metrics reporting table
create or replace transient table reporting_base_prod.shared.med61_realtime_orders as
select
    ds.store_brand as brand,
    ds.store_region as region,
    ds.store_country as country,
    ds.store_name,
    ds.store_type,
    s.gender,
    s.is_fl_scrubs_customer,
    s.fk_free_trial,
    s.date_hour,
    iff(ds.store_region = 'EU',dateadd(minute, 9 * 60, date_trunc('minute', max(max_refresh_time))),max(max_refresh_time)) as max_refresh_time,
    -- activating
    sum(iff(s.order_type ILIKE 'activating vip', coalesce(edw.order_count,rt.order_count), 0)) as activating_order_count,
    sum(iff(s.order_type ILIKE 'activating vip', coalesce(edw.units,rt.units), 0)) as activating_unit_count,
    sum(iff(s.order_type ilike 'activating vip', coalesce(edw.discount,rt.discount_net_vat), 0)) as activating_discount,
    sum(iff(s.order_type ilike 'activating vip', coalesce(edw.subtotal,rt.subtotal_net_vat), 0)) as activating_subtotal,
    sum(iff(s.order_type ILIKE 'activating vip', coalesce(edw.revenue,rt.revenue), 0)) as activating_revenue,
    -- nonactivating
    sum(iff(s.order_type ilike any ('repeat vip', 'first guest', 'repeat guest'), coalesce(edw.order_count,rt.order_count), 0)) as nonactivating_order_count,
    sum(iff(s.order_type ilike any ('repeat vip', 'first guest', 'repeat guest'), coalesce(edw.token_order_count,rt.token_order_count), 0)) as nonactivating_token_order_count,
    sum(iff(s.order_type ilike any ('repeat vip', 'first guest', 'repeat guest'), coalesce(edw.units,rt.units), 0)) as nonactivating_unit_count,
    sum(iff(s.order_type ilike any ('repeat vip', 'first guest', 'repeat guest'), coalesce(edw.discount,rt.discount_net_vat), 0)) as nonactivating_discount,
    sum(iff(s.order_type ilike any ('repeat vip', 'first guest', 'repeat guest'), coalesce(edw.subtotal,rt.subtotal_net_vat), 0)) as nonactivating_subtotal,
    sum(iff(s.order_type ilike any ('repeat vip', 'first guest', 'repeat guest'), coalesce(edw.revenue,rt.revenue), 0)) as nonactivating_revenue,
    -- first guest
    sum(iff(s.order_type ilike 'first guest', coalesce(edw.order_count,rt.order_count), 0)) as first_guest_order_count,
    sum(iff(s.order_type ilike 'first guest', coalesce(edw.units,rt.units), 0)) as first_guest_unit_count,
    sum(iff(s.order_type ilike 'first guest', coalesce(edw.discount,rt.discount_net_vat), 0)) as first_guest_discount,
    sum(iff(s.order_type ilike 'first guest', coalesce(edw.subtotal,rt.subtotal_net_vat), 0)) as first_guest_subtotal,
    sum(iff(s.order_type ilike 'first guest', coalesce(edw.revenue,rt.revenue), 0)) as first_guest_revenue,
    -- repeat guest
    sum(iff(s.order_type ilike 'repeat guest', coalesce(edw.order_count,rt.order_count), 0)) as repeat_guest_order_count,
    sum(iff(s.order_type ilike 'repeat guest', coalesce(edw.units,rt.units), 0)) as repeat_guest_unit_count,
    sum(iff(s.order_type ilike 'repeat guest', coalesce(edw.discount,rt.discount_net_vat), 0)) as repeat_guest_discount,
    sum(iff(s.order_type ilike 'repeat guest', coalesce(edw.subtotal,rt.subtotal_net_vat), 0)) as repeat_guest_subtotal,
    sum(iff(s.order_type ilike 'repeat guest', coalesce(edw.revenue,rt.revenue), 0)) as repeat_guest_revenue,
    -- repeat vip
    sum(iff(s.order_type ilike 'repeat vip', coalesce(edw.order_count,rt.order_count), 0)) as repeat_vip_order_count,
    sum(iff(s.order_type ilike 'repeat vip', coalesce(edw.token_order_count,rt.token_order_count), 0)) as repeat_vip_token_order_count,
    sum(iff(s.order_type ilike 'repeat vip', coalesce(edw.units,rt.units), 0)) as repeat_vip_unit_count,
    sum(iff(s.order_type ilike 'repeat vip', coalesce(edw.discount,rt.discount_net_vat), 0)) as repeat_vip_discount,
    sum(iff(s.order_type ilike 'repeat vip', coalesce(edw.subtotal,rt.subtotal_net_vat), 0)) as repeat_vip_subtotal,
    sum(iff(s.order_type ilike 'repeat vip', coalesce(edw.revenue,rt.revenue), 0)) as repeat_vip_revenue
from _scaffold_orders s
join edw_prod.data_model.dim_store ds
    on s.store_id = ds.store_id
left join _edw_orders edw
    on s.store_id = edw.store_id
    and s.gender = edw.gender
    and s.is_fl_scrubs_customer = edw.is_fl_scrubs_customer
    and s.fk_free_trial = edw.fk_free_trial
    and s.order_type = edw.order_type
    and s.date_hour = edw.date_hour
left join _realtime_orders rt
    on s.store_id = rt.store_id
    and s.gender = rt.gender
    and s.is_fl_scrubs_customer = rt.is_fl_scrubs_customer
    and s.fk_free_trial = rt.fk_free_trial
    and s.order_type = rt.order_type
    and s.date_hour = rt.date_hour
group by 1,2,3,4,5,6,7,8,9;

alter table reporting_base_prod.shared.med61_realtime_orders set data_retention_time_in_days = 0;

/*

-- QA FOR ORDER AND ACQUISITION
-- comment out when in prod / uncomment when testing and update realtime watermark date
------------------------------------------------------------------------------------

-- qa acq metrics against test table after any code changes

-- refresh times
select region, max(max_refresh_timestamp_leads) as leads, max(max_refresh_timestamp_vips) as vips,
       max(fb_meta_update_datetime_pst) as fb
from _acq_test where to_date(date_hour) = current_date() group by 1;

select * from _acq_test
where store_name = 'Fabletics US'
and gender = 'Female' and is_fl_scrubs_customer = false and retail_customer = false and to_date(date_hour) = current_date()
order by date_hour asc;

-- edw
select brand, sum(vips) as vips, sum(fb_spend) as spend from _acq_test
where to_date(date_hour) < $low_watermark_date_rt group by 1;
select brand, sum(vips) as vips, sum(fb_spend) as spend from reporting_base_prod.shared.med61_realtime_acquisition
where to_date(date_hour) < $low_watermark_date_rt group by 1;

-- rt
select date_hour, sum(fb_spend) as spend, sum(vips) as vips from _acq_test
where brand = 'Fabletics' and to_date(date_hour) >= $low_watermark_date_rt and region = 'NA' and retail_customer = false group by 1 order by date_hour desc;
select date_hour, sum(fb_spend) as spend, sum(vips) as vips from reporting_base_prod.shared.med61_realtime_acquisition
where brand = 'Fabletics' and to_date(date_hour) >= $low_watermark_date_rt and region = 'NA' and retail_customer = false group by 1 order by date_hour desc;


-- qa order metrics against daily cash

-- refresh times
select region, max(max_refresh_time) from _orders_test group by 1;

select * from _orders_test
where store_name = 'Fabletics US'
and gender = 'Female' and store_type = 'Online' and is_fl_scrubs_customer = false and to_date(date_hour) = current_date()
order by date_hour asc;


-- daily cash table
create or replace temporary table _daily_cash as
select distinct
    store_brand,
    report_mapping,
    case lower(report_mapping)
        when 'flna-fvip-fl-orev' then 'Fabletics'
        when 'flna-mvip-fl-orev' then 'Fabletics Men'
        when 'flna-sc-orev' then 'Fabletics Scrubs'
        when 'ytna-orev' then 'Yitty'
    end as brand,
    date,
    sum(activating_product_order_count) as act_count,
    sum(activating_product_order_product_subtotal_amount) as act_sub,
    sum(activating_product_order_product_discount_amount) as act_disct,
    sum(activating_product_gross_revenue) as act_rev,
    sum(nonactivating_product_order_count) as non_count,
    sum(nonactivating_product_order_product_subtotal_amount) as non_sub,
    sum(nonactivating_product_order_product_discount_amount) as non_disct,
    sum(nonactivating_product_gross_revenue) as non_rev
from edw_prod.reporting.daily_cash_final_output dc
where lower(report_mapping) in ('flna-fvip-fl-orev', 'flna-mvip-fl-orev', 'flna-sc-orev', 'ytna-orev')
  --and date >= $low_watermark_date
  and date >= current_date() - 30
  and date_object = 'placed'
  and currency_object = 'usd'
group by 1, 2, 3, 4
order by 1, 2, 3, 4;

-- real-time/edw order table
create or replace temporary table _orders_final_rt as
select distinct
    case
        when is_fl_scrubs_customer = true then 'Fabletics Scrubs'
        when lower(gender) = 'male' and brand = 'Fabletics' then 'Fabletics Men'
        when lower(gender) = 'female' and brand = 'Fabletics' then 'Fabletics'
        else brand
    end as brand,
    to_date(date_hour) as date,
    sum(activating_order_count)    as act_count,
    sum(activating_subtotal) as act_sub,
    sum(activating_discount) as act_disct,
    sum(activating_revenue)        as act_rev,
    sum(nonactivating_order_count) as non_count,
    sum(nonactivating_subtotal) as non_sub,
    sum(nonactivating_discount) as non_disct,
    sum(nonactivating_revenue)     as non_rev
from _orders_test
where store_type in ('Online', 'Mobile App')
  and region = 'NA'
  and brand not in ('JustFab', 'ShoeDazzle', 'FabKids', 'Savage X')
  --and to_date(date_hour) >= $low_watermark_date_rt
    and to_date(date_hour) >= current_date() - 30
group by 1, 2
order by 1, 2;

-- variance
create or replace temporary table _final_variance as
select
    dc.brand,
    dc.date, --date_trunc('month',o.date) as month,
    o.brand as orders_brand,
    o.date as orders_date,
    -- total order counts
    sum(o.act_count + o.non_count) as order_final_total_order,
    sum(dc.act_count + dc.non_count) as dail_cash_total_order,
    abs((sum(o.act_count + o.non_count) - sum(dc.act_count + dc.non_count)) / sum(o.act_count + o.non_count)) as total_order_dif,
    -- activating
    sum(o.act_count) as o_act_count,
    sum(dc.act_count) as dc_act_count,
    abs((sum(o.act_count - dc.act_count) / sum(o.act_count))) as act_count_diff,
    sum(o.act_sub) as o_act_sub,
    sum(dc.act_sub) as dc_act_sub,
    abs((sum(o.act_sub - dc.act_sub) / sum(o.act_sub))) as act_sub_diff,
    sum(o.act_disct) as o_act_disct,
    sum(dc.act_disct) as dc_act_disct,
    abs((sum(o.act_disct - dc.act_disct) / sum(o.act_disct))) as act_disct_diff,
    sum(o.act_rev) as o_act_rev,
    sum(dc.act_rev) as dc_act_rev,
    abs((sum(o.act_rev - dc.act_rev) / sum(o.act_rev))) as act_rev_diff,
    -- non activating
    sum(o.non_count) as o_non_count,
    sum(dc.non_count) as dc_non_count,
    abs((sum(o.non_count - dc.non_count) / sum(o.non_count))) as non_count_diff,
    sum(o.non_sub) as o_non_sub,
    sum(dc.non_sub) as dc_non_sub,
    abs((sum(o.non_sub - dc.non_sub) / sum(o.non_sub))) as non_sub_diff,
    sum(o.non_disct) as o_non_disct,
    sum(dc.non_disct) as dc_non_disct,
    abs((sum(o.non_disct - dc.non_disct) / sum(o.non_disct))) as non_disct_diff,
    sum(o.non_rev) as o_non_rev,
    sum(dc.non_rev) as dc_non_rev,
    abs((sum(o.non_rev - dc.non_rev) / sum(o.non_rev))) as non_rev_diff
from _daily_cash dc
full join _orders_final_rt o on o.brand = dc.brand and o.date = dc.date
where dc.date is not null and o.date is not null
and dc.date != current_date()
group by 1, 2,3,4
order by o.date desc;

select * from _final_variance;

*/
