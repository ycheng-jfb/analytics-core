set high_watermark_date = current_date() - 60;

-- pixel table
create or replace temporary table _pixel as
with _pixel_raw as (
select
    case pixel_id
        when '537344443131231' then 'Fabletics CA'
        when '715289052842650' then 'Yitty US'
        when '1653034964810714' then 'Savage X EU'
        when '218843068308551' then 'Fabletics US'
        when '686991808018907' then 'JustFab US'
        when '212920452242866' then 'FabKids US'
        when '528161084251376' then 'Savage X US'
        when '741318252734475' then 'JustFab EU'
        when '2194587504139935' then 'Fabletics EU'
        when '279505792226308' then 'ShoeDazzle US'
        when '383363077795017' then 'Savage X US Test'
    end as store_name,
    concat('Meta ',iff(source='BROWSER','Browser','Server')) as source,
    iff(event='CompleteRegistration','Complete Registration', event) as event,
    start_time::datetime as datetime,
    value,
    max(start_time)::datetime as latest_refresh_datetime
from lake.facebook.ads_pixel_stats
unpivot (value for source in (browser, server))
where event in ('Purchase','CompleteRegistration')
group by 1,2,3,4,5)

select store_name, source, event, datetime, latest_refresh_datetime, sum(value) as value
from _pixel_raw
group by 1,2,3,4,5;

-- internal capi
create or replace temporary table _capi as
with _capi_raw as (
select distinct
    event_id,
    case pixel_id
        when '537344443131231' then 'Fabletics CA'
        when '715289052842650' then 'Yitty US'
        when '1653034964810714' then 'Savage X EU'
        when '218843068308551' then 'Fabletics US'
        when '686991808018907' then 'JustFab US'
        when '212920452242866' then 'FabKids US'
        when '528161084251376' then 'Savage X US'
        when '741318252734475' then 'JustFab EU'
        when '2194587504139935' then 'Fabletics EU'
        when '279505792226308' then 'ShoeDazzle US'
        when '383363077795017' then 'Savage X US Test'
    end as store_name,
    'Meta CAPI' as source,
    iff(event_name='CompleteRegistration','Complete Registration', event_name) as event,
    event_time::datetime as datetime,
    1 as value,
    max(event_time)::datetime as latest_refresh_datetime
from reporting_media_base_prod.facebook.conversions_history_audit c
where event_name in ('Purchase','CompleteRegistration')
and event_time::date >= $high_watermark_date
group by 1,2,3,4,5)

select store_name, source, event, datetime, latest_refresh_datetime, sum(value) as value
from _capi_raw
group by 1,2,3,4,5;

-- segment
-- fabletics
create or replace temporary table _segment as
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Complete Registration' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_fl.java_fabletics_complete_registration r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where timestamp >= $high_watermark_date
group by 1,2,3,4
union
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Purchase' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_fl.java_fabletics_order_completed r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where properties_is_activating = true
and timestamp >= $high_watermark_date
group by 1,2,3,4

union

-- savage x
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Complete Registration' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_sxf.java_sxf_complete_registration r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where timestamp >= $high_watermark_date
group by 1,2,3,4
union
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Purchase' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_sxf.java_sxf_order_completed r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where properties_is_activating = true
and timestamp >= $high_watermark_date
group by 1,2,3,4

union

-- justfab
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Complete Registration' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_gfb.java_justfab_complete_registration r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where timestamp >= $high_watermark_date
group by 1,2,3,4
union
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Purchase' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_gfb.java_justfab_order_completed r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where properties_is_activating = true
and timestamp >= $high_watermark_date
group by 1,2,3,4

union

-- shoedazzle
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Complete Registration' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_gfb.java_shoedazzle_complete_registration r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where timestamp >= $high_watermark_date
group by 1,2,3,4
union
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Purchase' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_gfb.java_shoedazzle_order_completed r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where properties_is_activating = true
and timestamp >= $high_watermark_date
group by 1,2,3,4

union

-- fabkids
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Complete Registration' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_gfb.java_fabkids_complete_registration r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where timestamp >= $high_watermark_date
group by 1,2,3,4
union
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'Segment' as source,
    'Purchase' as event,
    convert_timezone('UTC','America/Los_Angeles', timestamp) as datetime,
    max(timestamp)::datetime as latest_refresh_datetime,
    count(distinct concat(anonymousid,properties_customer_id)) as value
from lake.segment_gfb.java_fabkids_order_completed r
join edw_prod.data_model.dim_store ds on r.properties_store_id = ds.store_id
where properties_is_activating = true
and timestamp >= $high_watermark_date
group by 1,2,3,4;

-- edw
create or replace temporary table _edw as
with _edw as (
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'EDW' as source,
    'Complete Registration' as event,
    registration_local_datetime::datetime as datetime,
    count(1) as value,
    max(registration_local_datetime) as latest_refresh_datetime
from edw_prod.data_model.fact_registration r
join edw_prod.data_model.dim_store ds on r.store_id = ds.store_id
where registration_local_datetime >= $high_watermark_date
and is_retail_registration = false
and is_fake_retail_registration = false
group by 1,2,3,4
union
select
    iff(store_region = 'EU',concat(store_brand,' EU'),store_name) as store_name,
    'EDW' as source,
    'Purchase' as event,
    activation_local_datetime::datetime as datetime,
    count(1) as value,
    max(activation_local_datetime) as latest_refresh_datetime
from edw_prod.data_model.fact_activation r
join edw_prod.data_model.dim_store ds on r.store_id = ds.store_id
where activation_local_datetime >= $high_watermark_date
and is_retail_vip = false
group by 1,2,3,4)

select store_name, source, event, datetime, latest_refresh_datetime::datetime, sum(value) as value
from _edw where event = 'Complete Registration'
group by 1,2,3,4,5
union
select store_name, source, event, datetime, latest_refresh_datetime::datetime, sum(value) as value
from _edw where event = 'Purchase'
group by 1,2,3,4,5;

-- real-time from med61
create or replace temporary table _med61 as
with _med61_raw as (
select
    iff(region='EU',concat(brand,' EU'),concat(brand,' ',country)) as store_name,
    'Source' as source,
    'Complete Registration' as event,
    date_hour as datetime,
    max(max_refresh_timestamp_leads)::datetime as latest_refresh_datetime,
    sum(leads) as value
from reporting_base_prod.shared.med61_realtime_acquisition
where date_hour >= $high_watermark_date
and retail_customer = false
group by 1,2,3,4
union
select
    iff(region='EU',concat(brand,' EU'),concat(brand,' ',country)) as store_name,
    'Source' as source,
    'Purchase' as event,
    date_hour as datetime,
    max(max_refresh_timestamp_vips)::datetime as latest_refresh_datetime,
    sum(vips) as value
from reporting_base_prod.shared.med61_realtime_acquisition
where date_hour >= $high_watermark_date
and retail_customer = false
group by 1,2,3,4)

select store_name, source, event, datetime, max(latest_refresh_datetime) over (partition by store_name, event) as latest_refresh_datetime, value
from _med61_raw;

-- final table
create or replace transient table reporting_media_prod.dbo.data_source_comparison_anomaly_detection as
select * from _pixel
union
select * from _capi
union
select * from _segment
union
select * from _edw
union
select * from _med61;
