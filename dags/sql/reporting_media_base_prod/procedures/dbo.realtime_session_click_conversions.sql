-- this proc generates two reporting base prod tables:
    -- 1) realtime_sessions: realtime daily sessions by session ID (only segment sessions included)
    -- 2) realtime_session_click_conversions: daily session click conversions by utm identifier (ad id or campaign name)

set max_session_datetime_incr = (select max(session_datetime) from reporting_media_base_prod.dbo.realtime_sessions);

----------------------------------------------------------------------------
-- 1) realtime_sessions (incremental):
-- session utms are re-parsed to match correct utm structure so we can tie back to ui metadata
-- channels: facebook, tiktok, google, and snap (all else in other bucket)

create or replace temporary table _sessions_base as
select distinct edw_prod.stg.udf_unconcat_brand(s.session_id) as meta_original_session_id
from lake_consolidated_view.ultra_merchant.session s
where s.datetime_added > $max_session_datetime_incr;

create or replace temporary table _segment_sessions as
select distinct meta_original_session_id
from
(select distinct try_to_number(properties_session_id) as meta_original_session_id
 from lake.segment_fl.javascript_fabletics_page
 where try_to_number(properties_session_id) in (select distinct meta_original_session_id from _sessions_base)
 union
 select distinct try_to_number(properties_session_id) as meta_original_session_id
 from lake.segment_fl.react_native_fabletics_screen
 where try_to_number(properties_session_id) in (select distinct meta_original_session_id from _sessions_base)
 union
 select distinct try_to_number(properties_session_id) as meta_original_session_id
 from lake.segment_gfb.react_native_justfab_screen
 where try_to_number(properties_session_id) in (select distinct meta_original_session_id from _sessions_base)
 union
 select distinct try_to_number(properties_session_id) as meta_original_session_id
 from lake.segment_gfb.javascript_justfab_page
 where try_to_number(properties_session_id) in (select distinct meta_original_session_id from _sessions_base)
 union
 select distinct try_to_number(properties_session_id) as meta_original_session_id
 from lake.segment_gfb.javascript_fabkids_page
 where try_to_number(properties_session_id) in (select distinct meta_original_session_id from _sessions_base)
 union
 select distinct try_to_number(properties_session_id) as meta_original_session_id
 from lake.segment_gfb.javascript_shoedazzle_page
 where try_to_number(properties_session_id) in (select distinct meta_original_session_id from _sessions_base)
 union
 select distinct try_to_number(properties_session_id) as meta_original_session_id
 from lake.segment_sxf.javascript_sxf_page
 where try_to_number(properties_session_id) in (select distinct meta_original_session_id from _sessions_base));

create or replace temporary table _sessions as
select
    ss.*,
    s.session_id,
    s.store_id,
    s.datetime_added,
    su.referer,
    su.uri
from _segment_sessions ss
left join lake_consolidated_view.ultra_merchant.session_uri su on ss.meta_original_session_id = edw_prod.stg.udf_unconcat_brand(su.session_id)
left join lake_consolidated_view.ultra_merchant.session s on ss.meta_original_session_id = s.meta_original_session_id;

create or replace temporary table _uri as
  select session_id,
         store_id,
         datetime_added,
         'https://' || reporting_media_base_prod.dbo.udf_decode_url(reporting_media_base_prod.dbo.udf_decode_url(uri)) as uri
  from _sessions
  where uri is not null
  union
  select session_id,
         store_id,
         datetime_added,
         reporting_media_base_prod.dbo.udf_decode_url(reporting_media_base_prod.dbo.udf_decode_url(referer)) as uri
  from _sessions
  where referer ilike 'http%%?%%';

update _uri
set uri = insert(uri, position(lower('utm_medium'), lower(uri), 1), 0, '&')
where uri ilike '%%utm_medium%%';

create or replace temporary table _uri_parameters as
select
    l.session_id,
    l.store_id,
    l.datetime_added,
    lower(f.key) as key,
    f.value :: varchar as value
from _uri l,
    lateral flatten(input => parse_url(l.uri,1)['parameters']) f
where f.key ilike any ('utm_medium','utm_source','utm_campaign','utm_content','utm_term');

create or replace temporary table _final_sessions as
select
    edw_prod.stg.udf_unconcat_brand(session_id) as meta_original_session_id,
    session_id,
    store_id,
    datetime_added,
    lower(utm_source) as utm_source,
    lower(utm_medium) as utm_medium,
    lower(utm_campaign) as utm_campaign,
    lower(utm_content) as utm_content,
    lower(utm_term) as utm_term
from _uri_parameters
pivot(max(value) for key in ('utm_medium','utm_source','utm_campaign','utm_content','utm_term')) as p
    (session_id,store_id,datetime_added,utm_medium,utm_source,utm_campaign,utm_content,utm_term);

insert into reporting_media_base_prod.dbo.realtime_sessions
select
    meta_original_session_id,
    session_id,
    s.store_id,
    store_brand as store_brand_name,
    store_region,
    store_country,
    datetime_added as session_datetime,
    -- youtube
    case when (utm_medium = 'youtube' and (utm_source ilike '%%google%%' or utm_source ilike '%%warnermusic%%' or utm_source is null))
            or (utm_medium = 'google' and utm_source = 'youtube')
            or (utm_medium ilike '%%youtube%%'  and utm_source = 'google')
            or utm_campaign ilike '%%ytube%%'
        then 'youtube'
        -- gdn
        when utm_medium ='gdn' and utm_source = 'google'
            or (utm_medium = 'google' and utm_source ilike any ('%%gdn%%','%%display%%'))
            or (utm_medium ='display' and utm_source ilike any ('%%gdn%%','%%google%%'))
            or (utm_medium ilike '%%display%%' and utm_source = 'google') then 'display'
        when utm_campaign ilike '%|gdn|_%' escape '|' then 'display'
        -- discovery
        when utm_medium = 'discovery'
            or (utm_medium = 'google' and utm_source ilike '%discovery%')
            or utm_campaign ilike '%|_discovery|_%' escape '|'
        then 'discovery'
        -- pmax
        when contains(utm_campaign,'_pmax') or contains(utm_campaign,'performancemax')
            or contains(utm_medium,'performance_max')
        then 'pmax'
        -- shopping
        when (utm_medium ilike '%%shopping%%' and utm_source ilike '%%google%%')
            or (utm_source = 'shopping' and utm_medium ilike '%%google%%')
        then 'shopping'
        -- non branded search
        when ((utm_medium in ('search_non_branded','nonbrandsearch') or utm_medium ilike '%%search_nonbrand%%')
            and utm_source ilike '%%google%%')
            or (utm_source ilike '%%search_nonbrand%%' and utm_medium ilike '%%google%%')
        then 'non branded search'
        -- branded search
        when (utm_medium = 'search_branded' and utm_source ilike '%%google%%')
            or (utm_source = 'search_branded' and utm_medium ilike '%%google%%')
        then 'branded search'
        -- campaign parsing
        when utm_campaign ilike '%%pla%%' then 'shopping'
        when utm_campaign ilike any ('%%nb%%','%%nonbrand%%') then 'non branded search'
        when utm_campaign ilike '%%brand%%' then 'branded search'
        -- tiktok, snapchat, facebook, pinterest
        when utm_source in ('tiktok','snapchat','twitter','pinterest') then lower(utm_source)
        when contains(utm_term,'fb_ad_id') or contains(utm_source,'facebook') then 'fb+ig'
    else 'other' end as channel,
    iff(channel in ('fb+ig','tiktok','snapchat','twitter','other','pinterest'),channel,'google') as subchannel,
    regexp_replace(utm_campaign,'(fb_campaign_id_|fb_campaign_id|twitter_campaign_id_|pin_campaign_id_)','') as campaign_id,
    regexp_replace(utm_content,'(fb_adset_id_|fb_adset_id|twitter_adgroup_id_|pin_adgroup_id_)','') as adgroup_id,
    regexp_replace(utm_term,'(fb_ad_id_|yt_ad_id_|fb_ad_id|yt_ad_id|twitter_ad_id_|pin_id_)','') as ad_id
from _final_sessions s
join edw_prod.data_model.dim_store ds on ds.store_id = s.store_id;

---------------------------------------------------------------------------
-- 2) realtime_session_click_conversions (last 30 days only):
-- for google channels (besides youtube), the utm grain is campaign_id = campaign_name
-- for all other channels, the utm grain is utm_term or the ad id

set lead_registration_meta_update_datetime = (select max(datetime_added) from lake_consolidated_view.ultra_merchant.membership_signup);
set activation_meta_update_datetime = (select max(datetime_activated) from lake_consolidated_view.ultra_merchant.membership);
set latest_session_time = (select max(session_datetime) from reporting_media_base_prod.dbo.realtime_sessions);

create or replace temporary table _lead_base as
select distinct
    s.session_id,
    s.session_datetime,
    ms.customer_id,
    ms.datetime_added as datetime_added,
    row_number() over(partition by s.session_id order by customer_id) as rn
from reporting_media_base_prod.dbo.realtime_sessions s
join lake_consolidated_view.ultra_merchant.membership_signup ms on s.session_id = ms.session_id
and ms.membership_signup_id is not null
and ms.customer_id is not null
where s.channel != 'other'
and session_datetime > current_date() - 30
qualify row_number() over(partition by s.session_id order by customer_id) = 1;

create or replace temporary table _realtime_performance_by_ad_id as
select
    s.session_datetime as date,
    s.store_id,
    s.channel,
    s.subchannel,
    cast(ad_id as varchar) as ad_id,
    count(*) as click_through_sessions,
    count(iff(ms.customer_id is not null,1,null)) as leads,
    count(iff(m.datetime_activated is not null,1,null)) as vips_from_leads_1d,
    count(iff(m1.datetime_activated is not null,1,null)) as vips_from_leads_7d,
    count(iff(m2.datetime_activated is not null,1,null)) as vips_from_leads_30d
from (select * from reporting_media_base_prod.dbo.realtime_sessions
      where session_datetime > current_date() - 30
      and channel in ('fb+ig','snapchat','tiktok','youtube','twitter','pinterest')
     ) s
left join _lead_base ms on ms.session_id = s.session_id
left join lake_consolidated_view.ultra_merchant.membership m on m.customer_id = ms.customer_id
    and datediff(day,ms.datetime_added,m.datetime_activated) + 1 = 1
left join lake_consolidated_view.ultra_merchant.membership m1 on m1.customer_id = ms.customer_id
    and datediff(day,ms.datetime_added,m1.datetime_activated) < 7
left join lake_consolidated_view.ultra_merchant.membership m2 on m2.customer_id = ms.customer_id
    and datediff(day,ms.datetime_added,m2.datetime_activated) < 30
group by 1,2,3,4,5;

create or replace temporary table _realtime_performance_by_campaign_id as
select
    s.session_datetime as date,
    s.store_id,
    s.channel,
    s.subchannel,
    cast(campaign_id as varchar) as campaign_id,
    count(*) as click_through_sessions,
    count(iff(ms.customer_id is not null,1,null)) as leads,
    count(iff(m.datetime_activated is not null,1,null)) as vips_from_leads_1d,
    count(iff(m1.datetime_activated is not null,1,null)) as vips_from_leads_7d,
    count(iff(m2.datetime_activated is not null,1,null)) as vips_from_leads_30d
from (select * from reporting_media_base_prod.dbo.realtime_sessions
      where session_datetime > current_date() - 30
      and channel in  ('discovery','display','shopping','non branded search','branded search','pmax')
     ) s
left join _lead_base ms on ms.session_id = s.session_id
left join lake_consolidated_view.ultra_merchant.membership m on m.customer_id = ms.customer_id
    and datediff(day,ms.datetime_added,m.datetime_activated) + 1 = 1
left join lake_consolidated_view.ultra_merchant.membership m1 on m1.customer_id = ms.customer_id
    and datediff(day,ms.datetime_added,m1.datetime_activated) < 7
left join lake_consolidated_view.ultra_merchant.membership m2 on m2.customer_id = ms.customer_id
    and datediff(day,ms.datetime_added,m2.datetime_activated) < 30
group by 1,2,3,4,5;

create or replace transient table reporting_media_base_prod.dbo.realtime_session_click_conversions as
select
    store_id,
    to_date(date) as date,
    channel,
    subchannel,
    ad_id as utm_identifier,
    sum(click_through_sessions) as click_through_sessions,
    sum(leads) as leads,
    sum(vips_from_leads_1d) as vips_from_leads_1d,
    sum(vips_from_leads_7d) as vips_from_leads_7d,
    sum(vips_from_leads_30d) as vips_from_leads_30d,
    $latest_session_time,
    $lead_registration_meta_update_datetime,
    $activation_meta_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime
from _realtime_performance_by_ad_id where ad_id is not null
group by 1,2,3,4,5,11,12,13,14,15
union
select
    store_id,
    to_date(date) as date,
    channel,
    subchannel,
    campaign_id as utm_identifier,
    sum(click_through_sessions) as click_through_sessions,
    sum(leads) as leads,
    sum(vips_from_leads_1d) as vips_from_leads_1d,
    sum(vips_from_leads_7d) as vips_from_leads_7d,
    sum(vips_from_leads_30d) as vips_from_leads_30d,
    $latest_session_time,
    $lead_registration_meta_update_datetime,
    $activation_meta_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime
from _realtime_performance_by_campaign_id where campaign_id is not null
group by 1,2,3,4,5,11,12,13,14,15;
