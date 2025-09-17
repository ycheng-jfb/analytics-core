
-- don't think this needs to be updated but check final spend / influencer name mapping

create or replace temporary table _spend as
select
    lower(sp.media_partner_id_shared_id) as unique_id,
    cast(case when sp.mens_flag = 1 then st.store_id || '011'
         when sp.is_scrubs_flag = 1 then st.store_id || '022'
         else st.store_id end as int) as media_store_id,
    sp.date,
    sum(sp.upfront_spend) as upfront_spend,
    sum(sp.cogs) as cogs
from reporting_media_base_prod.influencers.daily_spend sp
join edw_prod.data_model.dim_store st on st.store_id = sp.store_id
group by 1,2,3;


create or replace temporary table _leads as
select
     lower(media_partner_id) as unique_id,
     media_store_id,
     cast(event_datetime as date) as date,
     count(*) as click_through_leads
from reporting_media_base_prod.influencers.click_conversion_counts c
where event_type = 'Lead'
and event_description = 'Click'
group by 1,2,3;


create or replace temporary table _vips_from_leads30d as
select
    lower(media_partner_id) as unique_id,
    media_store_id,
    cast(event_datetime as date) as date,
    count(*) as click_through_vips_from_leads_30d
from reporting_media_base_prod.influencers.click_conversion_counts c
where event_type = 'VIP'
and event_description = 'Click-fromLead30D'
group by 1,2,3;

create or replace temporary table _vips_not_from_leads as
select
    lower(media_partner_id) as unique_id,
    media_store_id,
    cast(event_datetime as date) as date,
    count(*) as click_through_vips_same_session_not_from_lead_pool
from reporting_media_base_prod.influencers.click_conversion_counts c
where event_type = 'VIP'
and event_description = 'Click-notfromLead'
group by 1,2,3;

create or replace temporary table _session_counts as
select
    lower(media_partner_id) as unique_id,
    media_store_id,
    cast(event_datetime as date) as date,
    count(*) as organic_clicks
from reporting_media_base_prod.influencers.click_conversion_counts c
where event_type = 'Session'
and event_description = 'Session'
group by 1,2,3;


create or replace temporary table _unique_ids as
select distinct unique_id,media_store_id,date from _spend
union
select distinct unique_id,media_store_id,date from _leads
union
select distinct unique_id,media_store_id,date from _vips_from_leads30d
union
select distinct unique_id,media_store_id,date from _vips_not_from_leads
union
select distinct unique_id,media_store_id,date from _session_counts;


----------------------------------------------------------------------------

create or replace transient table reporting_media_prod.influencers.daily_performance_by_post as
select
    m.media_store_id,
    st.store_brand_name as business_unit,
    st.store_brand_abbr,
    st.store_country_abbr as country,
    st.store_region_abbr as region,
    m.date,
    lower(m.unique_id) as media_partner_id,
    null as shared_id,
    null as influencer_name,
    null as channel_tier,
    coalesce(s.upfront_spend,0) as upfront_cost,
    coalesce(s.cogs,0) as cogs,
    coalesce(l.click_through_leads,0) as click_through_leads,
    coalesce(v.click_through_vips_from_leads_30d,0) as click_through_vips_from_leads_30d,
    coalesce(vn.click_through_vips_same_session_not_from_lead_pool,0) as click_through_vips_same_session_not_from_lead_pool,
    coalesce(ses.organic_clicks,0) as organic_clicks,

    CURRENT_TIMESTAMP() as meta_update_datetime

from _unique_ids m
join reporting_media_prod.dbo.media_dim_store_flm_sessions st on st.media_store_id = m.media_store_id
left join _spend s on lower(s.unique_id) = lower(m.unique_id)
    and s.media_store_id = m.media_store_id
    and s.date = m.date
left join _leads l on lower(l.unique_id) = lower(m.unique_id)
    and l.media_store_id = m.media_store_id
    and l.date = m.date
left join _vips_from_leads30d v on lower(v.unique_id) = lower(m.unique_id)
    and v.media_store_id = m.media_store_id
    and v.date = m.date
left join _vips_not_from_leads vn on lower(vn.unique_id) = lower(m.unique_id)
    and vn.media_store_id = m.media_store_id
    and vn.date = m.date
left join _session_counts ses on ses.unique_id = m.unique_id
    and ses.media_store_id = m.media_store_id
    and ses.date = m.date
where m.date >= '2019-01-01';

--------------------------------------------------------------------

-- add in most recent media partner ID / influencer name / tier combo
create or replace temporary  table _final_mapping as
    select media_partner_id_shared_id as media_partner_id, channel, influencer_name
from (
select media_partner_id_shared_id, channel, influencer_name,
    row_number() over (partition by  media_partner_id_shared_id order by date desc) as rn
from reporting_media_base_prod.influencers.daily_spend)
where rn = 1;

update reporting_media_prod.influencers.daily_performance_by_post src
set channel_tier = _final_mapping.channel,
    influencer_name = _final_mapping.influencer_name
from _final_mapping where lower(_final_mapping.media_partner_id) =  lower(src.media_partner_id);

update reporting_media_prod.influencers.daily_performance_by_post
    set channel_tier = 'MissingInSpendDoc',
        influencer_name = 'MissingInSpendDoc',
        upfront_cost = 0,
        cogs = 0
where channel_tier is null;
