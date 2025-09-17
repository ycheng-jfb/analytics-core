

create or replace temporary table _influencer_paid_spend_mappings as
select distinct
       media_partner_id,
       business_unit_abbr,
       iff(lower(business_unit_abbr) = 'flm',1,0) as mens_flag,
       iff(lower(business_unit_abbr) = 'scb',1,0) as is_scrubs_flag,
       region
from lake_view.sharepoint.med_organic_influencer_mapping -- new gsheet
where media_partner_id is not null;

update _influencer_paid_spend_mappings
    set media_partner_id = replace(media_partner_id,'|nosid','')
where media_partner_id ilike '%|nosid%';

------------------------------------------------------------------------

create or replace temporary table _spend AS
select
    lower(p.media_partner_id) as unique_id,
    cast(case when sp.mens_flag=1 then st.store_id || '011'
        when sp.is_scrubs_flag=1 then st.store_id || '022'
    else st.store_id end as int) as media_store_id,
    sp.date,
    sum(sp.upfront_spend) as upfront_spend,
    sum(sp.cogs) as cogs
from reporting_media_base_prod.influencers.daily_spend sp
join edw_prod.data_model.dim_store st on st.store_id = sp.store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(sp.media_partner_id_shared_id)
    and st.store_brand_abbr = case when p.business_unit_abbr in ('FLM','SCB') then 'FL' else p.business_unit_abbr end
    and sp.mens_flag = p.mens_flag
    and sp.is_scrubs_flag = p.is_scrubs_flag
    and st.store_region = p.region
group by 1,2,3
union
select
    lower(p.media_partner_id) as unique_id,
    cast(case when sp.mens_flag=1 then st.store_id || '011'
        when sp.is_scrubs_flag=1 then st.store_id || '022'
    else st.store_id end as int) as media_store_id,
    sp.date,
    sum(sp.upfront_spend) as upfront_spend,
    sum(sp.cogs) as cogs
from reporting_media_base_prod.influencers.daily_spend sp
join edw_prod.data_model.dim_store st on st.store_id = sp.store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(sp.media_partner_id)
    and st.store_brand_abbr = case when p.business_unit_abbr in ('FLM','SCB') then 'FL' else p.business_unit_abbr end
    and sp.mens_flag = p.mens_flag
    and sp.is_scrubs_flag = p.is_scrubs_flag
    and st.store_region = p.region
group by 1,2,3;


--------------------------------------------------------------------

create or replace temporary table _conversions AS
select
    lower(split_part(media_partner_id,'|',1)) as media_partner_id,
    lower(REPLACE(media_partner_id,'|nosid','')) as media_partner_id_shared_id,
    media_store_id,
    event_type,
    event_description,
    event_datetime
from reporting_media_base_prod.influencers.click_conversion_counts;

create or replace temporary table _new_conversions AS
select
    lower(p.media_partner_id) as unique_id,
    c.media_store_id,
    c.event_type,
    c.event_description,
    c.event_datetime
from _conversions c
join reporting_media_prod.dbo.media_dim_store_flm_sessions st on st.media_store_id = c.media_store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(c.media_partner_id_shared_id)
    and st.store_brand_abbr = p.business_unit_abbr
    and st.store_region_abbr = p.region
union
select
    lower(p.media_partner_id) as unique_id,
    c.media_store_id,
    c.event_type,
    c.event_description,
    c.event_datetime
from _conversions c
join reporting_media_prod.dbo.media_dim_store_flm_sessions st on st.media_store_id = c.media_store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(c.media_partner_id)
    and st.store_brand_abbr = p.business_unit_abbr
    and st.store_region_abbr = p.region;

--------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _leads as
select
     unique_id,
     media_store_id,
     CAST(event_datetime as date) as date,
     COUNT(*) as click_through_leads
from _new_conversions c
where event_type = 'Lead'
and event_description = 'Click'
group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _vips_from_leads30d as
select
    unique_id,
    media_store_id,
    CAST(event_datetime as date) as date,
    COUNT(*) as click_through_vips_from_leads_30d
from _new_conversions c
where event_type = 'VIP'
and event_description = 'Click-fromLead30D'
group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _vips_not_from_leads as
select
    unique_id,
    media_store_id,
    CAST(event_datetime as date) as date,
    COUNT(*) as click_through_vips_same_session_not_from_lead_pool
from _new_conversions c
where event_type = 'VIP'
and event_description = 'Click-notfromLead'
group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _session_counts as
select
     unique_id,
     media_store_id,
     CAST(event_datetime as date) as date,
     COUNT(*) as organic_clicks
from _new_conversions c
where event_type = 'Session'
and event_description = 'Session'
group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _hdyh_leads_raw as
select
     unique_id,
     media_store_id,
     CAST(event_datetime as date) as date,
     COUNT(*) as hdyh_leads_raw
from _new_conversions c
where event_type = 'Lead'
and event_description = 'HDYH'
group by 1,2,3;

CREATE OR REPLACE TEMPORARY TABLE _hdyh_vips_raw as
select
     unique_id,
     media_store_id,
     CAST(event_datetime as date) as date,
     COUNT(*) as hdyh_vips_raw
from _new_conversions c
where event_type = 'VIP'
and event_description = 'HDYH'
group by 1,2,3;


--------------------------------------------------------------------

-- activating revenue and order units
create or replace temporary table _rev_order_units as
select
    lower(split_part(media_partner_id,'|',1)) as media_partner_id,
    lower(replace(media_partner_id,'|nosid','')) as media_partner_id_shared_id,
    media_store_id,
    cast(event_datetime as date) as date,
    sum(revenue) as activating_order_revenue,
    sum(margin) as activating_order_margin,
    sum(order_units) as activating_order_units
from reporting_media_base_prod.influencers.click_conversion_counts
where event_type = 'VIP' and event_description != 'HDYH'
group by 1,2,3,4;

create or replace temporary table _rev_order_units_mapped as
select
    lower(p.media_partner_id) as unique_id,
    c.media_store_id,
    c.date,
    c.activating_order_revenue,
    c.activating_order_margin,
    c.activating_order_units
from _rev_order_units c
join reporting_media_prod.dbo.media_dim_store_flm_sessions st on st.media_store_id = c.media_store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(c.media_partner_id_shared_id)
    and st.store_brand_abbr = p.business_unit_abbr
    and st.store_region_abbr = p.region
union
select
    lower(p.media_partner_id) as unique_id,
    c.media_store_id,
    c.date,
    c.activating_order_revenue,
    c.activating_order_margin,
    c.activating_order_units
from _rev_order_units c
join reporting_media_prod.dbo.media_dim_store_flm_sessions st on st.media_store_id = c.media_store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(c.media_partner_id)
    and st.store_brand_abbr = p.business_unit_abbr
    and st.store_region_abbr = p.region;

create or replace temporary table _new_rev_order_units AS
SELECT
    unique_id,
    media_store_id,
    date,
    sum(activating_order_revenue) as activating_order_revenue,
    sum(activating_order_margin) as activating_order_margin,
    sum(activating_order_units) as activating_order_units
FROM _rev_order_units_mapped
group by 1,2,3;

--------------------------------------------------------------------

-- repeat revenue and order units
create or replace temporary table _rev_order_units_repeat as
select
    lower(split_part(media_partner_id,'|',1)) as media_partner_id,
    lower(replace(media_partner_id,'|nosid','')) as media_partner_id_shared_id,
    media_store_id,
    cast(event_datetime as date) as date,
    count(*) as repeat_orders,
    sum(revenue) as repeat_order_revenue,
    sum(margin) as repeat_order_margin,
    sum(order_units) as repeat_order_units
from reporting_media_base_prod.influencers.click_conversion_counts
where event_type = 'Repeat' and event_description = 'Click Purchase'
group by 1,2,3,4;

create or replace temporary table _rev_order_units_repeat_mapped as
select
    lower(p.media_partner_id) as unique_id,
    c.media_store_id,
    c.date,
    c.repeat_orders,
    c.repeat_order_revenue,
    c.repeat_order_margin,
    c.repeat_order_units
from _rev_order_units_repeat c
join reporting_media_prod.dbo.media_dim_store_flm_sessions st on st.media_store_id = c.media_store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(c.media_partner_id_shared_id)
    and st.store_brand_abbr = p.business_unit_abbr
    and st.store_region_abbr = p.region
union
select
    lower(p.media_partner_id) as unique_id,
    c.media_store_id,
    c.date,
    c.repeat_orders,
    c.repeat_order_revenue,
    c.repeat_order_margin,
    c.repeat_order_units
from _rev_order_units_repeat c
join reporting_media_prod.dbo.media_dim_store_flm_sessions st on st.media_store_id = c.media_store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(c.media_partner_id)
    and st.store_brand_abbr = p.business_unit_abbr
    and st.store_region_abbr = p.region;

create or replace temporary table _new_rev_order_units_repeat AS
SELECT
    unique_id,
    media_store_id,
    date,
    sum(repeat_orders) as repeat_orders,
    sum(repeat_order_revenue) as repeat_order_revenue,
    sum(repeat_order_margin) as repeat_order_margin,
    sum(repeat_order_units) as repeat_order_units
FROM _rev_order_units_repeat_mapped
group by 1,2,3;

--------------------------------------------------------------------
-- media spend on influencers

create or replace temporary table _all_channel_opt as
select lower(m.media_partner_id) as unique_id,
       cast(case when lower(o.store_brand_name) = 'fabletics men' then cast(o.store_id as int) || '011'
           when lower(o.store_brand_name) = 'fabletics scrubs' then cast(o.store_id as int) || '022'
           else cast(o.store_id as int) end as int) as media_store_id,
       date,
       channel,
       sum(spend_usd) as spend,
       sum(clicks) as clicks,
       sum(ui_optimization_pixel_lead) as pixel_leads,
       sum(ui_optimization_pixel_vip) as pixel_vips
from reporting_media_prod.dbo.all_channel_optimization o
join lake_view.sharepoint.med_organic_influencer_mapping m on lower(m.full_name) = lower(o.influencer_name)
    and lower(m.business_unit_abbr) = lower(o.store_brand_abbr)
    and o.region = m.region
where channel in ('fb+ig','youtube','tiktok','pinterest','snapchat')
    and media_partner_id is not null
    and store_id is not null
group by 1,2,3,4;

create or replace temporary table _infl_in_paid_media as
select unique_id,
       media_store_id,
       date,

       sum(iff(channel = 'fb+ig', spend, 0)) as fbig_spend,
       sum(iff(channel = 'fb+ig', clicks, 0)) as fbig_clicks,
       sum(iff(channel = 'fb+ig', pixel_leads, 0)) as fbig_pixel_leads,
       sum(iff(channel = 'fb+ig', pixel_vips, 0)) as fbig_pixel_vips,

       sum(iff(channel = 'youtube', spend, 0)) as youtube_spend,
       sum(iff(channel = 'youtube', clicks, 0)) as youtube_clicks,
       sum(iff(channel = 'youtube', pixel_leads, 0)) as youtube_pixel_leads,
       sum(iff(channel = 'youtube', pixel_vips, 0)) as youtube_pixel_vips,

       sum(iff(channel = 'snapchat', spend, 0)) as snapchat_spend,
       sum(iff(channel = 'snapchat', clicks, 0)) as snapchat_clicks,
       sum(iff(channel = 'snapchat', pixel_leads, 0)) as snapchat_pixel_leads,
       sum(iff(channel = 'snapchat', pixel_vips, 0)) as snapchat_pixel_vips,

       sum(iff(channel = 'tiktok', spend, 0)) as tiktok_spend,
       sum(iff(channel = 'tiktok', clicks, 0)) as tiktok_clicks,
       sum(iff(channel = 'tiktok', pixel_leads, 0)) as tiktok_pixel_leads,
       sum(iff(channel = 'tiktok', pixel_vips, 0)) as tiktok_pixel_vips,

       sum(iff(channel = 'pinterest', spend, 0)) as pinterest_spend,
       sum(iff(channel = 'pinterest', clicks, 0)) as pinterest_clicks,
       sum(iff(channel = 'pinterest', pixel_leads, 0)) as pinterest_pixel_leads,
       sum(iff(channel = 'pinterest', pixel_vips, 0)) as pinterest_pixel_vips

from _all_channel_opt
group by 1,2,3;

--------------------------------------------------------------------

create or replace temporary table _media_partner_ids as
select distinct unique_id,media_store_id,date from _spend
union
select distinct unique_id,media_store_id,date from _leads
union
select distinct unique_id,media_store_id,date from _vips_from_leads30d
union
select distinct unique_id,media_store_id,date from _vips_not_from_leads
union
select distinct unique_id,media_store_id,date from _hdyh_leads_raw
union
select distinct unique_id,media_store_id,date from _hdyh_vips_raw
union
select distinct unique_id,media_store_id,date from _session_counts
union
select distinct unique_id,media_store_id,date from _new_rev_order_units
union
select distinct unique_id,media_store_id,date from _new_rev_order_units_repeat
union
select distinct unique_id,media_store_id,date from _infl_in_paid_media;

--------------------------------------------------------------------

create or replace temporary table _output as
select
    m.date,
    m.unique_id as media_partner_id,
    m.media_store_id,
    st.store_brand_name as business_unit,
    st.store_brand_abbr,
    st.store_country_abbr as country,
    st.store_region_abbr as region,
    null as shared_id,
    null as influencer_name,
    null as channel_tier,

    coalesce(s.upfront_spend,0) as upfront_cost,
    coalesce(s.cogs,0) as cogs,
    coalesce(ses.organic_clicks,0) as organic_clicks,
    coalesce(l.click_through_leads,0) as click_through_leads,
    coalesce(v.click_through_vips_from_leads_30d,0) as click_through_vips_from_leads_30d,
    coalesce(vn.click_through_vips_same_session_not_from_lead_pool,0) as click_through_vips_same_session_not_from_lead_pool,

    coalesce(lh.hdyh_leads_raw,0) as hdyh_leads_raw,
    coalesce(vh.hdyh_vips_raw,0) as hdyh_vips_raw,

    coalesce(rou.activating_order_revenue,0) as activating_order_revenue,
    coalesce(rou.activating_order_margin,0) as activating_order_margin,
    coalesce(rou.activating_order_units,0) as activating_order_units,

    coalesce(rour.repeat_orders,0) as repeat_orders,
    coalesce(rour.repeat_order_revenue,0) as repeat_order_revenue,
    coalesce(rour.repeat_order_margin,0) as repeat_order_margin,
    coalesce(rour.repeat_order_units,0) as repeat_order_units,

    coalesce(fbig_spend,0) as fbig_spend,
    coalesce(fbig_clicks,0) as fbig_clicks,
    coalesce(fbig_pixel_leads,0) as fbig_pixel_leads,
    coalesce(fbig_pixel_vips,0) as fbig_pixel_vips,

    coalesce(youtube_spend,0) as youtube_spend,
    coalesce(youtube_clicks,0) as youtube_clicks,
    coalesce(youtube_pixel_leads,0) as youtube_pixel_leads,
    coalesce(youtube_pixel_vips,0) as youtube_pixel_vips,

    coalesce(pinterest_spend,0) as pinterest_spend,
    coalesce(pinterest_clicks,0) as pinterest_clicks,
    coalesce(pinterest_pixel_leads,0) as pinterest_pixel_leads,
    coalesce(pinterest_pixel_vips,0) as pinterest_pixel_vips,

    coalesce(snapchat_spend,0) as snapchat_spend,
    coalesce(snapchat_clicks,0) as snapchat_clicks,
    coalesce(snapchat_pixel_leads,0) as snapchat_pixel_leads,
    coalesce(snapchat_pixel_vips,0) as snapchat_pixel_vips,

    coalesce(tiktok_spend,0) as tiktok_spend,
    coalesce(tiktok_clicks,0) as tiktok_clicks,
    coalesce(tiktok_pixel_leads,0) as tiktok_pixel_leads,
    coalesce(tiktok_pixel_vips,0) as tiktok_pixel_vips,

    current_timestamp() as meta_update_datetime

from _media_partner_ids m
left join reporting_media_prod.dbo.media_dim_store_flm_sessions st on st.media_store_id = m.media_store_id
left join _spend s on s.unique_id = m.unique_id
    and s.media_store_id = m.media_store_id
    and s.date = m.date
left join _leads l on l.unique_id = m.unique_id
    and l.media_store_id = m.media_store_id
    and l.date = m.date
left join _vips_from_leads30d v on v.unique_id = m.unique_id
    and v.media_store_id = m.media_store_id
    and v.date = m.date
left join _vips_not_from_leads vn on vn.unique_id = m.unique_id
    and vn.media_store_id = m.media_store_id
    and vn.date = m.date
left join _hdyh_leads_raw lh on lh.unique_id = m.unique_id
   and lh.media_store_id = m.media_store_id
   and lh.date = m.date
left join _hdyh_vips_raw vh on vh.unique_id = m.unique_id
   and vh.media_store_id = m.media_store_id
   and vh.date = m.date
left join _session_counts ses on ses.unique_id = m.unique_id
    and ses.media_store_id = m.media_store_id
    and ses.date = m.date
left join _new_rev_order_units rou on rou.unique_id = m.unique_id
    and rou.media_store_id = m.media_store_id
    and rou.date = m.date
left join _new_rev_order_units_repeat rour on rour.unique_id = m.unique_id
    and rour.media_store_id = m.media_store_id
    and rour.date = m.date
left join _infl_in_paid_media fb on fb.unique_id = m.unique_id
    and fb.media_store_id = m.media_store_id
    and fb.date = m.date;

--------------------------------------------------------------------

-- add in most recent media partner ID / influencer name / tier combo

create or replace temporary table _spend_mapped as
select
    lower(p.media_partner_id) as media_partner_id,
    sp.date,
    sp.channel,
    sp.influencer_name
from reporting_media_base_prod.influencers.daily_spend sp
join edw_prod.data_model.dim_store st on st.store_id = sp.store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(sp.media_partner_id_shared_id)
    and st.store_region = p.region
    and st.store_brand_abbr = case when p.business_unit_abbr in ('FLM','SCB') then 'FL' else p.business_unit_abbr end
    and sp.mens_flag = p.mens_flag
    and sp.is_scrubs_flag = p.is_scrubs_flag
union
select
    lower(p.media_partner_id) as media_partner_id,
    sp.date,
    sp.channel,
    sp.influencer_name
from reporting_media_base_prod.influencers.daily_spend sp
join edw_prod.data_model.dim_store st on st.store_id = sp.store_id
join _influencer_paid_spend_mappings p on lower(p.media_partner_id) = lower(sp.media_partner_id)
    and st.store_region = p.region
    and st.store_brand_abbr = case when p.business_unit_abbr in ('FLM','SCB') then 'FL' else p.business_unit_abbr end
    and sp.mens_flag = p.mens_flag
    and sp.is_scrubs_flag = p.is_scrubs_flag;


create or replace temporary  table _final_mapping as
    select media_partner_id, channel, influencer_name
from (
select media_partner_id, channel, influencer_name,
    row_number() over (partition by media_partner_id order by date desc) as rn
from _spend_mapped)
where rn = 1;

update _output src
set channel_tier = _final_mapping.channel,
    influencer_name = _final_mapping.influencer_name
from _final_mapping where lower(_final_mapping.media_partner_id) =  lower(src.media_partner_id);

UPDATE _output
    SET channel_tier = 'MissingInSpendDoc',
        influencer_name = 'MissingInSpendDoc',
        upfront_cost = 0,
        cogs = 0
WHERE channel_tier is null;


----------------------------------------------------------------------------
-- insert status column for fabletics men promoters

create or replace temporary table _flm_micro_status AS
select distinct unique_id, status
from lake_view.sharepoint.med_microinfluencer_links_flm;

----------------------------------------------------------------------------
-- final table
create or replace transient table reporting_media_prod.influencers.daily_performance_by_influencer as
select i.*, status as flm_micro_status
from _output i
left join _flm_micro_status s on lower(s.unique_id) = lower(i.media_partner_id);
