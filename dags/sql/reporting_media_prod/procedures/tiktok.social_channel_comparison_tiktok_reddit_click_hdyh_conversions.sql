
set min_date = '2020-03-01';
set max_date = cast(current_timestamp() as date);
set meta_update_datetime = (select max(meta_update_datetime) from reporting_media_prod.dbo.fact_media_cost);

-- social channel comparison -- fb+ig, pinterest, snap, reddit, tiktok, twitter
-- click and hdyh deduped, favoring click conversions
create or replace temporary table _click_leads as
select distinct
                l.store_id,
                l.customer_id,
                cast(case when gender = 'M' then ds.store_id || '011' else ds.store_id end as int) as media_store_id,
                session_id,
                cast(l.registration_local_datetime as date) as reg_date,
                cast(l.registration_local_datetime as date) as act_date,
                case when lower(utm_source) = 'pinterest' then 'pinterest'
                    when lower(utm_source) = 'snapchat' then 'snapchat'
                    when lower(utm_source) = 'tiktok' then 'tiktok'
                    when lower(utm_source) = 'reddit' then 'reddit'
                    when lower(utm_source) = 'twitter' then 'twitter'
                    when lower(UTM_MEDIUM) = 'podcast' then 'radio/podcast'
                    else 'meta' end as channel,
                'lead' as event_type,
                'click' as event_description
from edw_prod.data_model_jfb.fact_registration l
join edw_prod.data_model_jfb.dim_store ds on l.store_id = ds.store_id
join edw_prod.data_model_jfb.dim_customer dc on dc.customer_id = l.customer_id
where (lower(utm_medium) = 'paid_social_media'
           or lower(utm_medium) = 'podcast'
           or (lower(utm_source) = 'reddit' and lower(utm_medium) = 'testing'))
    and lower(utm_source) not in ('shopmessage')
    and cast(l.registration_local_datetime as date) >= $min_date
    and ds.store_country = 'US'
    and l.is_secondary_registration = false;

create or replace temporary table _vips_from_click_leads as
-- vips from primary leads within 30 days
select distinct
                l.store_id,
                l.customer_id,
                l.media_store_id,
                l.session_id, -- registration session
                l.reg_date,
                cast(activation_local_datetime as date) as act_date,
                l.channel,
                'vip' as event_type,
                'click' as event_description
from _click_leads l
join edw_prod.data_model_jfb.fact_activation v on l.customer_id = v.customer_id
        and datediff(day,l.reg_date,cast(activation_local_datetime as date)) < 30
        and l.store_id = v.store_id
join edw_prod.data_model_jfb.dim_store ds on v.store_id = ds.store_id
where cast(activation_local_datetime as date) >= $min_date
    and store_country = 'US';

-- hdyh conversions from fb+ig, pinterest, snap, reddit, tiktok, twitter
create or replace temporary table _hdyh_leads as
select distinct
                l.store_id,
                l.customer_id,
                cast(case when dc.gender = 'M' then cast(ds.store_id as string) || '011' else ds.store_id end as int) as media_store_id,
                session_id,
                cast(l.registration_local_datetime as date) as reg_date,
                cast(l.registration_local_datetime as date) as act_date,
                h.channel,
                'lead' as event_type,
                'hdyh' as event_description
from edw_prod.data_model_jfb.fact_registration l
join edw_prod.data_model_jfb.dim_store ds on l.store_id = ds.store_id
join edw_prod.data_model_jfb.dim_customer dc on dc.customer_id = l.customer_id
    and dc.store_id = l.store_id
left join reporting_media_base_prod.dbo.vw_med_hdyh_mapping_tsos h on h.hdyh_value = lower(replace(l.how_did_you_hear_parent,' ',''))
    and lower(h.channel) in ('meta','tiktok','pinterest','snapchat','twitter','reddit', 'radio/podcast')
where cast(l.registration_local_datetime as date) >= $min_date
    and store_country = 'US'
    and l.is_secondary_registration = false;


create or replace temporary table _hdyh_all_vips as
select distinct
                l.store_id,
                l.customer_id,
                l.media_store_id,
                l.session_id, -- registration session
                l.reg_date,
                cast(activation_local_datetime as date) as act_date, -- activation date
                channel,
                row_number() over(partition by v.customer_id order by activation_local_datetime asc) as rn_customer
from _hdyh_leads l
join edw_prod.data_model_jfb.fact_activation v on v.customer_id = l.customer_id
    and datediff(day, l.reg_date, v.activation_local_datetime) < 30
    and l.store_id = v.store_id
where
    l.channel is not null;


create or replace temporary table _hdyh_vips as
select
                store_id,
                customer_id,
                media_store_id,
                session_id,
                reg_date,
                act_date,
                channel,
                'vip' as event_type,
                'hdyh' as event_description
from _hdyh_all_vips
where rn_customer = 1;

create or replace temporary table _channel_click_hdyh_conversion_counts
(
    store_id int,
    customer_id varchar(55),
    media_store_id int,
    session_id varchar(55),
    reg_date date,
    act_date date,
    channel varchar(55),
    event_type varchar(55),
    event_description varchar(55)
);

insert into _channel_click_hdyh_conversion_counts
select l.*
from _click_leads l
left join _channel_click_hdyh_conversion_counts cl on cl.customer_id = l.customer_id
    and cl.event_type = l.event_type
where cl.customer_id is null;

insert into _channel_click_hdyh_conversion_counts
select v.*
from _vips_from_click_leads v
left join _channel_click_hdyh_conversion_counts cl on cl.customer_id = v.customer_id
    and cl.event_type = v.event_type
where cl.customer_id is null;

insert into _channel_click_hdyh_conversion_counts
select hl.*
from _hdyh_leads hl
left join _channel_click_hdyh_conversion_counts cl on cl.customer_id = hl.customer_id
    and cl.event_type = hl.event_type
where cl.customer_id is null
and hl.channel is not null;

insert into _channel_click_hdyh_conversion_counts
select hv.*
from _hdyh_vips hv
left join _channel_click_hdyh_conversion_counts cl on cl.customer_id = hv.customer_id
    and cl.event_type = hv.event_type
where cl.customer_id is null;

create or replace temporary table _pivot_event_types as
select
    store_id,
    customer_id,
    media_store_id,
    session_id,
    reg_date,
    act_date,
    channel,
    event_type,
    case when event_type = 'lead' and event_description = 'click' then 1 else 0 end as click_leads,
    case when event_type = 'lead' and event_description = 'hdyh' then 1 else 0 end as hdyh_leads,
    case when event_type = 'vip' and event_description = 'click' then 1 else 0 end as click_vips_30d,
    case when event_type = 'vip' and event_description = 'click' and to_date(reg_date) = to_date(act_date) then 1 else 0 end as click_vips_1d,
    case when event_type = 'vip' and event_description = 'hdyh' then 1 else 0 end as hdyh_vips_30d,
    case when event_type = 'vip' and event_description = 'hdyh' and to_date(reg_date) = to_date(act_date) then 1 else 0 end as hdyh_vips_1d
from _channel_click_hdyh_conversion_counts;

--temp table for leads
create or replace temporary table _leads as
select
    media_store_id,
    reg_date as date,
    lower(channel) as channel,
    sum(click_leads) as click_leads,
    sum(hdyh_leads) as hdyh_leads
from _pivot_event_types
where event_type = 'lead'
group by 1,2,3;

-- temp table for vips
create or replace temporary table _vips as
select
    media_store_id,
    act_date as date,
    lower(channel) as channel,
    sum(click_vips_30d) as click_vips_30d,
    sum(click_vips_1d) as click_vips_1d,
    sum(hdyh_vips_30d) as hdyh_vips_30d,
    sum(hdyh_vips_1d) as hdyh_vips_1d
from _pivot_event_types
where event_type = 'vip'
group by 1,2,3;

-- combos select distinct store, date from each
create or replace temporary table _conv_combos as
select distinct
media_store_id,date,channel
from _leads
union
select distinct
media_store_id, date, channel
from _vips;

-- final becomes combos with leads and vips left joined
create or replace temporary table _final_conversions as
select
    c.media_store_id,
    c.date,
    c.channel,
    sum(click_leads) as click_leads,
    sum(hdyh_leads) as hdyh_leads,
    sum(click_vips_30d) as vips_from_click_leads_30d,
    sum(click_vips_1d) as vips_from_click_leads_1d,
    sum(hdyh_vips_30d) as hdyh_vips_30d,
    sum(hdyh_vips_1d) as hdyh_vips_1d
from _conv_combos c
left join _leads l on c.media_store_id = l.media_store_id
                          and c.date = l.date
                          and c.channel = l.channel
left join _vips v on c.media_store_id = v.media_store_id
                         and c.date = v.date
                         and c.channel = v.channel
group by 1,2,3;


----------------------------------------------------------------------------

create or replace temporary table _fmc as
select store_id,
       is_mens_flag,
       cast(case when is_mens_flag=1 then store_id || '011' else store_id end as int) as media_store_id,
       channel,
       cast(media_cost_date as date) as date,
       sum(cost) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks
from reporting_media_prod.dbo.vw_fact_media_cost
where channel in ('fb+ig','snapchat','pinterest','reddit','tiktok','twitter','radio/podcast')
    and media_cost_date >= $min_date
    and media_cost_date < $max_date
group by 1,2,3,4,5;

create or replace temporary table _social_channel_spend as
select
    fmc.media_store_id,
    channel,
    date,
    spend,
    impressions,
    clicks
from _fmc fmc
join reporting_media_prod.dbo.media_dim_store_flm_sessions mds on mds.media_store_id = fmc.media_store_id
where store_country_abbr = 'US';

----------------------------------------------------------------------------

create or replace temporary table _combos as
select media_store_id, date, lower(channel) as channel from _final_conversions
union
select media_store_id, date, channel from _social_channel_spend;

create or replace temporary table _final_metrics as
select
    c.media_store_id,
    c.date,
    c.channel,
    s.spend,
    s.impressions,
    s.clicks,
    fc.click_leads,
    fc.vips_from_click_leads_30d,
    fc.vips_from_click_leads_1d,
    fc.hdyh_leads,
    fc.hdyh_vips_30d,
    fc.hdyh_vips_1d
from _combos c
left join _social_channel_spend s on s.media_store_id = c.media_store_id
    and s.date = c.date
    and s.channel = c.channel
left join _final_conversions fc on c.media_store_id = fc.media_store_id
    and c.date = fc.date
    and c.channel = lower(fc.channel);

create or replace transient table reporting_media_prod.tiktok.social_channel_comparison_tiktok_reddit_click_hdyh_conversions as
select f.*,
       store_brand_name,
       store_region_abbr as region,
       store_country_abbr as country,
       $meta_update_datetime as meta_update_datetime
from _final_metrics f
join reporting_media_prod.dbo.media_dim_store_flm_sessions ds on ds.media_store_id = f.media_store_id;
