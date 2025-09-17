create or replace transient table reporting_media_prod.reddit.reddit_daily_click_optimization
(
    media_store_id int,
    store_brand_name varchar(55),
    store_region_abbr varchar(55),
    store_country_abbr varchar(55),
    date date,
    channel varchar(55),
    campaign_name varchar(255),
    adgroup_name varchar(255),
    ad_name varchar(255),
    leads number(38,0),
    vips_from_leads_30d number(38,0),
    vips_from_leads_1d number(38,0),
    vips_same_session number(38,0)
);

----------------------------------------------------------------------------
----------------------------------------------------------------------------

-- click through leads
create or replace temporary table _click_leads_reddit_flag as
select distinct
                l.store_id,
                l.customer_id,
                dc.gender,
                cast(case when lower(gender) = 'm' then l.store_id || '011' else l.store_id end as int) as media_store_id,
                session_id,
                cast(l.registration_local_datetime as date) as date,
                utm_source as channel,
                'lead' as membership_event,
                utm_campaign as campaign_name,
                replace(lower(utm_content),'adgroup_name_','') as adgroup_name,
                replace(lower(utm_term),'ad_name_','') as ad_name
from edw_prod.data_model.fact_registration l
join edw_prod.data_model.dim_customer dc on dc.customer_id = l.customer_id
    and dc.store_id = l.store_id
where lower(utm_medium) in ('paid_social_media','testing')
    and lower(utm_source) in ('reddit')
    and cast(l.registration_local_datetime as date) >= '2020-03-01'
    and is_secondary_registration = false;

create or replace temporary table _click_leads_reddit as
select
    media_store_id,
    store_id,
    customer_id,
    session_id,
    date,
    channel,
    membership_event,
    campaign_name,
    adgroup_name,
    ad_name
from _click_leads_reddit_flag;


-- vips from leads 30d
create or replace temporary table _vips_from_leads_30d_reddit as
select
    media_store_id,
    l.store_id,
    l.customer_id,
    l.session_id,
    date,
    channel,
    'vip from lead 30d' as membership_event,
    campaign_name,
    adgroup_name,
    ad_name
from _click_leads_reddit l
join edw_prod.data_model.fact_activation v on l.customer_id = v.customer_id
    and datediff(day,l.date,v.activation_local_datetime) < 30
    and l.store_id = v.store_id;

-- vips from leads 1d
create or replace temporary table _vips_from_leads_1d_reddit as
select
    media_store_id,
    l.store_id,
    l.customer_id,
    l.session_id,
    date,
    channel,
    'vip from lead 1d' as membership_event,
    campaign_name,
    adgroup_name,
    ad_name
from _click_leads_reddit l
join edw_prod.data_model.fact_activation v on l.customer_id = v.customer_id
    and datediff(day,l.date,v.activation_local_datetime) = 0
    and l.store_id = v.store_id;


-- same session vips
create or replace temporary table _click_vips_reddit_flag as
select distinct
                v.store_id,
                v.customer_id,
                dc.gender,
                cast(case when lower(gender) = 'm' then v.store_id || '011' else v.store_id end as int) as media_store_id,
                session_id,
                cast(activation_local_datetime as date) as date,
                utm_source as channel,
                'same session vip' as membership_event,
                utm_campaign as campaign_name,
                replace(lower(utm_content),'adgroup_name_','') as adgroup_name,
                replace(lower(utm_term),'ad_name_','') as ad_name
from edw_prod.data_model.fact_activation v
left join edw_prod.data_model.dim_customer dc on dc.customer_id = v.customer_id
    and dc.store_id = v.store_id
where lower(utm_medium) in ('paid_social_media','testing')
    and lower(utm_source) in ('reddit')
    and cast(activation_local_datetime as date) >= '2020-03-01';


create or replace temporary table _click_vips_reddit as
select
    media_store_id,
    store_id,
    customer_id,
    session_id,
    date,
    channel,
    membership_event,
    campaign_name,
    adgroup_name,
    ad_name
from _click_vips_reddit_flag;

----------------------------------------------------------------------------

create or replace temporary table _click_conversions_reddit as
select * from _click_leads_reddit
union
select * from _vips_from_leads_30d_reddit
union
select * from _vips_from_leads_1d_reddit
union
select * from _click_vips_reddit;

create or replace temporary table _pivot_columns as
select *,
       iff(membership_event = 'lead',1,0) as leads,
       iff(membership_event = 'vip from lead 30d',1,0) as vips_from_leads_30d,
       iff(membership_event = 'vip from lead 1d',1,0) as vips_from_leads_1d,
       iff(membership_event = 'same session vip',1,0) as vips_same_session
from _click_conversions_reddit;


insert into reporting_media_prod.reddit.reddit_daily_click_optimization
select
    p.media_store_id,
    store_brand_name,
    store_region_abbr,
    store_country_abbr,
    date,
    channel,
    campaign_name,
    adgroup_name,
    ad_name,
    sum(leads) as leads,
    sum(vips_from_leads_30d) as vips_from_leads_30d,
    sum(vips_from_leads_1d) as vips_from_leads_1d,
    sum(vips_same_session) as vips_same_session
from _pivot_columns p
join reporting_media_prod.dbo.media_dim_store_flm_sessions ds on ds.media_store_id = p.media_store_id
group by 1,2,3,4,5,6,7,8,9;
