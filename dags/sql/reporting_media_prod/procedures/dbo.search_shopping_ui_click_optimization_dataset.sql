set start_date = date_trunc(year, dateadd(year, -1, current_date()));
set end_date = current_date();

------------------------------------------------------------------------------------
/* get UI data for google and microsoft and union into a single table */

create or replace temporary table _google_ads as
select store_brand_name,
       region,
       country,
       account_name,
       channel,
       subchannel,
       campaign_name,
       campaign_id,
       date,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_lead_1dc_click) as pixel_lead_1dc,
       sum(pixel_lead_30dc_click) as pixel_lead_30dc,
       sum(pixel_vip_1dc_click) as pixel_vip_1dc,
       sum(pixel_vip_30dc_click) as pixel_vip_30dc
from reporting_media_prod.google_ads.search_optimization_dataset
where lower(store_brand_abbr) in ('fl','flm','scb','yty')
    and lower(region) = 'na'
    and date >= $start_date
    and date < $end_date
group by 1,2,3,4,5,6,7,8,9;

create or replace temporary table _microsoft_ads as
select store_brand_name,
       region,
       country,
       account_name,
       channel,
       subchannel,
       campaign_name,
       campaign_id,
       date,
       sum(spend_usd) as spend,
       sum(impressions) as impressions,
       sum(clicks) as clicks,
       sum(pixel_lead_1dc) as pixel_lead_1dc,
       sum(pixel_lead_30dc) as pixel_lead_30dc,
       sum(pixel_vip_1dc) as pixel_vip_1dc,
       sum(pixel_vip_30dc) as pixel_vip_30dc
from reporting_media_prod.microsoft_ads.microsoft_ads_optimization_dataset
where lower(store_brand_abbr) in ('fl','flm','scb','yty')
    and lower(region) = 'na'
    and date >= $start_date
    and date < $end_date
group by 1,2,3,4,5,6,7,8,9;

create or replace temporary table _final_ui_data as
select store_brand_name as ad_store,
       region,
       country,
       channel,
       subchannel,
       account_name,
       campaign_name,
       campaign_id,
       date,
       zeroifnull(spend) as spend,
       impressions,
       clicks,
       pixel_lead_1dc,
       pixel_lead_30dc,
       pixel_vip_1dc,
       pixel_vip_30dc
from
    (select * from _google_ads
    union
    select * from _microsoft_ads);

------------------------------------------------------------------------------------
/* get all click leads and vips coming from google or microsoft search & shopping */

create or replace temporary table _click_leads as
select distinct case when lower(ds.store_brand_abbr) = 'fl' and is_scrubs_customer = true then 'Fabletics Scrubs'
                     when lower(ds.store_brand_abbr) = 'fl' and lower(gender) = 'm' then 'Fabletics Men'
                     else ds.store_brand end as store_brand_name,
                ds.store_region as region,
                ds.store_country as country,
                r.customer_id,
                0 as order_id,
                r.registration_local_datetime::date as date,
                r.registration_channel as channel,
                r.registration_subchannel as subchannel,
                r.utm_medium,
                r.utm_source,
                r.utm_campaign,
                r.utm_term,
                'lead' as event_type,
                'click' as event_description,
                1 as event_count
from edw_prod.data_model.fact_registration r
join edw_prod.data_model.dim_store ds on ds.store_id = r.store_id
join edw_prod.data_model.dim_customer c on c.customer_id = r.customer_id
where lower(ds.store_brand_abbr) in ('fl','yty')
    and lower(ds.store_region) = 'na'
    and r.is_secondary_registration = false
    and r.is_retail_registration = false
    and r.is_fake_retail_registration = false
    and r.registration_local_datetime::date >= $start_date
    and r.registration_local_datetime::date < $end_date
    and r.registration_channel in ('branded search','non branded search','shopping');

create or replace temporary table _d1_vips_from_click_leads as
select distinct r.store_brand_name,
                r.region,
                r.country,
                r.customer_id,
                a.order_id,
                r.date,
                r.channel,
                r.subchannel,
                r.utm_medium,
                r.utm_source,
                r.utm_campaign,
                r.utm_term,
                'vip' as event_type,
                'd1 from click leads' as event_description,
                1 as event_count
from _click_leads r
join edw_prod.data_model.fact_activation a on a.customer_id = r.customer_id
    and datediff(day,r.date,a.source_activation_local_datetime) = 0
where a.is_retail_vip = false;

create or replace temporary table _total_vips_from_click_leads as
select distinct r.store_brand_name,
                r.region,
                r.country,
                r.customer_id,
                a.order_id,
                r.date,
                r.channel,
                r.subchannel,
                r.utm_medium,
                r.utm_source,
                r.utm_campaign,
                r.utm_term,
                'vip' as event_type,
                'total from click leads' as event_description,
                1 as event_count
from _click_leads r
join edw_prod.data_model.fact_activation a on a.customer_id = r.customer_id
where a.is_retail_vip = false;

create or replace temporary table _click_vips_not_from_lead_pool as
select distinct case when lower(ds.store_brand_abbr) = 'fl' and is_scrubs_customer = true then 'Fabletics Scrubs'
                     when lower(ds.store_brand_abbr) = 'fl' and lower(gender) = 'm' then 'Fabletics Men'
                     else ds.store_brand end as store_brand_name,
                ds.store_region as region,
                ds.store_country as country,
                a.customer_id,
                order_id,
                a.source_activation_local_datetime::date as date,
                a.activation_channel as channel,
                a.activation_subchannel as subchannel,
                a.utm_medium,
                a.utm_source,
                a.utm_campaign,
                a.utm_term,
                'vip' as event_type,
                'click not from lead pool' as event_description,
                1 as event_count
from edw_prod.data_model.fact_activation a
join edw_prod.data_model.dim_store ds on ds.store_id = a.store_id
join edw_prod.data_model.dim_customer c on c.customer_id = a.customer_id
where lower(ds.store_brand_abbr) in ('fl','yty')
    and lower(ds.store_region) = 'na'
    and a.is_retail_vip = false
    and a.source_activation_local_datetime::date >= $start_date
    and a.source_activation_local_datetime::date < $end_date
    and a.activation_channel in ('branded search','non branded search','shopping');

------------------------------------------------------------------------------------
/* We want to capture all conversions coming from search & shopping, even if they do not drive the lead.
   However, we don't want to double count VIP conversions. Below, we are only counting the VIPs not from lead pool
   if that order is not already found in the from leads tables. This does mean, however, that if a customer becomes
   a lead via a shopping ad and later converts through branded search, they will only be attributed to shopping */

create or replace temporary table _dedupe_click_conversions
(
    store_brand_name VARCHAR,
    region VARCHAR,
    country VARCHAR,
    customer_id VARCHAR ,
    order_id VARCHAR,
    date DATE,
    channel VARCHAR,
    subchannel VARCHAR,
    utm_medium VARCHAR,
    utm_source VARCHAR,
    utm_campaign VARCHAR,
    utm_term VARCHAR,
    event_type VARCHAR,
    event_description VARCHAR,
    event_count INT
);

insert into _dedupe_click_conversions
    select *
    from _click_leads;

insert into _dedupe_click_conversions
    select *
    from _d1_vips_from_click_leads;

insert into _dedupe_click_conversions
    select *
    from _total_vips_from_click_leads;

-- only include VIPs that are NOT already included in the from leads counts
insert into _dedupe_click_conversions
    select v.*
    from _click_vips_not_from_lead_pool v
    left join _dedupe_click_conversions d on v.order_id = d.order_id
        and v.event_type = d.event_type
    where d.order_id is null;


create or replace temporary table _final_deduped_click_conversions as
select store_brand_name,
       region,
       country,
       date,
       channel,
       subchannel,
       utm_campaign as campaign_name,
       iff(utm_term is null, '0', utm_term) as campaign_id,
       sum(iff(event_type = 'lead', event_count, 0)) as click_leads,
       sum(iff(event_description = 'd1 from click leads', event_count, 0)) as d1_vips_from_click_leads,
       sum(iff(event_description = 'total from click leads', event_count, 0)) as total_vips_from_click_leads,
       sum(iff(event_description = 'click not from lead pool', event_count, 0)) as click_vips_not_from_lead_pool,
       total_vips_from_click_leads + click_vips_not_from_lead_pool as total_vips
from _dedupe_click_conversions
group by 1,2,3,4,5,6,7,8;

------------------------------------------------------------------------------------
/* Combining UI data with session / UTM can be tricky. To try and clean the UTM data up a
   little bit, I am creating a campaign name mapping table below. */

create or replace temporary table _campaign_metadata_mapping as
select distinct ad_store,
                region,
                country,
                channel,
                subchannel,
                account_name,
                campaign_name,
                campaign_id
from _final_ui_data;

create or replace temporary table _click_conversions_clean_names as
select distinct coalesce(m1.ad_store, m2.ad_store, c.store_brand_name) as store_brand_name,
                coalesce(m1.region, m2.region, c.region) as region,
                coalesce(m1.country, m2.country, c.country) as country,
                coalesce(m1.channel, m2.channel, c.channel) as channel,
                coalesce(m1.subchannel, m2.subchannel, c.subchannel) as subchannel,
                coalesce(m1.account_name, m2.account_name, 'unknown') as account_name,
                replace(lower(coalesce(m1.campaign_name, m2.campaign_name, 'unknown')),' ','') as campaign_name,
                coalesce(cast(m1.campaign_id as varchar), cast(m2.campaign_id as varchar), 'unknown') as campaign_id,
                date,
                sum(click_leads) as click_leads,
                sum(d1_vips_from_click_leads) as d1_vips_from_click_leads,
                sum(total_vips_from_click_leads) as total_vips_from_click_leads,
                sum(click_vips_not_from_lead_pool) as click_vips_not_from_lead_pool,
                sum(total_vips) as total_vips
from _final_deduped_click_conversions c
left join _campaign_metadata_mapping m1 on replace(lower(m1.campaign_id),' ','') = replace(lower(c.campaign_id),' ','')
left join _campaign_metadata_mapping m2 on replace(lower(m2.campaign_name),' ','') = replace(lower(c.campaign_name),' ','')
group by 1,2,3,4,5,6,7,8,9;

------------------------------------------------------------------------------------
/* There are cases where some dates / campaigns will show up in the UTM data and don't in the UI data and vice versa.
   We want to make sure we are capturing all of it! So, we create a combos table or "scaffold" so that we can ensure
   we are including all counts from each table */

create or replace temporary table _combos as
select distinct store_brand_name,
                region,
                country,
                channel,
                subchannel,
                account_name,
                campaign_name,
                campaign_id,
                date
from _click_conversions_clean_names
union
select distinct ad_store as store_brand_name,
                region,
                country,
                channel,
                subchannel,
                account_name,
                campaign_name,
                cast(campaign_id as varchar) as campaign_id,
                date
from _final_ui_data;


/* this will be the final reporting table! but make sure all the columns populate as 0 if they are null :) */
create or replace transient table reporting_media_prod.dbo.search_shopping_ui_click_optimization_dataset as
select c.*,
       zeroifnull(spend) as spend,
       zeroifnull(impressions) as impressions,
       zeroifnull(clicks) as clicks,
       zeroifnull(pixel_lead_1dc) as pixel_lead_1dc,
       zeroifnull(pixel_lead_30dc) as pixel_lead_30dc,
       zeroifnull(pixel_vip_1dc) as pixel_vip_1dc,
       zeroifnull(click_leads) as click_leads,
       zeroifnull(d1_vips_from_click_leads) as d1_vips_from_click_leads,
       zeroifnull(total_vips_from_click_leads) as total_vips_from_click_leads,
       zeroifnull(click_vips_not_from_lead_pool) as click_vips_not_from_lead_pool,
       zeroifnull(total_vips) as total_vips
from _combos c
left join _final_ui_data u on u.ad_store = c.store_brand_name
    and u.region = c.region
    and u.country = c.country
    and u.date = c.date
    and u.account_name = c.account_name
    and u.channel = c.channel
    and u.subchannel = c.subchannel
    and replace(lower(u.campaign_name),' ','') = c.campaign_name
    and cast(u.campaign_id as varchar) = c.campaign_id
left join _click_conversions_clean_names d on d.store_brand_name = c.store_brand_name
    and d.region = c.region
    and d.country = c.country
    and d.date = c.date
    and d.account_name = c.account_name
    and d.channel = c.channel
    and d.subchannel = c.subchannel
    and d.campaign_name = c.campaign_name
    and d.campaign_id = c.campaign_id;
