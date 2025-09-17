
set start_date = '2024-01-01';
set end_date = current_date();

------------------------------------------
-- conversions
create or replace temporary table _leads as
select st.store_brand,
       st.store_country as country,
       iff(lower(st.store_brand) = 'fabletics' and  lower(dc.gender) = 'm',true,false) as is_mens_customer,
       iff(lower(st.store_brand) = 'fabletics' and dc.is_scrubs_customer = true, true, false) as is_scrubs_customer,

       cast(iff(lower(store_brand) = 'yitty', 0, iff(is_retail_registration = true, 1, 0)) as boolean) as retail_lead,
       cast(false as boolean) as retail_vip,
       cast(iff(retail_lead = true or retail_vip = true, 1, 0) as boolean) as retail_customer,

       ifnull(case when registration_channel = 'shopping' then
                   case when utm_campaign ilike '%brand%' then 'branded shopping'
                        else 'non branded shopping' end
                else registration_channel end,'unclassified') as click_channel,

       lower(ifnull(h.channel,'unknown')) as hdyh_channel,
       l.registration_local_datetime::date as date,

       count(distinct iff(l.is_secondary_registration = 0, l.customer_id, null)) as primary_leads

from edw_prod.data_model.fact_registration l
join edw_prod.data_model.dim_customer dc on dc.customer_id = l.customer_id
join edw_prod.data_model.dim_store st on st.store_id = l.store_id
left join reporting_media_base_prod.dbo.vw_med_hdyh_mapping h on h.hdyh_value = lower(replace(l.how_did_you_hear_parent,' ',''))
where ifnull(l.is_fake_retail_registration, false) = false
    and l.registration_local_datetime::date >= $start_date
    and l.registration_local_datetime::date < $end_date
    and lower(store_brand_abbr) in ('fl','yty')
    and lower(store_region) = 'na'
group by 1,2,3,4,5,6,7,8,9,10;

create or replace temporary table _acquisition_media_stg as
select dc.customer_id,
       iff(lower(st.store_brand) = 'fabletics' and  lower(dc.gender) = 'm',true,false) as is_mens_customer,
       iff(lower(st.store_brand) = 'fabletics' and dc.is_scrubs_customer = true, true, false) as is_scrubs_customer,

       cast(iff(lower(store_brand) = 'yitty', 0, iff(is_retail_registration = true, 1, 0)) as boolean) as retail_lead,
       cast(iff(lower(store_brand) = 'yitty', 0, iff(is_retail_vip = true, 1, 0)) as boolean) as retail_vip,
       cast(iff(retail_lead = true or retail_vip = true, 1, 0) as boolean) as retail_customer,

       fr.store_id as registration_store_id,
       fr.registration_local_datetime,
       fa.activation_local_datetime,
       fa.cancellation_local_datetime,
       fa.cancel_type,
       fa.sub_store_id,
       fa.store_id as activation_store_id,

       ifnull(case when registration_channel = 'shopping' then
                   case when fr.utm_campaign ilike '%brand%' then 'branded shopping'
                        else 'non branded shopping' end
                else registration_channel end,'unclassified') as click_channel,

       lower(ifnull(h.channel,'unknown')) as hdyh_channel

from edw_prod.data_model.fact_activation fa
join edw_prod.data_model.fact_registration fr on fa.customer_id = fr.customer_id
    and ifnull(fr.is_secondary_registration, false) = false
    and ifnull(fr.is_fake_retail_registration, false) = false
join edw_prod.data_model.dim_customer dc on dc.customer_id = fa.customer_id
join edw_prod.data_model.dim_store st on st.store_id = fa.sub_store_id
left join reporting_media_base_prod.dbo.vw_med_hdyh_mapping h on h.hdyh_value = lower(replace(fr.how_did_you_hear_parent,' ',''))
where activation_local_datetime::date >= $start_date
    and activation_local_datetime::date <= $end_date
    and lower(store_brand_abbr) in ('fl','yty')
    and lower(store_region) = 'na';

create or replace temporary table _vips_from_leads as
select ds.store_brand,
       ds.store_country as country,
       is_mens_customer,
       is_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       click_channel,
       hdyh_channel,
       registration_local_datetime::date as date,
       count(iff(datediff(day, registration_local_datetime, activation_local_datetime) = 0, customer_id, null)) as vip_from_leads_d1
from _acquisition_media_stg s
join edw_prod.data_model.dim_store ds on ds.store_id = s.sub_store_id
where activation_store_id = registration_store_id
group by 1,2,3,4,5,6,7,8,9,10;

create or replace temporary table _vips as
select ds.store_brand,
       ds.store_country as country,
       is_mens_customer,
       is_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       click_channel,
       hdyh_channel,
       activation_local_datetime::date as date,
       count(*) as total_vips_on_date
from _acquisition_media_stg s
join edw_prod.data_model.dim_store ds on ds.store_id = s.sub_store_id
group by 1,2,3,4,5,6,7,8,9,10;

------------------------------------------------------------------------------------
-- conversions scaffold

create or replace temporary table _scaffold_conv as
select date,
       store_brand,
       country,
       is_mens_customer,
       is_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       click_channel,
       hdyh_channel
from _leads
union
select date,
       store_brand,
       country,
       is_mens_customer,
       is_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       click_channel,
       hdyh_channel
from _vips_from_leads
union
select date,
       store_brand,
       country,
       is_mens_customer,
       is_scrubs_customer,
       retail_lead,
       retail_vip,
       retail_customer,
       click_channel,
       hdyh_channel
from _vips;

create or replace temporary table _all_channels_conv as
select s.*,
       primary_leads,
       vip_from_leads_d1,
       total_vips_on_date
from _scaffold_conv s
left join _leads l on s.date = l.date
    and s.store_brand = l.store_brand
    and s.country = l.country
    and s.is_mens_customer = l.is_mens_customer
    and s.is_scrubs_customer = l.is_scrubs_customer
    and s.retail_lead = l.retail_lead
    and s.retail_vip = l.retail_vip
    and s.retail_customer = l.retail_customer
    and s.click_channel = l.click_channel
    and s.hdyh_channel = l.hdyh_channel
left join _vips_from_leads vl on s.date = vl.date
    and s.store_brand = vl.store_brand
    and s.country = vl.country
    and s.is_mens_customer = vl.is_mens_customer
    and s.is_scrubs_customer = vl.is_scrubs_customer
    and s.retail_lead = vl.retail_lead
    and s.retail_vip = vl.retail_vip
    and s.retail_customer = vl.retail_customer
    and s.click_channel = vl.click_channel
    and s.hdyh_channel = vl.hdyh_channel
left join _vips v on s.date = v.date
    and s.store_brand = v.store_brand
    and s.country = v.country
    and s.is_mens_customer = v.is_mens_customer
    and s.is_scrubs_customer = v.is_scrubs_customer
    and s.retail_lead = v.retail_lead
    and s.retail_vip = v.retail_vip
    and s.retail_customer = v.retail_customer
    and s.click_channel = v.click_channel
    and s.hdyh_channel = v.hdyh_channel;

update _all_channels_conv v
set click_channel = 'direct traffic'
from reporting_media_base_prod.lkp.channel_display_name c where c.channel_key = v.click_channel
and lower(c.channel_type) = 'organic';

update _all_channels_conv v
set click_channel = 'fb+ig'
where click_channel = 'facebook';

update _all_channels_conv
set click_channel = 'programmatic-gdn'
where lower(click_channel) = 'google display';

-- only include hdyh for paid channels, set all organic answers to n/a
update _all_channels_conv
set hdyh_channel = 'n/a'
where lower(hdyh_channel) in ('ignore','friend','metanav','organic social','retail','unknown');

create or replace temporary table _conv_unattributed as
select store_brand,
       country,
       is_mens_customer,
       is_scrubs_customer,
       retail_customer,
       date,
       click_channel,
       hdyh_channel,
       sum(primary_leads) as primary_leads,
       sum(vip_from_leads_d1) as vips_from_leads_d1,
       sum(total_vips_on_date) as total_vips_on_date
from _all_channels_conv
group by 1,2,3,4,5,6,7,8;

------------------------------------------------------------------------------------
-- conversion attribution

create or replace temporary table _attributed_conversions as
select store_brand,
       country,
       is_mens_customer,
       is_scrubs_customer,
       retail_customer,
       date,

       case when lower(click_channel) in ('direct traffic','branded search','branded shopping') and lower(hdyh_channel) != 'n/a' then hdyh_channel
           else lower(click_channel)
           end as channel_attributed,

       case when lower(click_channel) in ('direct traffic','branded search','branded shopping') and lower(hdyh_channel) != 'n/a' then 'hdyh'
           else 'click'
           end as credit_source,

       case when channel_attributed = 'non branded search' and credit_source = 'hdyh' then 'reallocate' else 'keep' end as reallocate_credit,

        sum(primary_leads) as primary_leads,
        sum(vips_from_leads_d1) as vips_from_leads_d1,
        sum(total_vips_on_date) as total_vips_on_date

from _conv_unattributed
group by 1,2,3,4,5,6,7,8,9;

create or replace temporary table _nonbrand_click_conv as
select *
from _attributed_conversions
where reallocate_credit = 'keep' and channel_attributed ilike '%non branded%';

create or replace temporary table _nb_hdyh_redistributed as
select store_brand,
       country,
       is_mens_customer,
       is_scrubs_customer,
       retail_customer,
       date,
       click_channel as channel_attributed,
       'hdyh' as credit_source,
       primary_leads_final as primary_leads,
       vips_from_leads_d1_final as vips_from_leads_d1,
       total_vips_on_date_final as total_vips_on_date
from (

select a.store_brand,
       a.country,
       a.is_mens_customer,
       a.is_scrubs_customer,
       a.retail_customer,
       a.date,
       a.primary_leads,
       a.vips_from_leads_d1,
       a.total_vips_on_date,
       t.channel_attributed as click_channel,
       sum(t.primary_leads) over (partition by a.store_brand, a.is_mens_customer, a.is_scrubs_customer, a.retail_customer, a.country, a.date) as total_leads,
       sum(t.vips_from_leads_d1) over (partition by a.store_brand, a.is_mens_customer, a.is_scrubs_customer, a.retail_customer, a.country, a.date) as total_vips_from_leads_d1,
       sum(t.total_vips_on_date) over (partition by a.store_brand, a.is_mens_customer, a.is_scrubs_customer, a.retail_customer, a.country, a.date) as total_vips,
       a.primary_leads * (t.primary_leads / iff(total_leads = 0, null, total_leads)) as primary_leads_final,
       a.vips_from_leads_d1 * (t.vips_from_leads_d1 / iff(total_vips_from_leads_d1 = 0, null, total_vips_from_leads_d1)) as vips_from_leads_d1_final,
       a.total_vips_on_date * (t.total_vips_on_date / iff(total_vips = 0, null, total_vips)) as total_vips_on_date_final
from _attributed_conversions a
left join _nonbrand_click_conv t on a.store_brand = t.store_brand
    and a.is_mens_customer = t.is_mens_customer
    and a.is_scrubs_customer = t.is_scrubs_customer
    and a.retail_customer = t.retail_customer
    and a.country = t.country
    and a.date = t.date
where a.reallocate_credit = 'reallocate'
);

create or replace temporary table _final_conversions_unpivot as
select case when store_brand = 'Fabletics' and is_scrubs_customer = true then 'Fabletics Scrubs'
            when store_brand = 'Fabletics' and is_mens_customer = true then 'Fabletics Men'
            else store_brand end as store_brand,
       country,
       retail_customer,
       date,
       channel_attributed as channel,
       credit_source,
       sum(primary_leads) as primary_leads,
       sum(vips_from_leads_d1) as vips_from_leads_d1,
       sum(total_vips_on_date) as total_vips_on_date
from (
select store_brand,
       country,
       is_mens_customer,
       is_scrubs_customer,
       retail_customer,
       date,
       channel_attributed,
       credit_source,
       sum(primary_leads) as primary_leads,
       sum(vips_from_leads_d1) as vips_from_leads_d1,
       sum(total_vips_on_date) as total_vips_on_date
from _attributed_conversions
where reallocate_credit = 'keep'
group by 1,2,3,4,5,6,7,8
union all
select store_brand,
       country,
       is_mens_customer,
       is_scrubs_customer,
       retail_customer,
       date,
       channel_attributed,
       credit_source,
       sum(primary_leads) as primary_leads,
       sum(vips_from_leads_d1) as vips_from_leads_d1,
       sum(total_vips_on_date) as total_vips_on_date
from _nb_hdyh_redistributed
where channel_attributed is not null
group by 1,2,3,4,5,6,7,8)
group by 1,2,3,4,5,6;

create or replace temporary table _final_conversions_attributed as
select store_brand,
       country,
       retail_customer,
       date,
       channel,
       sum(primary_leads) as primary_leads,
       sum(vips_from_leads_d1) as vips_from_leads_d1,
       sum(total_vips_on_date) as total_vips_on_date,
       sum(iff(credit_source = 'hdyh', primary_leads, 0)) as primary_leads_hdyh,
       sum(iff(credit_source = 'hdyh', vips_from_leads_d1, 0)) as vips_from_leads_d1_hdyh,
       sum(iff(credit_source = 'hdyh', total_vips_on_date, 0)) as total_vips_on_date_hdyh,
       sum(iff(credit_source = 'click', primary_leads, 0)) as primary_leads_click,
       sum(iff(credit_source = 'click', vips_from_leads_d1, 0)) as vips_from_leads_d1_click,
       sum(iff(credit_source = 'click', total_vips_on_date, 0)) as total_vips_on_date_click
from _final_conversions_unpivot
group by 1,2,3,4,5;


------------------------------------------------------------------------------------
------------------------------------------
-- spend

create or replace temporary table _daily_spend as
select case when store_brand = 'Fabletics' and is_scrubs_flag = true then 'Fabletics Scrubs'
            when store_brand = 'Fabletics' and is_mens_flag = true then 'Fabletics Men'
            else store_brand end as store_brand,
       store_country as country,
       cast(iff(lower(channel) = 'physical partnerships', true, false) as boolean) as retail_customer,
       media_cost_date::date as date,
       channel,
       sum(cost) as spend
from reporting_media_prod.dbo.vw_fact_media_cost f
join edw_prod.data_model.dim_store ds on ds.store_id = f.store_id
where lower(store_brand_abbr) in ('fl','yty')
    and lower(store_region) = 'na'
    and media_cost_date::date >= $start_date
    and media_cost_date::date < $end_date
group by 1,2,3,4,5;

create or replace temporary table _shopping_split as
select store_brand_name as store_brand,
       country,
       cast(false as boolean) as retail_customer,
       date,
       case when campaign_name ilike '%brand%' then 'branded shopping' else 'non branded shopping' end as channel,
       sum(spend_usd) as spend,
       sum(spend) over (partition by store_brand, country, retail_customer, date) as total_daily_spend,
       spend / NULLIFZERO(total_daily_spend) as pct_total_daily
from reporting_media_prod.google_search_ads_360.doubleclick_campaign_optimization_dataset
where date >= $start_date
    and date < $end_date
    and lower(store_brand_abbr) in ('fl','flm','scb','yty')
    and lower(region) = 'na'
    and channel = 'shopping'
group by 1,2,3,4,5;

create or replace temporary table _daily_spend_final as
select *
from _daily_spend
where channel != 'shopping'
union
select d.store_brand,
       d.country,
       d.retail_customer,
       d.date,
       s.channel,
       d.spend * s.pct_total_daily as spend
from _daily_spend d
left join _shopping_split s on d.date = s.date
    and d.store_brand = s.store_brand
    and d.country = s.country
    and d.retail_customer = s.retail_customer
where d.channel = 'shopping';



------------------------------------------------------------------------------------

create or replace temporary table _scaffold as
select distinct store_brand, country, retail_customer, date, channel
from _final_conversions_attributed
where date >= $start_date
    and date < $end_date
union
select distinct store_brand, country, retail_customer, date, channel
from _daily_spend_final;

create or replace temporary table _final_new_cac as
select s.*,
       zeroifnull(spend) as spend,
       zeroifnull(primary_leads) as primary_leads,
       zeroifnull(vips_from_leads_d1) as vips_from_leads_d1,
       zeroifnull(total_vips_on_date) as total_vips_on_date,
       zeroifnull(primary_leads_hdyh) as primary_leads_hdyh,
       zeroifnull(vips_from_leads_d1_hdyh) as vips_from_leads_d1_hdyh,
       zeroifnull(total_vips_on_date_hdyh) as total_vips_on_date_hdyh,
       zeroifnull(primary_leads_click) as primary_leads_click,
       zeroifnull(vips_from_leads_d1_click) as vips_from_leads_d1_click,
       zeroifnull(total_vips_on_date_click) as total_vips_on_date_click
from _scaffold s
left join _daily_spend_final d on d.store_brand = s.store_brand
    and d.country = s.country
    and d.retail_customer = s.retail_customer
    and d.date = s.date
    and d.channel = s.channel
left join _final_conversions_attributed c on c.store_brand = s.store_brand
    and c.country = s.country
    and c.retail_customer = s.retail_customer
    and c.date = s.date
    and c.channel = s.channel;

create or replace transient table reporting_media_prod.attribution.fl_supplemental_cac_by_lead_google as
select *,
       current_timestamp()::timestamp_ltz as refresh_datetime
from _final_new_cac;
