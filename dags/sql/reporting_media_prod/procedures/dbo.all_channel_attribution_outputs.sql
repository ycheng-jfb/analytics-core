-- All Channel CACs
-- sources: all channel optimization (pixel), cac by lead, and top down target setting

-- source update times
set top_down_update_datetime = (select max(process_run_datetime) from reporting_media_prod.attribution.mmm_outputs_and_target_setting);
set pixel_update_datetime = (select max(data_last_updated_datetime) from reporting_media_prod.dbo.all_channel_optimization);
set cac_by_lead_update_datetime = (select max(meta_update_datetime) from reporting_media_prod.attribution.cac_by_lead_channel_daily);

--- top down target setting
create or replace temporary table _top_down as
select distinct
    store_brand_name,
    date,
    case when feature ilike '%fb+ig%' then 'fb+ig'
        when feature ilike '%discovery%' or feature ilike '%display%' then 'programmatic-gdn'
        when feature ilike '%streaming%' or feature ilike '%tv%' then 'tv+streaming'
        when feature ilike '%programmatic%' then 'programmatic'
        when feature ilike '%pmax%' then 'pmax'
        when feature ilike '%youtube%' then 'youtube'
        when feature ilike '%shopping%' then 'shopping'
    else feature end as channel,
    sum(feature_attributed_vips_without_base) as vips
from reporting_media_prod.attribution.mmm_outputs_and_target_setting
where feature_type = 'marketing'
and modeled_metric != 'not modeled'
and store_brand_name in ('Fabletics','Fabletics Men')
and date >= '2020-01-01' and date <= current_date()
group by 1,2,3;

-- pixel
create or replace temporary table _pixel as
select
       store_brand_name,
       date,
       iff(lower(channel) in ('display','discovery'),'programmatic-gdn',lower(channel)) as channel,
       channel_cpa_by_lead,
       sum(spend_usd) as spend,
       sum(spend_with_vendor_fees_usd) as spend_vendor_fees,
       sum(impressions) as impressions,
       sum(ui_optimization_pixel_lead) as pixel_leads,
       sum(ui_optimization_pixel_vip) as pixel_vips
from reporting_media_prod.dbo.all_channel_optimization
where region = 'NA'
and channel not in ('twitter','affiliate')
and date >= '2020-01-01' and date <= current_date()
group by 1,2,3,4;

-- cac by lead
create or replace temporary table _cac_by_lead as
select distinct
    case  when is_fl_scrubs_customer = 1 then 'Fabletics Scrubs'
        when is_fl_mens_vip = 1 then 'Fabletics Men'
    else business_unit end as store_brand_name,
    lower(channel) as channel,
    date,
    sum(spend_usd) as channel_spend,
    sum(primary_leads) as channel_leads,
    sum(total_vips_on_date) as channel_vips,
    sum(vips_from_leads_24hr) as channel_vips_from_leads_24hr,
    sum(vips_from_leads_d1) as channel_vips_from_leads_d1,
 --from HDYH
    sum(iff(subchannel = 'hdyh', primary_leads, 0)) as channel_leads_from_hdyh,
    sum(iff(subchannel = 'hdyh', total_vips_on_date, 0)) as channel_vips_from_hdyh,
    sum(iff(subchannel = 'hdyh', vips_from_leads_24hr, 0)) as channel_vips_from_leads_24hr_from_hdyh,
    sum(iff(subchannel = 'hdyh', vips_from_leads_d1, 0)) as channel_vips_from_leads_d1_from_hdyh
from reporting_media_prod.attribution.cac_by_lead_channel_daily
where date >= '2020-01-01' and date <= current_date()
and region = 'NA'
and retail_customer = 'false'
and is_fk_free_trial = 'false'
group by 1,2,3;

-- combo channel mapping
create or replace temporary table _combos as
select distinct store_brand_name, channel, date
from _top_down
union
select distinct store_brand_name, channel, date
from _pixel
union
select distinct store_brand_name, channel, date
from _cac_by_lead;

-- final table
-- removing channels where there's no pixel and td data overall
create or replace transient table reporting_media_prod.dbo.all_channel_attribution_outputs as
with _combined as (
select distinct
    t1.*,
    -- cac by lead
    sum(channel_spend) as channel_spend,
    sum(channel_leads) as channel_leads,
    sum(channel_vips) as channel_vips,
    sum(channel_vips_from_leads_24hr) as channel_vips_from_leads_24hr,
    sum(channel_vips_from_leads_d1) as channel_vips_from_leads_d1,
    sum(channel_leads_from_hdyh) as channel_leads_from_hdyh,
    sum(channel_vips_from_hdyh) as channel_vips_from_hdyh,
    sum(channel_vips_from_leads_24hr_from_hdyh) as channel_vips_from_leads_24hr_from_hdyh,
    sum(channel_vips_from_leads_d1_from_hdyh) as channel_vips_from_leads_d1_from_hdyh,
    -- pixel
    sum(spend) as spend,
    sum(spend_vendor_fees) as spend_vendor_fees,
    sum(impressions) as impressions,
    sum(pixel_leads) as pixel_leads,
    sum(pixel_vips) as pixel_vips,
    -- top down
    sum(t4.vips) as top_down_vips
from _combos t1
left join _cac_by_lead t2 on t1.store_brand_name = t2.store_brand_name
    and t1.channel = t2.channel
    and t1.date = t2.date
left join _pixel t3 on t1.store_brand_name = t3.store_brand_name
    and t1.channel = t3.channel
    and t1.date = t3.date
left join _top_down t4 on t3.store_brand_name = t4.store_brand_name
    and t3.channel = t4.channel
    and t3.date = t4.date
group by 1,2,3
),
_final_table as (
    select *,
           iff(sum(top_down_vips) over (partition by store_brand_name,channel) is null
                   and sum(pixel_vips) over (partition by store_brand_name, channel) is null,
               'exclude', 'include') as channel_exclusions
    from _combined
)
select
    *,
    $top_down_update_datetime as top_down_update_datetime,
    $pixel_update_datetime as pixel_update_datetime,
    $cac_by_lead_update_datetime as cac_by_lead_update_datetime
from _final_table
where channel_exclusions = 'include';
