set end_date = convert_timezone('America/Los_Angeles', current_timestamp)::date;
set report_date = dateadd(day, -1, $end_date);
set report_month = date_trunc(month,$report_date);
set run_rate_multiplier = (select day(last_day($report_date)) / day($report_date));

create or replace temporary table _finance as
select
    store_brand_name,
    region,
    segment,
    month,
    'all' as channel,
    null as top_down_channel_name,
    sum(media_spend) as media_spend,
    sum(leads) as leads,
    sum(m1_vips) as m1_vips,
    sum(m2plus_vips) as m2plus_vips,
    sum(vips_on_date) as total_vips,
    sum(media_spend)/iff(sum(vips_on_date)=0,null,sum(vips_on_date)) as total_vip_cac
from (
    select
        case when lower(store) in ('jfna','jfeu') then 'JustFab'
            when lower(store) = 'sdna' then 'ShoeDazzle'
            when lower(store) = 'fkna' then 'FabKids'
            when lower(store) in ('flna-movip','fleu-mvip') then 'Fabletics Men'
            when lower(store) in ('flna-fovip','fleu-fvip') then 'Fabletics'
            when lower(store) in ('sxna','sxeu') then 'Savage X'
            when lower(store) = 'ytna' then 'Yitty'
            end as store_brand_name,
        case when lower(store) ilike '%%na%%' then 'NA'
            when lower(store) ilike '%%eu%%' then 'EU'
            end as region,
        segment,
        month_date as month,
        media_spend,
        leads,
        m1_vips,
        m2plus_vips,
        vips as vips_on_date
    from reporting_base_prod.shared.acq01_acquisition_budget_forecast
    where lower(segment) in ('actuals mtd','actuals runrate','budget','forecast')
        and lower(store) in ('jfna','sdna','fkna','flna-movip','flna-fovip','sxna','sxeu','fleu-fvip','fleu-mvip','jfeu','ytna')

union all -- bring in retail actuals MTD for FL and FLM

    select
        case when is_fl_mens_flag = 0 then 'Fabletics'
             when is_fl_mens_flag = 1 then 'Fabletics Men'
             end as store_brand_name,
         store_region as region,
        'Actuals MTD' as segment,
        date_trunc(month,date) as month,
        sum(total_spend) as media_spend,
        sum(primary_leads) as leads,
        sum(vips_from_leads_m1) as m1vips,
        sum(total_vips_on_date-vips_from_leads_m1) as m2plus_vips,
        sum(total_vips_on_date) as vips_on_date
    from reporting_media_prod.attribution.daily_acquisition_metrics_cac
    where lower(currency) = 'usd'
        and date >= '2021-01-01'
        and lower(store_brand) = 'fabletics'
        and lower(store_region) = 'na'
        and retail_customer = 1
        group by 1,2,3,4

union all -- bring in retail actuals runrate for FL and FLM

    select
        case when is_fl_mens_flag = 0 then 'Fabletics'
             when is_fl_mens_flag = 1 then 'Fabletics Men'
             end as store_brand_name,
        store_region as region,
    'Actuals RunRate' as segment,
    date_trunc(month,date) as month,
    sum(iff(date_trunc(month, date) = $report_month, total_spend*$run_rate_multiplier, total_spend)) as media_spend,
    sum(iff(date_trunc(month, date) = $report_month, primary_leads*$run_rate_multiplier, primary_leads)) as leads,
    sum(iff(date_trunc(month, date) = $report_month, vips_from_leads_m1*$run_rate_multiplier, vips_from_leads_m1)) as m1vips,
    sum(iff(date_trunc(month, date) = $report_month, (total_vips_on_date-vips_from_leads_m1)*$run_rate_multiplier, total_vips_on_date-vips_from_leads_m1)) as m2plus_vips,
    sum(iff(date_trunc(month, date) = $report_month, total_vips_on_date*$run_rate_multiplier, total_vips_on_date)) as vips_on_date
    from reporting_media_prod.attribution.daily_acquisition_metrics_cac
    where lower(currency) = 'usd'
        and date >= '2021-01-01'
        and lower(store_brand) = 'fabletics'
        and lower(store_region) = 'na'
        and retail_customer = 1
        group by 1,2,3,4
    )
group by 1,2,3,4,5,6;

---------------------------------------------

create or replace temporary table _media_outlook as
select store_brand_name,
       region,
       media_outlook_type as segment,
       month,
       channel,
       top_down_channel_name,
       projected_spend as media_spend,
       null as leads,
       null as m1_vips,
       null as m2plus_vips,
       projected_spend/total_vip_cac_target as total_vips,
       total_vip_cac_target as total_vip_cac
from lake_view.sharepoint.med_media_outlook_channel_spend;

---------------------------------------------

create or replace transient table reporting_media_base_prod.dbo.media_overall_budget_outlook_actuals as
select *
from _media_outlook
union
select *
from _finance;

