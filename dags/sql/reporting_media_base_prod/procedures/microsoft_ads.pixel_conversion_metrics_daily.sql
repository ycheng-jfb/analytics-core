set high_watermark_datetime = (select max(meta_update_datetime) from lake_view.microsoft_ads.conversion_performance_daily_report);

create or replace temporary table _conversion_name as
with _recent_name as (
select distinct goal,
                goal_id,
                s.conversion_event_type,
                s.click_window,
                row_number() over (partition by goal_id order by m.meta_update_datetime desc) as rn
from lake_view.microsoft_ads.conversion_performance_daily_report m
join lake_view.sharepoint.med_microsoft_conversion_mapping s on m.goal_id = s.conversion_event_id
and m.date >= s.start_date
and m.date <= s.end_date)

select goal as conversion_name,
       goal_id as conversion_id,
       conversion_event_type,
       click_window
from _recent_name
where rn = 1;

create or replace transient table reporting_media_base_prod.microsoft_ads.pixel_conversion_metrics_daily as
with _conversion_name_grouping as (
select distinct
    st.store_id,
    account_id,
    campaign_id,
    ad_group_id as adgroup_id,
    conversion_name,
    date,
    case when lower(conversion_event_type) = 'vip' and lower(click_window) = '30dc' then 'vip 30dc'
        when lower(conversion_event_type) = 'lead' and lower(click_window) = '30dc' then 'lead 30dc'
        when lower(conversion_event_type) = 'vip' and lower(click_window) = '1dc' then 'vip 1dc'
        when lower(conversion_event_type) = 'lead' and lower(click_window) = '1dc' then 'lead 1dc'
    end as pixel_conversion_name,
    sum(all_conversions) as all_conversions

from lake_view.microsoft_ads.conversion_performance_daily_report c
join _conversion_name cn on c.goal_id = cn.conversion_id
join lake_view.sharepoint.med_account_mapping_media am on am.source_id = c.account_id
    and lower(am.source) ilike '%microsoft%'
join edw_prod.data_model.dim_store st on st.store_id = am.store_id
group by 1,2,3,4,5,6,7)

select
    store_id,
    account_id,
    campaign_id,
    adgroup_id,
    date,

    sum(iff(pixel_conversion_name = 'lead 30dc', all_conversions, 0)) as pixel_lead_30dc,
    sum(iff(pixel_conversion_name = 'lead 1dc', all_conversions, 0)) as pixel_lead_1dc,

    sum(iff(pixel_conversion_name = 'vip 30dc', all_conversions, 0)) as pixel_vip_30dc,
    sum(iff(pixel_conversion_name = 'vip 1dc', all_conversions, 0)) as pixel_vip_1dc,

    $high_watermark_datetime as latest_conversion_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from _conversion_name_grouping
group by 1,2,3,4,5;
