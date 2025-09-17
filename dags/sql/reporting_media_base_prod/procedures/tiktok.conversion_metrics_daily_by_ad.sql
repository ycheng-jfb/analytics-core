
set high_watermark_datetime = (select max(meta_update_datetime) from LAKE_VIEW.TIKTOK.DAILY_SPEND);

-- conversions from tiktok API
-- showing same session counts prior to June 9, 2021 and 7x1 after
create or replace temporary table _tiktok_api as
select
    advertiser_id as account_id,
    ad_id,
    st.store_id,
    date,

    sum(lead_click_1d) as lead,
    sum(vip_click_1d) as vip,
    sum(vta_conversion) as vip_view_1d,
    sum(cta_conversion) as cta,
    sum(purchase_click_1d) as purchase,
    sum(on_web_subscribe) as subscribe,

    sum(video_play_actions) as video_play_actions,
    sum(video_watched_2s) as video_watched_2s,
    sum(video_watched_6s) as video_watched_6s

from LAKE_VIEW.TIKTOK.DAILY_SPEND tt
    join lake_view.sharepoint.med_account_mapping_media_tsos am on am.source_id = tt.advertiser_id
        and lower(am.source) ilike '%tiktok%'
    join edw_prod.data_model_jfb.dim_store st on st.store_id = am.store_id
group by 1,2,3,4;


-- map campaign names to TikTok IDs
create or replace temporary table _ids_mapped as
select distinct
    m.account_id,
    m.campaign_id,
    m.adgroup_id,
    m.ad_id,
    cast(case when st.store_id = 52 and cm.campaign_name ilike '%FLM%' then st.store_id || '011' else st.store_id end as int) as media_store_id,
    cm.campaign_id as utm_campaign_id,
    cm.adgroup_id as utm_adgroup_id,
    cm.ad_id as utm_ad_id
from reporting_media_base_prod.tiktok.metadata m
join LAKE_VIEW.SHAREPOINT.MED_TIKTOK_CAMPAIGN_METADATA cm on lower(m.campaign_name) = lower(cm.campaign_name)
    and lower(m.adgroup_name) = lower(cm.adgroup_name)
    and lower(m.ad_name) = lower(cm.ad_name)
join lake_view.sharepoint.med_account_mapping_media_tsos am on cast(am.source_id as varchar) = cast(m.account_id as varchar)
    and lower(am.source) ilike '%tiktok%'
join edw_prod.data_model_jfb.dim_store st on st.store_id = am.store_id
;



-- vips from leads 30d tiktok (removing same session conversions likely captured by tiktok pixel)
-- prior to tiktok release of 7x1 on June 9, 2021
create or replace temporary table _vips_from_leads_30d_tiktok as
select distinct
                cast(case when g.label ilike '%FLM%' then ds.store_id || '011' else ds.store_id end as int) as media_store_id,
                ds.store_id,
                l.customer_id,
                l.session_id,
                cast(registration_local_datetime as date) as date, -- spend date (lead date)
                l.utm_source as channel,
                replace(l.utm_campaign,'tiktok_campaign_id_','') as utm_campaign_id,
                iff(contains(l.utm_content,'utm_term'),
                    replace(left(l.utm_content,charindex('utm_term',l.utm_content)-1),'tiktok_adgroup_id_',''),
                    replace(l.utm_content,'tiktok_adgroup_id_','')) as utm_adgroup_id,
                replace(l.utm_term,'tiktok_ad_id_','') as utm_ad_id,
                'VIP from Lead 30d' as membership_event
from edw_prod.data_model_jfb.fact_registration l
join edw_prod.data_model_jfb.fact_activation v on l.customer_id = v.customer_id
    and datediff(day,l.registration_local_datetime,v.activation_local_datetime) < 30
    and datediff(hour,l.registration_local_datetime,v.activation_local_datetime) > 1 -- removing "same session" vips
join edw_prod.data_model_jfb.dim_store ds on ds.store_id = l.store_id
left join lake_jfb.ultra_merchant.dm_gateway g on g.dm_gateway_id = l.dm_gateway_id
where l.utm_medium in ('paid_social_media')
    and l.utm_source in ('tiktok')
    and cast(registration_local_datetime as date) >= '2020-03-01';

-- map DB conversions to tiktok IDs
create or replace temporary table _vips_from_leads_mapped as
select l.*,
       i.account_id,
       i.campaign_id,
       i.adgroup_id,
       i.ad_id
from _vips_from_leads_30d_tiktok l
join _ids_mapped i on lower(l.utm_campaign_id) = lower(i.utm_campaign_id)
    and lower(l.utm_adgroup_id) = lower(i.utm_adgroup_id)
    and lower(l.utm_ad_id) = lower(i.utm_ad_id)
    and l.media_store_id = i.media_store_id;


create or replace temporary table _final_vips_from_leads_30d AS
select
       account_id,
       store_id,
       date,
       ad_id,
       count(*) as vips_from_leads_30d
from _vips_from_leads_mapped
group by 1,2,3,4;


create or replace temporary table _combos as
select distinct account_id, store_id, date, ad_id from _tiktok_api
union
select distinct account_id, store_id, date, ad_id from _final_vips_from_leads_30d;


create or replace transient table reporting_media_base_prod.tiktok.conversion_metrics_daily_by_ad as
select
    c.account_id,
    c.ad_id,
    c.store_id,
    c.date,

    sum(lead) as lead,
    sum(vip) as vip,
    sum(vip_view_1d) as vip_view_1d,
    sum(cta) as cta,
    sum(purchase) as purchase,
    sum(vips_from_leads_30d) as vips_from_leads_30d,
    sum(subscribe) as subscribe,

    sum(video_play_actions) as video_play_actions,
    sum(video_watched_2s) as video_watched_2s,
    sum(video_watched_6s) as video_watched_6s,

    $high_watermark_datetime as latest_conversion_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from _combos c
left join _tiktok_api t on t.store_id = c.store_id
    and t.account_id = c.account_id
    and t.ad_id = c.ad_id
    and t.date = c.date
left join _final_vips_from_leads_30d l on l.store_id = c.store_id
    and l.account_id = c.account_id
    and l.ad_id = c.ad_id
    and l.date = c.date
group by 1,2,3,4;
