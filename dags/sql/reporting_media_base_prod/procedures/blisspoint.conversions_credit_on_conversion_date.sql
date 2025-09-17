
set conversion_update_datetime = (select max(meta_update_datetime) from lake.blisspoint.kpi_between);

create or replace transient table reporting_media_base_prod.blisspoint.conversions_credit_on_conversion_date as
select distinct
    date,
    'Video' as channel,
    'Streaming' as subchannel,
    'Blisspoint' as vendor,

    store_brand_name,
    store_brand_abbr,
    store_region_abbr as region,
    store_country_abbr as country,
    store_reporting_name,
    media_store_id,
    'USD' as currency,

    iff(lower(network) is null,'Unknown',lower(network)) as targeting,
    iff(cast(isci as varchar) is null,'Unknown',cast(isci as varchar)) as ad_id,
    iff(cast(creative_name as varchar) is null,'Unknown',cast(creative_name as varchar)) as ad_name,
    iff(cast(parent_creative_name as varchar) is null,'Unknown',cast(parent_creative_name as varchar)) as parent_creative_name,

    sum(iff(lead_count_1_day_view='nan',0,lead_count_1_day_view)) as lead_view_1d,
    sum(iff(lead_count_3_day_view='nan',0,lead_count_3_day_view)) as lead_view_3d,
    sum(iff(lead_count_7_day_view='nan',0,lead_count_7_day_view)) as lead_view_7d,
    sum(iff(vip_count_1_day_view='nan',0,vip_count_1_day_view)) as vip_view_1d,
    sum(iff(vip_count_3_day_view='nan',0,vip_count_3_day_view)) as vip_view_3d,
    sum(iff(vip_count_7_day_view='nan',0,vip_count_7_day_view)) as vip_view_7d,
    sum(iff(lead_count_1_day_view_halo='nan',0,lead_count_1_day_view_halo)) as lead_view_1d_halo,
    sum(iff(lead_count_3_day_view_halo='nan',0,lead_count_3_day_view_halo)) as lead_view_3d_halo,
    sum(iff(lead_count_7_day_view_halo='nan',0,lead_count_7_day_view_halo)) as lead_view_7d_halo,
    sum(iff(vip_count_1_day_view_halo='nan',0,vip_count_1_day_view_halo)) as vip_view_1d_halo,
    sum(iff(vip_count_3_day_view_halo='nan',0,vip_count_3_day_view_halo)) as vip_view_3d_halo,
    sum(iff(vip_count_7_day_view_halo='nan',0,vip_count_7_day_view_halo)) as vip_view_7d_halo,

    $conversion_update_datetime as conversion_meta_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from  lake.blisspoint.kpi_between r
join reporting_media_prod.DBO.MEDIA_DIM_STORE_FLM_SESSIONS st
    on st.store_reporting_name = case when brand = 'fabletics' then 'Fabletics ' || r.country
                                      when brand = 'fableticsmen' then 'Fabletics Men ' || r.country
                                      when brand = 'justfab' then 'JustFab ' || r.country
                                      when brand = 'shoedazzle' then 'ShoeDazzle ' || r.country
                                      when brand = 'fabkids' then 'FabKids ' || r.country
                                      when brand = 'savage x' then 'Savage X' end
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
