
set spend_update_datetime = (select max(meta_update_datetime) from lake.blisspoint.hourly_spend);

create or replace transient table reporting_media_base_prod.blisspoint.daily_spend_by_creative as
select distinct
    cast(datetime as date) as date,
    channel,
    subchannel,
    vendor,
    store_brand_name,
    store_brand_abbr,
    store_region_abbr as region,
    store_country_abbr as country,
    store_reporting_name,
    media_store_id,
    'USD' as currency,

    iff(lower(network) is null,'Unknown',lower(network)) as targeting,
    iff(cast(creative as varchar) is null,'Unknown',cast(creative as varchar)) as ad_id,
    iff(cast(creative_name as varchar) is null,'Unknown',cast(creative_name as varchar)) as ad_name,
    iff(cast(creative_parent as varchar) is null,'Unknown',cast(creative_parent as varchar)) as parent_creative_name,

    sum(media_cost) as media_cost,
    sum(agency_fee+ad_serving) as vendor_fees,
    sum(media_cost+agency_fee+ad_serving) as total_cost,
    sum(impressions) as impressions,

    $spend_update_datetime as daily_spend_meta_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from lake.blisspoint.hourly_spend s
join reporting_media_prod.dbo.media_dim_store_flm_sessions st
    on st.store_reporting_name = case when business_unit = 'fabletics' then 'Fabletics ' || s.country
                                      when business_unit = 'fableticsmen' then 'Fabletics Men ' || s.country
                                      when business_unit = 'justfab' then 'JustFab ' || s.country
                                      when business_unit = 'shoedazzle' then 'ShoeDazzle ' || s.country
                                      when business_unit = 'fabkids' then 'FabKids ' || s.country
                                      when business_unit = 'savage x' then 'Savage X' end
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
