
----------------------------------------------------------------------------
-- blisspoint conversions on spend date

create or replace temporary table _spend_date as
select
    'Spend Date' as date_attributed_conversions,

    s.date,
    s.store_brand_name,
    s.store_brand_abbr,
    s.targeting,
    s.region,
    s.country,
    s.store_reporting_name,
    s.media_store_id,
    s.currency,

    s.ad_id,
    s.ad_name,
    s.parent_creative_name,

    s.channel,
    s.subchannel,
    s.vendor,

    'Prospecting' as member_segment,
    0 as is_retargeting_flag,
    offer,
    influencer_name,
    creative_concept,
    video_length,
    video_dimension,
    product_category,
    produced_by_studio_flag,
    creative_date,

    media_cost,
    vendor_fees,
    total_cost,
    impressions,
    0 as clicks,

    lead_view_1d,
    lead_view_3d,
    lead_view_7d,
    vip_view_1d,
    vip_view_3d,
    vip_view_7d,
    lead_view_1d_halo,
    lead_view_3d_halo,
    lead_view_7d_halo,
    vip_view_1d_halo,
    vip_view_3d_halo,
    vip_view_7d_halo

from reporting_media_base_prod.blisspoint.daily_spend_by_creative s
left join reporting_media_base_prod.blisspoint.conversions_credit_on_spend_date c on c.ad_id = s.ad_id
    and c.date = s.date
    and c.targeting = s.targeting
    and c.media_store_id = s.media_store_id
    and c.subchannel = s.subchannel
left join reporting_media_base_prod.blisspoint.vw_blisspoint_naming_convention n on n.ad_id = s.ad_id
            and n.store_brand_abbr = s.store_brand_abbr;


----------------------------------------------------------------------------
-- blisspoint conversions on conversion date

create or replace temporary table _conversion_date_combos as
select distinct date, store_brand_name, store_brand_abbr, region, country, store_reporting_name,
                media_store_id, currency, channel, subchannel, vendor,
                targeting, ad_id, ad_name, parent_creative_name
from reporting_media_base_prod.blisspoint.daily_spend_by_creative
union
select distinct date, store_brand_name, store_brand_abbr, region, country, store_reporting_name,
                media_store_id, currency, channel, subchannel, vendor,
                targeting, ad_id, ad_name, parent_creative_name
from reporting_media_base_prod.blisspoint.conversions_credit_on_conversion_date;


create or replace temporary table _conversion_date as
select 'Conversion Date' as date_attributed_conversions,

       cc.date,
       cc.store_brand_name,
       cc.store_brand_abbr,
       cc.targeting,
       cc.region,
       cc.country,
       cc.store_reporting_name,
       cc.media_store_id,
       cc.currency,

       cc.ad_id,
       cc.ad_name,
       cc.parent_creative_name,

       cc.channel,
       cc.subchannel,
       cc.vendor,

        'Prospecting' as member_segment,
        0 as is_retargeting_flag,
        offer,
        influencer_name,
        creative_concept,
        video_length,
        video_dimension,
        product_category,
        produced_by_studio_flag,
        creative_date,

       media_cost,
       vendor_fees,
       total_cost,
       impressions,
       0 as clicks,

       lead_view_1d,
       lead_view_3d,
       lead_view_7d,
       vip_view_1d,
       vip_view_3d,
       vip_view_7d,
       lead_view_1d_halo,
       lead_view_3d_halo,
       lead_view_7d_halo,
       vip_view_1d_halo,
       vip_view_3d_halo,
       vip_view_7d_halo

from _conversion_date_combos cc
left join reporting_media_base_prod.blisspoint.daily_spend_by_creative s on cc.date = s.date
    and cc.store_brand_name = s.store_brand_name
    and cc.store_brand_abbr = s.store_brand_abbr
    and cc.region = s.region
    and cc.country = s.country
    and cc.store_reporting_name = s.store_reporting_name
    and cc.media_store_id = s.media_store_id
    and cc.currency = s.currency
    and cc.channel = s.channel
    and cc.subchannel = s.subchannel
    and cc.vendor = s.vendor
    and cc.targeting = s.targeting
    and cc. ad_id = s.ad_id
    and cc.ad_name = s.ad_name
    and cc.parent_creative_name = s.parent_creative_name
left join reporting_media_base_prod.blisspoint.conversions_credit_on_conversion_date c on cc.date = c.date
    and cc.store_brand_name = c.store_brand_name
    and cc.store_brand_abbr = c.store_brand_abbr
    and cc.region = c.region
    and cc.country = c.country
    and cc.store_reporting_name = c.store_reporting_name
    and cc.media_store_id = c.media_store_id
    and cc.currency = c.currency
    and cc.channel = c.channel
    and cc.subchannel = c.subchannel
    and cc.vendor = c.vendor
    and cc.targeting = c.targeting
    and cc. ad_id = c.ad_id
    and cc.ad_name = c.ad_name
    and cc.parent_creative_name = c.parent_creative_name
left join reporting_media_base_prod.blisspoint.vw_blisspoint_naming_convention n on n.ad_id = cc.ad_id
    and n.store_brand_abbr = cc.store_brand_abbr;


----------------------------------------------------------------------------

set spend = (select max(daily_spend_meta_update_datetime) from reporting_media_base_prod.blisspoint.daily_spend_by_creative);
set spend_date = (select max(conversion_meta_update_datetime) from reporting_media_base_prod.blisspoint.conversions_credit_on_spend_date);
set conversion_date = (select max(conversion_meta_update_datetime) from reporting_media_base_prod.blisspoint.conversions_credit_on_conversion_date);


create or replace transient table reporting_media_prod.blisspoint.blisspoint_optimization_dataset as
select *,
        least($spend, $spend_date, $conversion_date) as latest_source_update,
        current_timestamp()::timestamp_ltz as meta_create_datetime,
        current_timestamp()::timestamp_ltz as meta_update_datetime
from
    (select * from _spend_date union select * from _conversion_date);
