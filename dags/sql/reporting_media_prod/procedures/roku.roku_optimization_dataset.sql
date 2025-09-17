
--combos: conversions are on conv date (include all dimensions bc we don't have a naming conv)
create or replace temporary table _combos as
select distinct channel, subchannel, store_brand_name, store_id, store_brand_abbr, tactic_name,campaign_name,campaign_id,
                flight_name, flight_id, ad_name, optimization_window, creative_code, ad_id, date
from reporting_media_base_prod.roku.daily_spend
union
select distinct channel, subchannel, store_brand_name, store_id, store_brand_abbr, tactic_name,campaign_name,campaign_id,
                flight_name, flight_id, ad_name, optimization_window, creative_code, ad_id, date
from reporting_media_base_prod.roku.daily_conversions;

--optimization pipeline
set spend_date = (select max(spend_meta_update_datetime) from reporting_media_base_prod.roku.daily_spend);
set conversion_date = (select max(conversion_meta_update_datetime) from reporting_media_base_prod.roku.daily_conversions);

create or replace transient table reporting_media_prod.roku.roku_optimization_dataset as
select

    cc.*,

    t3.influencer as influencer_name,
    t3.offer as offer_promo,

    'NA' as region,
    'US' as country,
    'USD' as currency,

    spend,
    impressions,
    clicks,

    vip_view,
    vip_click,
    total_vips,

    least($spend_date, $conversion_date) as latest_source_update,
    current_timestamp()::timestamp_ltz as roku_meta_create_datetime,
    current_timestamp()::timestamp_ltz as roku_meta_update_datetime

from _combos cc
left join reporting_media_base_prod.roku.daily_spend t1
on cc.ad_id = t1.ad_id
and cc.date = t1.date
left join reporting_media_base_prod.roku.daily_conversions t2
on cc.ad_id = t2.ad_id
and cc.date = t2.date
left join reporting_media_base_prod.dbo.creative_dimensions t3
on cc.creative_code = t3.creative_code;
