/*
    Table combines all channel pixel UI data into a single dataset

    Channel Groupings:
    channel and subchannel - original channel/subchannel from optimization datasets (maintained for top down model)
    channel_optimization - desired channel groups as optimized by media team
    channel_cpa_by_lead - corresponding channel found in cpa by lead channel report (allows for easy mapping between sources)

    example: Discovery
    channel_optimization - discovery
    channel_cpa_by_lead - programmatic-gdn
*/
set high_watermark_date = date_trunc('year', current_date) - interval '2 year';

-------------------------------------------------------------------------
-------------------------------------------------------------------------
-- facebook

set ios_rollout = '2021-04-19';

create or replace temporary table _facebook as
select
    channel,
    subchannel,
    vendor,
    channel as channel_optimization,
    channel as channel_cpa_by_lead,

    member_segment,
    bidding_event as bidding_objective,
    cast(account_id as varchar) as account_id,
    account_name,
    cast(campaign_id as varchar) as campaign_id,
    campaign_name,
    adgroup_name,
    cast(adgroup_id as varchar) as adgroup_id,
    ad_name,
    cast(ad_id as varchar) as ad_id,
    ad_is_active_flag,
    adgroup_is_active_flag,
    offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    creative_code,
    image_url,
    link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_with_vendor_fees_usd) as spend_with_vendor_fees_usd,
    sum(spend_account_currency) as spend_account_currency,
    sum(spend_with_vendor_fees_account_currency) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(video_views_2s) as video_views,
    sum(pixel_lead_click_1d) as pixel_lead_click_1d,
    sum(pixel_lead_view_1d) as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    sum(pixel_lead_click_7d) as pixel_lead_click_7d,
    sum(pixel_lead_view_7d) as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    -- use subscribe event for FLM and SXF NA prior to iOS14 rollout
    case when store_brand_name in ('Savage X','Fabletics Men') and region = 'NA' and date <= $ios_rollout
        then sum(pixel_subscribe_click_1d) else sum(pixel_vip_click_1d) end as pixel_vip_click_1d,
    case when store_brand_name in ('Savage X','Fabletics Men') and region = 'NA' and date <= $ios_rollout
        then sum(pixel_subscribe_view_1d) else sum(pixel_vip_view_1d) end as pixel_vip_view_1d,

    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,

    case when store_brand_name in ('Savage X','Fabletics Men') and region = 'NA' and date <= $ios_rollout
        then sum(pixel_subscribe_click_7d) else sum(pixel_vip_click_7d) end as pixel_vip_click_7d,
    case when store_brand_name in ('Savage X','Fabletics Men') and region = 'NA' and date <= $ios_rollout
        then sum(pixel_subscribe_view_7d) else sum(pixel_vip_view_7d) end as pixel_vip_view_7d,

    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.facebook.facebook_optimization_dataset_country
where date >= $high_watermark_date
    and account_name not in ('ShoeDazzle_US_VIPRET','Fabletics_US_Organic','JustFab_US_Organic')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;



-------------------------------------------------------------------------
-- youtube, discovery and gdn

create or replace temporary table _google_ads as
select
    channel,
    subchannel,
    subchannel as vendor,
    case when channel='display' then 'gdn'
        else channel end as channel_optimization,
    case when channel='youtube' then 'youtube'
        else 'programmatic-gdn' end as channel_cpa_by_lead,

    member_segment,
    bidding_objective,
    cast(account_id as varchar) as account_id,
    account_name,
    cast(campaign_id as varchar) as campaign_id,
    campaign_name,
    adgroup_name,
    cast(adgroup_id as varchar) as adgroup_id,
    ad_name,
    cast(ad_id as varchar) as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_with_vendor_fees_usd) as spend_with_vendor_fees_usd,
    sum(spend_account_currency) as spend_account_currency,
    sum(spend_with_vendor_fees_account_currency) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
    0 as video_views,
    sum(pixel_lead_1x1_click) as pixel_lead_click_1d,
    sum(pixel_lead_1x1_view) as pixel_lead_view_1d,
    sum(pixel_lead_3x3_click) as pixel_lead_click_3d,
    sum(pixel_lead_3x3_view) as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    sum(pixel_lead_30x7_view) as pixel_lead_view_7d,
    sum(pixel_lead_30x7_click) as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    sum(pixel_lead_30dc_click) as pixel_lead_30dc_click,
    sum(pixel_lead_7x1_click + pixel_lead_7x1_view) as pixel_lead_7x1,
    sum(pixel_lead_30x7x1_view + pixel_lead_30x7x1_click) as pixel_lead_30x7x1,

    sum(pixel_vip_1x1_click) as pixel_vip_click_1d,
    sum(pixel_vip_1x1_view) as pixel_vip_view_1d,
    sum(pixel_vip_3x3_click) as pixel_vip_click_3d,
    sum(pixel_vip_3x3_view) as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    sum(pixel_vip_30x7_view) as pixel_vip_view_7d,
    sum(pixel_vip_30x7_click) as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    sum(pixel_vip_30dc_click) as pixel_vip_30dc_click,
    sum(pixel_vip_7x1_click + pixel_vip_7x1_view) as pixel_vip_7x1,
    sum(pixel_vip_30x7x1_view + pixel_vip_30x7x1_click) as pixel_vip_30x7x1

from reporting_media_prod.google_ads.google_ads_optimization_dataset
where date >= $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;


-------------------------------------------------------------------------
-- snapchat

create or replace temporary table _snapchat as
select
    channel,
    subchannel,
    subchannel as vendor,
    channel as channel_optimization,
    channel as channel_cpa_by_lead,

    member_segment,
    bidding_event as bidding_objective,
    cast(account_id as varchar) as account_id,
    store_brand_name as account_name,
    cast(campaign_id as varchar) as campaign_id,
    campaign_name,
    adgroup_name,
    cast(adgroup_id as varchar) as adgroup_id,
    ad_name,
    cast(ad_id as varchar) as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_with_vendor_fees_usd) as spend_with_vendor_fees_usd,
    sum(spend_account_currency) as spend_account_currency,
    sum(spend_with_vendor_fees_account_currency) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
    0 as video_views,
    sum(pixel_lead_click_1d) as pixel_lead_click_1d,
    sum(pixel_lead_view_1d) as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    sum(pixel_lead_click_7d) as pixel_lead_click_7d,
    sum(pixel_lead_view_7d) as pixel_lead_view_7d,
    sum(pixel_lead_click_28d) as pixel_lead_click_30d,
    sum(pixel_lead_view_28d) as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    sum(pixel_vip_click_1d) as pixel_vip_click_1d,
    sum(pixel_vip_view_1d) as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    sum(pixel_vip_click_7d) as pixel_vip_click_7d,
    sum(pixel_vip_view_7d) as pixel_vip_view_7d,
    sum(pixel_vip_click_28d) as pixel_vip_click_30d,
    sum(pixel_vip_view_28d) as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.snapchat.snapchat_optimization_dataset
where date >= $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;




-------------------------------------------------------------------------
-- pinterest

create or replace temporary table _pinterest as
select
    lower(channel) as channel,
    lower(subchannel) as subchannel,
    lower(subchannel) as vendor,
    lower(channel) as channel_optimization,
    channel as channel_cpa_by_lead,

    member_segment,
    bidding_objective,
    cast(account_id as varchar) as account_id,
    store_brand_name as account_name,
    cast(campaign_id as varchar) as campaign_id,
    campaign_name,
    adgroup_name,
    cast(adgroup_id as varchar) as adgroup_id,
    ad_name,
    cast(ad_id as varchar) as ad_id,
    null as ad_is_active_flag,
      null as adgroup_is_active_flag,
    offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_with_vendor_fees_usd) as spend_with_vendor_fees_usd,
    sum(spend_account_currency) as spend_account_currency,
    sum(spend_with_vendor_fees_account_currency) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
0 as video_views,
  sum(pixel_lead_click_1d) as pixel_lead_click_1d,
  sum(pixel_lead_view_1d) as pixel_lead_view_1d,
  0 as pixel_lead_click_3d,
  0 as pixel_lead_view_3d,
  sum(pixel_lead_click_7d) as pixel_lead_click_7d,
  sum(pixel_lead_view_7d) as pixel_lead_view_7d,
  sum(pixel_lead_click_30d) as pixel_lead_click_30d,
  sum(pixel_lead_view_30d) as pixel_lead_view_30d,
  0 as pixel_lead_30dc_click,
  0 as pixel_lead_7x1,
  0 as pixel_lead_30x7x1,

  sum(pixel_vip_click_1d) as pixel_vip_click_1d,
  sum(pixel_vip_view_1d) as pixel_vip_view_1d,
  0 as pixel_vip_click_3d,
  0 as pixel_vip_view_3d,
  sum(pixel_vip_click_7d) as pixel_vip_click_7d,
  sum(pixel_vip_view_7d) as pixel_vip_view_7d,
  sum(pixel_vip_click_30d) as pixel_vip_click_30d,
  sum(pixel_vip_view_30d) as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.pinterest.pinterest_optimization_dataset_on_conversion_date
where date >= $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;


-------------------------------------------------------------------------
-- tradedesk programmatic (breakout tv+streaming)
/*
create or replace temporary table _tradedesk as
select
    case when lower(subchannel) = 'streaming' then 'tv+streaming' else 'programmatic' end as channel,
    case when lower(subchannel) = 'streaming' then 'ttd streaming' else subchannel end as subchannel,
    case when lower(subchannel) = 'streaming' then 'tv+streaming' else 'programmatic' end as channel_optimization,
    case when lower(subchannel) = 'streaming' then 'tv+streaming' else 'programmatic' end as channel_cpa_by_lead,

    member_segment,
    'Vip Bidding' as bidding_objective,
    cast(account_id as varchar) as account_id,
    account_name,
    cast(campaign_id as varchar) as campaign_id,
    campaign_name,
    adgroup_name,
    cast(adgroup_id as varchar) as adgroup_id,
    ad_name,
    cast(ad_id as varchar) as ad_id,
    offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_usd) as spend_with_vendor_fees_usd,
    sum(spend_account_currency) as spend_account_currency,
    sum(spend_account_currency) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,

    0 as pixel_lead_click_1d,
    0 as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    sum(pixel_lead_cross_device_7dv) as pixel_lead_view_7d,
    sum(pixel_lead_cross_device_30dc) as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    0 as pixel_vip_click_1d,
    0 as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    sum(pixel_vip_cross_device_7dv) as pixel_vip_view_7d,
    sum(pixel_vip_cross_device_30dc) as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.tradedesk.tradedesk_optimization_dataset
where date >= $high_watermark_date
    and timezone = 'America/Los_Angeles'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25;
*/

-------------------------------------------------------------------------
-- shopping + search (from search ads 360)

create or replace temporary table _shopping_search as
select
    channel,
    subchannel,
    subchannel as vendor,
    channel as channel_optimization,
    channel as channel_cpa_by_lead,

    'Prospecting' as member_segment,
    'Vip Bidding' as bidding_objective,
    cast(account_id as varchar) as account_id,
    account_name,
    cast(campaign_id as varchar) as campaign_id,
    campaign_name,
    null as adgroup_name,
    null as adgroup_id,
    null as ad_name,
    null as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    null as offer,
    null as influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    null as creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_usd) as spend_with_vendor_fees_usd,
    sum(spend_account_currency) as spend_account_currency,
    sum(spend_account_currency) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
    0 as video_views,
    0 as pixel_lead_click_1d,
    0 as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    0 as pixel_lead_view_7d,
    sum(lead_30dc) as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    sum(lead_30x7x1) as pixel_lead_30x7x1,

    0 as pixel_vip_click_1d,
    0 as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    0 as pixel_vip_view_7d,
    sum(vip_30dc) as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    sum(vip_30x7x1) as pixel_vip_30x7x1

from reporting_media_prod.google_search_ads_360.doubleclick_campaign_optimization_dataset
where date >= $high_watermark_date
    and lower(channel) in ('branded search','non branded search','shopping')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;



-------------------------------------------------------------------------
-- tv + streaming NA only (from blisspoint)

create or replace temporary table _streaming_tv as
select
    'tv+streaming' as channel,
    case when lower(subchannel) = 'streaming' then 'bpm streaming' else 'bpm tv' end as subchannel,
    case when lower(subchannel) = 'streaming' then 'bpm streaming' else 'bpm tv' end as vendor,
    'tv+streaming' as channel_optimization,
    'tv+streaming' as channel_cpa_by_lead,

    'Prospecting' as member_segment,
    'Vip Bidding' as bidding_objective,
    store_brand_name as account_id,
    store_brand_name as account_name,
    null as campaign_id,
    null as campaign_name,
    parent_creative_name as adgroup_name,
    null as adgroup_id,
    ad_name as ad_name,
    ad_id as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(substr(media_store_id,1,2) as integer) as store_id,
    null as creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(total_cost) as spend_usd,
    sum(total_cost) as spend_with_vendor_fees_usd,
    sum(total_cost) as spend_account_currency,
    sum(total_cost) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
    0 as video_views,
    0 as pixel_lead_click_1d,
    sum(lead_view_1d) as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    sum(lead_view_3d) as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    sum(lead_view_7d) as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    0 as pixel_vip_click_1d,
    sum(vip_view_1d) as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    sum(vip_view_3d) as pixel_vip_view_3d,
    null  as pixel_vip_click_7d,
    sum(vip_view_7d) as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.blisspoint.blisspoint_optimization_dataset
where date >= $high_watermark_date
    and region = 'NA'
    and date_attributed_conversions = 'Spend Date'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;


-------------------------------------------------------------------------
-- tv + streaming NA only (from tatari)
-- investigating data issues with tatari

create or replace temporary table _tatari as
select
    channel,
    case when lower(subchannel) = 'streaming' then 'tatari streaming' else 'tatari tv' end as subchannel,
    case when lower(subchannel) = 'streaming' then 'tatari streaming' else 'tatari tv' end as vendor,
    'tv+streaming' as channel_optimization,
    'tv+streaming' as channel_cpa_by_lead,

    'Prospecting' as member_segment,
    'Vip Bidding' as bidding_objective,
    store_brand_name as account_id,
    store_brand_name as account_name,
    campaign_id as campaign_id,
    null as campaign_name,
    null as adgroup_name,
    null as adgroup_id,
    ad_name as ad_name,
    ad_id as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    null as offer,
    null as influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    null as creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend) as spend_usd,
    sum(spend_with_vendor_fees) as spend_with_vendor_fees_usd,
    sum(spend) as spend_account_currency,
    sum(spend_with_vendor_fees) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    null as clicks,
    0 as video_views,
    sum(lead_incremental) as pixel_lead_click_1d, --incremental
    sum(lead_view_1d) as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    sum(lead_view_7d) as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    sum(lead_view_30d) as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    sum(vip_incremental) as pixel_vip_click_1d, --incremental
    sum(vip_view_1d) as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    sum(vip_view_7d) as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    sum(vip_view_30d) as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.tatari.tatari_optimization_dataset
where date >= $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;

-------------------------------------------------------------------------
-- roku streaming

create or replace temporary table _roku_streaming as
select
    channel,
    subchannel,
    subchannel as vendor,
    'tv+streaming' as channel_optimization,
    'tv+streaming' as channel_cpa_by_lead,

    'Prospecting' as member_segment,
    'Vip Bidding' as bidding_objective,
    store_brand_name as account_id,
    store_brand_name as account_name,
    campaign_id,
    campaign_name,
    flight_name as adgroup_name,
    flight_id as adgroup_id,
    ad_name as ad_name,
    ad_id as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    offer_promo as offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend) as spend_usd,
    sum(spend) as spend_with_vendor_fees_usd,
    sum(spend) as spend_account_currency,
    sum(spend) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
    0 as video_views,
    0 as pixel_lead_click_1d,
    0 as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    0 as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    0 as pixel_vip_click_1d,
    0 as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    0 as pixel_vip_view_7d,
    sum(vip_click) as pixel_vip_click_30d,
    sum(vip_view) as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.roku.roku_optimization_dataset
where date >= $high_watermark_date
    and region = 'NA'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;


-------------------------------------------------------------------------
-- organic influencers (click from links)

create or replace temporary table _influencer_1dc as
select
    'influencers' as channel,
    case when lower(channel_tier) = 'missinginspenddoc' then 'influencer'
        else lower(channel_tier) end as subchannel,
    case when lower(channel_tier) = 'missinginspenddoc' then 'influencer'
        else lower(channel_tier) end as vendor,
    'influencers' as channel_optimization,
    'influencers' as channel_cpa_by_lead,

    'Prospecting' as member_segment,
    'Vip Bidding' as bidding_objective,
    null as account_id,
    business_unit as account_name,
    null as campaign_id,
    null as campaign_name,
    null as adgroup_name,
    null as adgroup_id,
    null as ad_name,
    null as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    null as offer,
    influencer_name,
    business_unit as store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(substr(media_store_id,1,2) as integer) as store_id,
    null as creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(upfront_cost)+sum(cogs) as spend_usd,
    sum(upfront_cost)+sum(cogs) as spend_with_vendor_fees_usd,
    sum(upfront_cost)+sum(cogs) as spend_account_currency,
    sum(upfront_cost)+sum(cogs) as spend_with_vendor_fees_account_currency,

    0 as impressions,
    sum(organic_clicks) as clicks,
    0 as video_views,
    sum(click_through_leads) as pixel_lead_click_1d,
    0 as pixel_lead_view_1d,
    0 as pixel_lead_view_3d,
    0 as pixel_vip_click_3d,
    0 as pixel_lead_click_7d,
    0 as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    sum(click_through_vips_from_leads_30d)+sum(click_through_vips_same_session_not_from_lead_pool) as pixel_vip_click_1d,
    0 as pixel_vip_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    0 as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.influencers.daily_performance_by_post
where date >= $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;



-------------------------------------------------------------------------
-- organic influencers (view from HDYH)

create or replace temporary table _influencer_1dv as
select
    'influencers' as channel,
    case when lower(channel_tier) = 'missinginspenddoc' then 'influencer'
        else lower(channel_tier) end as subchannel,
    case when lower(channel_tier) = 'missinginspenddoc' then 'influencer'
        else lower(channel_tier) end as vendor,
    'influencers' as channel_optimization,
    'influencers' as channel_cpa_by_lead,

    'Prospecting' as member_segment,
    'Vip Bidding' as bidding_objective,
    null as account_id,
    business_unit as account_name,
    null as campaign_id,
    null as campaign_name,
    null as adgroup_name,
    null as adgroup_id,
    null as ad_name,
    null as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    null as offer,
    influencer_name,
    business_unit as store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(substr(media_store_id,1,2) as integer) as store_id,
    null as creative_code,
    null as image_url,
    null as link_to_post,
    date,

    0 as spend_usd,
    0 as spend_with_vendor_fees_usd,
    0 as spend_account_currency,
    0 as spend_with_vendor_fees_account_currency,

    0 as impressions,
    sum(organic_clicks) as clicks,
    0 as video_views,
    0 as pixel_lead_click_1d,
    sum(hdyh_leads_raw) as pixel_lead_view_1d,
    0 as pixel_lead_view_3d,
    0 as pixel_vip_click_3d,
    0 as pixel_lead_click_7d,
    0 as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    0 as pixel_vip_click_1d,
    sum(hdyh_vips_raw) as pixel_vip_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    0 as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.influencers.daily_performance_by_influencer
where date >= $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;



-------------------------------------------------------------------------
-- tiktok

create or replace temporary table _tiktok as
select
    channel,
    subchannel,
    subchannel as vendor,
    channel as channel_optimization,
    channel as channel_cpa_by_lead,

    member_segment,
    bidding_objective,
    cast(account_id as varchar) as account_id,
    store_brand_name as account_name,
    cast(campaign_id as varchar) as campaign_id,
    campaign_name,
    adgroup_name,
    cast(adgroup_id as varchar) as adgroup_id,
    ad_name,
    cast(t1.ad_id as varchar) as ad_id,
    ad_is_active_flag,
    adgroup_is_active_flag,
    offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country,
    region,
    cast(store_id as integer) as store_id,
    creative_code,
    t1.poster_url as image_url,
    t1.url as link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_with_vendor_fees_usd) as spend_with_vendor_fees_usd,
    sum(spend_account_currency) as spend_account_currency,
    sum(spend_with_vendor_fees_account_currency) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
    SUM(video_watched_2s) as video_views,
    0 as pixel_lead_click_1d,
    0 as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    0 as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    sum(lead) as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    0 as pixel_vip_click_1d,
    sum(vip_view_1d) as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    sum(cta) as pixel_vip_click_7d,
    0 as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    sum(vip) as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.tiktok.tiktok_optimization_dataset t1
left join lake_view.tiktok.ad_link t2
on t1.ad_id = cast(t2.ad_id as varchar)
where date >= $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;


-------------------------------------------------------------------------
-- twitter

create or replace temporary table _twitter as
select
    'twitter' as channel,
    'twitter' as subchannel,
    'twitter' as vendor,
    channel as channel_optimization,
    channel as channel_cpa_by_lead,

    member_segment,
    null as bidding_objective,
    cast(account_id as varchar) as account_id,
    account_name,
    cast(campaign_id as varchar) as campaign_id,
    campaign_name,
    adgroup_name,
    cast(adgroup_id as varchar) as adgroup_id,
    ad_name,
    cast(t1.unique_ad_id as varchar) as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    offer,
    influencer_name,
    store_brand_name,
    store_brand_abbr,
    country as country,
    region,
    cast(store_id as integer) as store_id,
    null as creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_usd) as spend_with_vendor_fees_usd,
    sum(spend_account_currency) as spend_account_currency,
    sum(spend_account_currency) as spend_with_vendor_fees_account_currency,

    sum(impressions) as impressions,
    sum(clicks) as clicks,
    0 as video_views,
    0 as pixel_lead_click_1d,
    0 as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    0 as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    sum(leads) as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    0 as pixel_vip_click_1d,
    0 as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    0 as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    sum(vips) as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.twitter.twitter_optimization_dataset t1
where date >= $high_watermark_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;


-------------------------------------------------------------------------
-- additional channels with no API (from CPA by Lead Channel)
-- affiliate, twitter NA only

create or replace temporary table _cpa_by_lead_channels as
select
    channel,
    subchannel,
    subchannel as vendor,
    channel as channel_optimization,
    channel as channel_cpa_by_lead,

    'Prospecting' as member_segment,
    'Vip Bidding' as bidding_objective,
    null as account_id,
    store as account_name,
    null as campaign_id,
    null as campaign_name,
    null as adgroup_name,
    null as adgroup_id,
    null as ad_name,
    null as ad_id,
    null as ad_is_active_flag,
    null as adgroup_is_active_flag,
    null as offer,
    null as influencer_name,
    case when is_fl_mens_vip=1 then 'Fabletics Men'
        when is_fl_scrubs_customer=1 then 'Fabletics Scrubs'
    else business_unit end as store_brand_name,
    case when store_brand_name = 'Fabletics Men' then 'FLM'
        when store_brand_name = 'Fabletics Scrubs' then 'SCB'
    else d.store_brand_abbr end as store_brand_abbr,
    country,
    region,
    cast(null as integer) as store_id,
    null as creative_code,
    null as image_url,
    null as link_to_post,
    date,

    sum(spend_usd) as spend_usd,
    sum(spend_usd) as spend_with_vendor_fees_usd,
    sum(spend_local) as spend_account_currency,
    sum(spend_local) as spend_with_vendor_fees_account_currency,

    0 as impressions,
    0 as clicks,
    0 as video_views,
    sum(primary_leads) as pixel_lead_click_1d,
    0 as pixel_lead_view_1d,
    0 as pixel_lead_click_3d,
    0 as pixel_lead_view_3d,
    0 as pixel_lead_click_7d,
    0 as pixel_lead_view_7d,
    0 as pixel_lead_click_30d,
    0 as pixel_lead_view_30d,
    0 as pixel_lead_30dc_click,
    0 as pixel_lead_7x1,
    0 as pixel_lead_30x7x1,

    sum(total_vips_on_date) as pixel_vip_click_1d,
    0 as pixel_vip_view_1d,
    0 as pixel_vip_click_3d,
    0 as pixel_vip_view_3d,
    0 as pixel_vip_click_7d,
    0 as pixel_vip_view_7d,
    0 as pixel_vip_click_30d,
    0 as pixel_vip_view_30d,
    0 as pixel_vip_30dc_click,
    0 as pixel_vip_7x1,
    0 as pixel_vip_30x7x1

from reporting_media_prod.attribution.cac_by_lead_channel_daily c
join (select distinct store_brand, store_brand_abbr from edw_prod.data_model.dim_store) d on c.business_unit = d.store_brand
where channel in ('affiliate','reddit')
    and date >= $high_watermark_date
    and retail_customer = 0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;


-------------------------------------------------------------------------

create or replace temporary table _output as
select *
from _facebook
union
select *
from _google_ads
union
select *
from _snapchat
union
select *
from _pinterest
union
select *
from _shopping_search
union
select *
from _streaming_tv
union
select *
from _tatari
union
select *
from _roku_streaming
union
select *
from _influencer_1dc
union
select *
from _influencer_1dv
union
select *
from _tiktok
union
select *
from _twitter
union
select *
from _cpa_by_lead_channels;

-------------------------------------------------------------------------
-- define "default channel optimization window" for conversions


create or replace temporary table _ui_optim as
select *,

       -- LEADS - account for changing windows
            -- GOOGLE ADS OVERHAUL --
            -- fabletics na
            case when channel_optimization in ('youtube')
                     and region = 'NA'
                     and store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty')
                     and date >= '2023-05-17' then pixel_lead_30x7x1

            when channel_optimization in ('discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty')
                     and date >= '2023-05-17' then pixel_lead_7x1

            -- savage x na
            when channel_optimization in ('youtube')
                     and region = 'NA'
                     and store_brand_name in ('Savage X')
                     and date >= '2023-06-06' then pixel_lead_30x7x1

            when channel_optimization in ('discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('Savage X')
                     and date >= '2023-06-06' then pixel_lead_7x1

            -- jfb na (using MCC event for all accounts)
            when channel_optimization in ('youtube','discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('JustFab','ShoeDazzle','FabKids')
                     and date >= '2023-06-29' then pixel_lead_30x7x1

            -- EU (using MCC event for all accounts)
            when channel_optimization in ('youtube','discovery','gdn')
                     and region = 'EU'
                     and date >= '2023-08-01' then pixel_lead_30x7x1

            when channel_optimization in ('non branded search','shopping','branded search') then pixel_lead_click_30d

            when channel_optimization in ('youtube','discovery') and region = 'NA' and date < '2021-02-01' then pixel_lead_click_1d+pixel_lead_view_1d
            when channel_optimization in ('youtube','discovery') and region = 'NA' and date >= '2021-02-01' then pixel_lead_click_3d+pixel_lead_view_3d
            when channel_optimization in ('gdn') and region = 'NA' and date < '2021-04-01' then pixel_lead_click_1d+pixel_lead_view_1d
            when channel_optimization in ('gdn') and region = 'NA' and date >= '2021-04-01' then pixel_lead_click_3d+pixel_lead_view_3d

            when channel_optimization = 'programmatic' then pixel_lead_click_30d+pixel_lead_view_7d
            when channel_optimization = 'pinterest' then pixel_lead_click_7d+pixel_lead_view_7d
            when channel_optimization = 'tv+streaming' and subchannel in ('tatari streaming','tatari tv') then pixel_lead_click_1d
            when channel_optimization = 'tv+streaming' and subchannel in ('ttd streaming') then pixel_lead_click_30d+pixel_lead_view_7d
            when channel_optimization = 'tv+streaming' then pixel_lead_view_7d

            when channel_optimization in ('fb+ig') and date < '2021-05-01' then pixel_lead_click_1d+pixel_lead_view_1d
            when channel_optimization in ('fb+ig') and date >= '2021-05-01' then pixel_lead_click_7d+pixel_lead_view_1d
            when channel_optimization in ('tiktok') then pixel_lead_7x1
            when channel_optimization in ('twitter') then pixel_lead_7x1

            else pixel_lead_click_1d+pixel_lead_view_1d end as ui_optimization_pixel_lead,

------------------------------------------------------------------------------------
       -- VIPs - account for changing windows

            -- GOOGLE ADS OVERHAUL --
            -- fabletics na
            case when channel_optimization in ('youtube')
                     and region = 'NA'
                     and store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty')
                     and date >= '2023-05-17' then pixel_vip_30x7x1

            when channel_optimization in ('discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty')
                     and date >= '2023-05-17' then pixel_vip_7x1

            -- savage x na
            when channel_optimization in ('youtube')
                     and region = 'NA'
                     and store_brand_name in ('Savage X')
                     and date >= '2023-06-06' then pixel_vip_30x7x1

            when channel_optimization in ('discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('Savage X')
                     and date >= '2023-06-06' then pixel_vip_7x1

            -- jfb na (using MCC event for all accounts)
            when channel_optimization in ('youtube','discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('JustFab','ShoeDazzle','FabKids')
                     and date >= '2023-06-29' then pixel_vip_30x7x1

            -- EU (using MCC event for all accounts)
            when channel_optimization in ('youtube','discovery','gdn')
                     and region = 'EU'
                     and date >= '2023-08-01' then pixel_vip_30x7x1

            when channel_optimization in ('non branded search','shopping','branded search') then pixel_vip_click_30d
            when channel_optimization in ('youtube','discovery') and region = 'NA' and date < '2021-02-01' then pixel_vip_click_1d+pixel_vip_view_1d
            when channel_optimization in ('youtube','discovery') and region = 'NA' and date >= '2021-02-01' then pixel_vip_click_3d+pixel_vip_view_3d
            when channel_optimization in ('gdn') and region = 'NA' and date < '2021-04-01' then pixel_vip_click_1d+pixel_vip_view_1d
            when channel_optimization in ('gdn') and region = 'NA' and date >= '2021-04-01' then pixel_vip_click_3d+pixel_vip_view_3d


            when channel_optimization = 'programmatic' then pixel_vip_click_30d+pixel_vip_view_7d
            when channel_optimization = 'pinterest' then pixel_vip_click_7d+pixel_vip_view_7d
            when channel_optimization = 'tv+streaming' and subchannel in ('tatari streaming','tatari tv') then pixel_vip_click_1d
            when channel_optimization = 'tv+streaming' and subchannel in ('ttd streaming') then pixel_vip_click_30d+pixel_vip_view_7d
            when channel_optimization = 'tv+streaming' and subchannel = 'roku streaming' then pixel_vip_view_30d
            when channel_optimization = 'tv+streaming' then pixel_vip_view_7d

            when channel_optimization in ('fb+ig') and date < '2021-05-01' then pixel_vip_click_1d+pixel_vip_view_1d
            when channel_optimization in ('fb+ig') and date >= '2021-05-01' then pixel_vip_click_7d+pixel_vip_view_1d
            when channel_optimization in ('tiktok') then pixel_vip_7x1
            when channel_optimization in ('twitter') then pixel_vip_7x1

            else pixel_vip_click_1d+pixel_vip_view_1d end as ui_optimization_pixel_vip,

------------------------------------------------------------------------------------
       -- optimization windows - account for changing windows

            -- GOOGLE ADS OVERHAUL --
            -- fabletics na
            case when channel_optimization in ('youtube')
                     and region = 'NA'
                     and store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty')
                     and date >= '2023-05-17' then '30x7x1'

            when channel_optimization in ('discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('Fabletics','Fabletics Men','Fabletics Scrubs','Yitty')
                     and date >= '2023-05-17' then '7x1'

            -- savage x na
            when channel_optimization in ('youtube')
                     and region = 'NA'
                     and store_brand_name in ('Savage X')
                     and date >= '2023-06-06' then '30x7x1'

            when channel_optimization in ('discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('Savage X')
                     and date >= '2023-06-06' then '7x1'

            -- jfb na (using MCC event for all accounts)
            when channel_optimization in ('youtube')
                     and region = 'NA'
                     and store_brand_name in ('JustFab','ShoeDazzle','FabKids')
                     and date >= '2023-06-29' then '30x7x1'

            when channel_optimization in ('discovery','gdn')
                     and region = 'NA'
                     and store_brand_name in ('JustFab','ShoeDazzle','FabKids')
                     and date >= '2023-06-29' then '30x1'

            -- EU (using MCC event for all accounts)
            when channel_optimization in ('youtube')
                     and region = 'EU'
                     and date >= '2023-08-01' then '30x7x1'

            when channel_optimization in ('discovery','gdn')
                     and region = 'EU'
                     and date >= '2023-08-01' then '30x1'

            when channel_optimization in ('non branded search','shopping','branded search') then '30DC'
            when channel_optimization in ('youtube','discovery') and region = 'NA' and date < '2021-02-01' then '1x1'
            when channel_optimization in ('youtube','discovery') and region = 'NA' and date >= '2021-02-01' then '3x3'
            when channel_optimization in ('gdn') and region = 'NA' and date < '2021-04-01' then '1x1'
            when channel_optimization in ('gdn') and region = 'NA' and date >= '2021-04-01' then '3x3'


            when channel_optimization = 'programmatic' then '30x7'
            when channel_optimization = 'pinterest' then '7x7'
            when channel_optimization = 'tv+streaming' and subchannel in ('tatari streaming','tatari tv') then 'Incremental'
            when channel_optimization = 'tv+streaming' and subchannel in ('ttd streaming') and date < '2021-04-22' then '30x7'
            when channel_optimization = 'tv+streaming' and subchannel in ('ttd streaming') and date >= '2021-04-22' then '30x30'
            when channel_optimization = 'tv+streaming' and subchannel = 'roku streaming' and store_brand_name  = 'Savage X' then '30DV'
            when channel_optimization = 'tv+streaming' and subchannel = 'roku streaming' and store_brand_name = 'Fabletics' then '14DV'
            when channel_optimization = 'tv+streaming' then '7DV'

            when channel_optimization in ('fb+ig') and date < '2021-05-01' then '1x1'
            when channel_optimization in ('fb+ig') and date >= '2021-05-01' then '7x1'
            when channel_optimization in ('tiktok') then '7x1'
            when channel_optimization in ('twitter') then '7x1'

            else '1x1' end as ui_optimization_window

from _output;


-------------------------------------------------------------------------
--- add running sum measures

create or replace temporary table _rs_ui_optim as
select
        *,
        sum(spend_usd) over(partition by store_brand_name, channel, cast(ad_id as varchar) order by date asc) as rs_spend_usd,
        sum(spend_with_vendor_fees_usd) over(partition by store_brand_name, channel, cast(ad_id as varchar) order by date asc) as rs_spend_with_vendor_fees_usd,
        sum(spend_account_currency) over(partition by store_brand_name, channel, cast(ad_id as varchar) order by date asc) as rs_spend_account_currency,
        sum(spend_with_vendor_fees_account_currency) over(partition by store_brand_name, channel, cast(ad_id as varchar) order by date asc) as rs_spend_with_vendor_fees_account_currency
from _ui_optim;

-------------------------------------------------------------------------
-- adding creative dimensions (only join on distinct creative code (mitigate de-duplication)) + cac targets

create or replace temporary table _final as
select
    op.*,
    t.ui_cac_target,
    cd.designer_initials,
    cd.date_created,
    cd.version,
    cd.ad_type,
    cd.video_length,
    cd.audio,
    cd.first_3_seconds_text,
    cd.first_3_seconds_visual,
    cd.model_action,
    cd.model_placement,
    cd.body_type,
    cd.gender,
    cd.still_life_angle_shot,
    cd.influencer,
    cd.ugc_talent_name,
    cd.egc_talent_name,
    cd.trello_id,
    cd.asset_type,
    cd.asset_size,
    cd.creative_concept,
    cd.test_name,
    cd.platform,
    cd.SKU1,
    cd.SKU2,
    cd.SKU3,
    cd.SKU4,
    cd.SKU5,
    cd.merchandise_type,
    cd.number_of_styles_shown,
    cd.logo,
    cd.offer_format, -- change to offer timing when building out reporting tables
    cd.offer_placement,
    cd.messaging_theme,
    cd.product_type
from _rs_ui_optim op
left join reporting_media_base_prod.dbo.creative_dimensions cd
    on cd.creative_code = op.creative_code
left join reporting_media_base_prod.dbo.media_channel_ui_targets_actuals t on op.date = t.date
    and op.channel = t.channel
    and op.store_brand_name = t.store_brand_name
    and op.region = t.region
    and op.ui_optimization_window = t.ui_optimization_window;

-------------------------------------------------------------------------
-- final reporting table

set data_last_updated = current_timestamp();

create or replace transient table reporting_media_prod.dbo.all_channel_optimization as

select *,
       $data_last_updated as data_last_updated_datetime
from _final;
