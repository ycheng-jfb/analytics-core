-- this proc generates two reporting prod tables:
    -- 1) realtime_spend: realtime spend performance for the last 30 days
    -- 2) realtime_click_performance: realtime spend and click performance for the last 30 days

-- spend update datetime
set fb_pixel_spend_meta_update_datetime = (select max(meta_update_datetime) as pixel_spend_meta_update_datetime from lake_view.facebook.ad_insights_by_hour);
set tt_pixel_spend_meta_update_datetime = (select max(meta_update_datetime)::timestamp_ltz as pixel_spend_meta_update_datetime from lake_view.tiktok.daily_spend);
set s_pixel_spend_meta_update_datetime = (select max(meta_update_datetime) as pixel_spend_meta_update_datetime from lake_view.snapchat.pixel_data_1day);
set g_pixel_spend_meta_update_datetime = (select max(meta_update_datetime) as pixel_spend_meta_update_datetime from lake_view.google_ads.ad_spend);
set search_pixel_spend_meta_update_datetime = (select max(meta_update_datetime) as pixel_spend_meta_update_datetime from lake_view.google_search_ads_360.campaign_stats);
set tw_pixel_spend_meta_update_datetime = (select max(date) as pixel_spend_meta_update_datetime from lake_view.twitter.twitter_spend_and_conversion_metrics_by_promoted_tweet_id);
set p_pixel_spend_meta_update_datetime = (select max(meta_update_datetime) as pixel_spend_meta_update_datetime from lake_view.pinterest.spend_ad1);

-- metadata update datetime
set fb_meta_update_datetime = (select max(meta_update_datetime) from reporting_media_base_prod.facebook.metadata);
set tt_meta_update_datetime = (select max(meta_update_datetime) from reporting_media_base_prod.tiktok.metadata);
set s_meta_update_datetime = (select max(meta_update_datetime) from reporting_media_base_prod.snapchat.metadata);
set g_meta_update_datetime = (select max(meta_update_datetime) from reporting_media_base_prod.google_ads.metadata);
set search_meta_update_datetime = (select max(meta_update_datetime) from lake_view.google_search_ads_360.campaign_label);
set tw_meta_update_datetime = (select max(meta_update_datetime) from reporting_media_base_prod.twitter.metadata);
set p_meta_update_datetime = (select max(meta_update_datetime) from reporting_media_base_prod.pinterest.metadata);

----------------------------------------------------------------------------
-- 1) realtime_spend:
-- the only channels included in this spend table are facebook, snapchat, tiktok, google
-- based on the channel, we'll either bring in the ad id or campaign name (identifier)

create or replace transient table reporting_media_prod.dbo.realtime_spend as
select st.store_id,
       'fb+ig' as channel,
       'fb+ig' as subchannel,
       to_date(date) as date,
       cast(ad_id as varchar) as identifier,
       round(sum(spend),0) as spend_account_currency,
       round(sum((spend) * coalesce(lkpusd.exchange_rate, 1)),0) as spend_usd,
       round(sum((spend) * coalesce(lkplocal.exchange_rate, 1)),0) as spend_local,
       round(ifnull(sum(impressions),0),0) as impressions,
       $fb_pixel_spend_meta_update_datetime as pixel_spend_meta_update_datetime
from lake_view.facebook.ad_insights_by_hour fb
join lake_view.sharepoint.med_account_mapping_media am on lower(am.source_id) = cast(lower(fb.account_id) as varchar)
    and lower(am.source) ilike '%facebook%'
join edw_prod.data_model.dim_store st on st.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and fb.date = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and fb.date = lkplocal.rate_date_pst
    and lkplocal.dest_currency = st.store_currency
where date >= current_date()-30
group by 1,2,3,4,5

union

select
    st.store_id,
    'tiktok' as channel,
    'tiktok' as subchannel,
    to_date(date) as date,
    cast(ad_id as varchar) as identifier,
    round(sum(media_cost),0) as spend_account_currency,
    round(sum((media_cost) * coalesce(lkpusd.exchange_rate, 1)),0) as spend_usd,
    round(sum((media_cost) * coalesce(lkplocal.exchange_rate, 1)),0) as spend_local,
    round(ifnull(sum(impressions),0),0) as impressions,
    $tt_pixel_spend_meta_update_datetime as pixel_spend_meta_update_datetime
from lake_view.tiktok.daily_spend t
join lake_view.sharepoint.med_account_mapping_media am on lower(am.source_id) = cast(lower(t.advertiser_id) as varchar)
    and lower(am.source) ilike '%tiktok%'
join edw_prod.data_model.dim_store st on st.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and t.date = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and t.date = lkplocal.rate_date_pst
    and lkplocal.dest_currency = st.store_currency
where date >= current_date()-30
group by 1,2,3,4,5

union

select
    st.store_id,
    'snapchat' as channel,
    'snapchat' as subchannel,
    to_date(t.start_time) as date,
    cast(t.ad_id as varchar) as identifier,
    round(sum(coalesce(spend,0)/1000000),0) as spend_account_currency,
    round(sum((coalesce(spend,0)/1000000)*coalesce(lkpusd.exchange_rate,1)),0) as spend_usd,
    round(sum((coalesce(spend,0)/1000000)*coalesce(lkplocal.exchange_rate,1)),0) as spend_local,
    round(ifnull(sum(impressions),0),0) as impressions,
    $s_pixel_spend_meta_update_datetime::timestamp_ltz as pixel_spend_meta_update_datetime
from lake_view.snapchat.pixel_data_1day t
    join lake_view.snapchat.ad_metadata adm on t.ad_id = adm.ad_id
    join lake_view.snapchat.adsquad_metadata adsm on adm.ad_squad_id = adsm.adsquad_id
    join lake_view.snapchat.campaign_metadata cmd on adsm.campaign_id=cmd.campaign_id
join lake_view.sharepoint.med_account_mapping_media am on lower(am.source_id) = cast(lower(cmd.AD_ACCOUNT_ID) as varchar)
    and lower(am.source) ilike '%snapchat%'
join edw_prod.data_model.dim_store st on st.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and to_date(t.start_time) = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and to_date(t.start_time) = lkplocal.rate_date_pst
    and lkplocal.dest_currency = st.store_currency
where date >= current_date()-30
group by 1,2,3,4,5

union

select
    ds.store_id as store_id,
    case when ys.external_customer_id in ('5403949764', '2646630478','8543764201') or lower(ys.campaign_name) ilike '%%gdn%%' then 'display'
        when lower(ys.campaign_name) ilike '%%discovery%%' or contains(lower(account_descriptive_name), 'discovery') then 'discovery'
        else 'youtube'
    end as channel_new,
    'google' as subchannel,
    to_date(date) as date,
    cast(lower(ys.ad_id) as varchar) as identifier,
    round(sum(ifnull((ys.cost :: float / 1000000) :: float, 0)),0)  as spend_account_currency,
    round(sum(ifnull((ys.cost :: float / 1000000) :: float, 0) * coalesce(lkpusd.exchange_rate, 1)),0)  as spend_usd,
    round(sum(ifnull((ys.cost :: float / 1000000) :: float, 0) * coalesce(lkplocal.exchange_rate, 1)),0)  AS spend_local,
    round(ifnull(sum(impressions),0),0) as impressions,
    $g_pixel_spend_meta_update_datetime::timestamp_ltz as pixel_spend_meta_update_datetime
from lake_view.google_ads.ad_spend ys
join lake_view.sharepoint.med_account_mapping_media am
    on ys.external_customer_id = am.source_id
    and reference_column = 'account_id'
    and am.include_in_cpa_report = 1
    and lower(source) in ('adwords youtube', 'adwords gdn') -- pulling search and shopping from searchads360
join edw_prod.data_model.dim_store ds
    on am.store_id = ds.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and to_date(ys.date) = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and to_date(ys.date) = lkplocal.rate_date_pst
    and lkplocal.dest_currency = ds.store_currency
where ys.date >= current_date() - 30
and channel_new = 'youtube'
group by 1,2,3,4,5

union

select
    ds.store_id as store_id,
    case when ys.external_customer_id in ('5403949764', '2646630478','8543764201') or lower(ys.campaign_name) ilike '%%gdn%%' then 'display'
        when lower(ys.campaign_name) ilike '%%discovery%%' or contains(lower(account_descriptive_name), 'discovery') then 'discovery'
        else 'youtube'
    end as channel_new,
    'google' as subchannel,
    to_date(date) as date,
    cast(lower(ys.campaign_name) as varchar) as identifier,
    round(sum(ifnull((ys.cost :: float / 1000000) :: float, 0)),0)  as spend_account_currency,
    round(sum(ifnull((ys.cost :: float / 1000000) :: float, 0) * coalesce(lkpusd.exchange_rate, 1)),0)  as spend_usd,
    round(sum(ifnull((ys.cost :: float / 1000000) :: float, 0) * coalesce(lkplocal.exchange_rate, 1)),0)  AS spend_local,
    round(ifnull(sum(impressions),0),0) as impressions,
    $g_pixel_spend_meta_update_datetime::timestamp_ltz as pixel_spend_meta_update_datetime
from lake_view.google_ads.ad_spend ys
join lake_view.sharepoint.med_account_mapping_media am
    on ys.external_customer_id = am.source_id
    and reference_column = 'account_id'
    and am.include_in_cpa_report = 1
    and lower(source) in ('adwords youtube', 'adwords gdn') -- pulling search and shopping from searchads360
join edw_prod.data_model.dim_store ds
    on am.store_id = ds.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and to_date(ys.date) = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and to_date(ys.date) = lkplocal.rate_date_pst
    and lkplocal.dest_currency = ds.store_currency
where ys.date >= current_date() - 30
and channel_new != 'youtube'
group by 1,2,3,4,5

union

select
    ds.store_id,
    case when campaign_labels ilike '%%| NonBrand%%' or campaign_labels ilike '%%|NonBrand%%' OR campaign_labels ilike '%%Non-Brand%%' then 'Non Branded Search'
        when campaign_labels ilike '%%| Brand%%' or campaign_labels ilike '%%|Brand%%' OR campaign_name = 'Techstyle' then 'Branded Search'
        when campaign_labels ilike '%%| GDN%%' or cs.customer_descriptive_name ilike '%%GDN%%' then 'Programmatic-GDN'
        when cs.campaign_name ilike  '%%$_PLA$_%%' escape '$' or cs.campaign_name ilike '%%Shopping%%' OR (campaign_labels ilike '%%| PLA%%' or campaign_labels ilike '%%|PLA%%') then 'Shopping'
        when cs.campaign_name ilike '%%_Brand%%' then 'Branded Search'
        when cs.campaign_name ilike '%%_NB_%%' then 'Non Branded Search'
        else 'Non Branded Search'
    end as channel,
        case when cs.customer_descriptive_name ilike '%%Bing%%' or cs.customer_descriptive_name ilike '%%BNG%%' or campaign_labels ilike '%%Bing%%' then 'Bing'
        when cs.customer_descriptive_name ilike '%%Gemini%%' or campaign_labels ilike '%%Gemini%%' or campaign_labels ilike '%%Yahoo%%' then 'Yahoo'
        when campaign_labels ilike '%%GDN%%' or cs.customer_descriptive_name ilike '%%GDN%%' then 'GDN'
        when cs.customer_descriptive_name ilike '%%Google%%' or cs.customer_descriptive_name ilike '%%GDN%%' OR campaign_labels ilike '%%Google%%' OR cs.customer_descriptive_name ilike '%%Search%%' or cs.customer_descriptive_name ilike '%%Shopping%%' then 'Google'
        else 'Google'
        end as subchannel,
    cs.date :: date as spend_date,
    cast(lower(cs.campaign_id) as varchar) as identifier,
    sum(cost_micros/1000000) as spend_account_currency,
    sum((cost_micros/1000000) * coalesce(lkpusd.exchange_rate, 1)) as spend_usd,
    sum((cost_micros/1000000) * coalesce(lkplocal.exchange_rate, 1)) as spend_local,
    sum(cs.impressions) as impressions,
    $search_pixel_spend_meta_update_datetime as pixel_spend_meta_update_datetime
from lake_view.google_search_ads_360.campaign_stats cs
left join (
    select campaign_id, listagg(label_name, ' | ') campaign_labels
    from lake_view.google_search_ads_360.campaign_label
    group by 1
) cl on cl.campaign_id = cs.campaign_id
join lake_view.sharepoint.med_account_mapping_media am on cs.customer_id = am.source_id and lower(source) ilike '%doubleclick%' -- figure out better way to map this in account mapping
join edw_prod.data_model.dim_store ds on am.store_id = ds.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd
    on am.currency = lkpusd.src_currency
    and cs.date = lkpusd.rate_date_pst
    and lkpusd.dest_currency = 'USD'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal
    on am.currency = lkplocal.src_currency
    and cs.date = lkplocal.rate_date_pst
    and lkplocal.dest_currency = ds.store_currency
where date >= current_date() - 30
group by 1,2,3,4,5

union

select
    ds.store_id,
    'twitter' as channel,
    'twitter' as subchannel,
    to_date(rs.date) as date,
    cast(ad_id_utm as varchar) as identifier,
    sum(ifnull(spend::decimal(18,4),0)) as spend_account_currency,
    sum(ifnull(spend::decimal(18,4) * coalesce(lkpusd.exchange_rate, 1),0)) as spend_usd,
    sum(ifnull(spend::decimal(18,4) * coalesce(lkpusd.exchange_rate, 1),0)) as spend_local,
    sum(ifnull(impressions::decimal(18,4),0)) as impressions,
    $tw_pixel_spend_meta_update_datetime::timestamp_ltz as pixel_spend_meta_update_datetime
from lake_view.twitter.twitter_spend_and_conversion_metrics_by_promoted_tweet_id rs
join (select distinct ad_id, ad_id_utm, unique_ad_id from reporting_media_base_prod.twitter.vw_twitter_naming_convention where lower(ad_id_utm) not in ('unclassified','none')) t on t.unique_ad_id = rs.promoted_tweet_id
join lake_view.sharepoint.med_account_mapping_media am
    on am.source_id = rs.account_id
        and lower(am.source) ilike 'twitter'
join edw_prod.data_model.dim_store ds
    on ds.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and to_date(rs.date) = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and to_date(rs.date) = lkplocal.rate_date_pst
    and lkplocal.dest_currency = ds.store_currency
where date > current_date() - 30
group by 1,2,3,4,5

union

select
    ds.store_id,
    'pinterest' as channel,
    'pinterest' as subchannel,
    to_date(date) as date,
    cast(pin_promotion_id as varchar) as identifier,
    sum(ifnull(spend_in_micro_dollar/1000000,0)) as spend_account_currency,
    sum(ifnull((spend_in_micro_dollar/1000000)::decimal(18,4) * coalesce(lkpusd.exchange_rate, 1),0)) as spend_usd,
    sum(ifnull((spend_in_micro_dollar/1000000)::decimal(18,4) * coalesce(lkpusd.exchange_rate, 1),0)) as spend_local,
    sum(ifnull(impression_1::decimal(18,4),0)) as impressions,
    $p_pixel_spend_meta_update_datetime::timestamp_ltz as pixel_spend_meta_update_datetime
from lake_view.pinterest.spend_ad1 ps1
join lake_view.sharepoint.med_account_mapping_media am ON am.source_id =ps1.advertiser_id
	AND am.reference_column = 'account_id'
	AND lower(am.source) = 'pinterest'
join edw_prod.data_model.dim_store ds
    on ds.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
    and to_date(ps1.date) = lkpusd.rate_date_pst
    and lower(lkpusd.dest_currency) = 'usd'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
    and to_date(ps1.date) = lkplocal.rate_date_pst
    and lkplocal.dest_currency = ds.store_currency
where date >= current_date() - 30
group by 1,2,3,4,5;

----------------------------------------------------------------------------
-- 2) realtime_click_performance:

create or replace temporary table _spend_conv_combo as
select distinct store_id, channel, subchannel,identifier, date
from reporting_media_prod.dbo.realtime_spend
union
select distinct store_id, channel, subchannel,utm_identifier, date
from reporting_media_base_prod.dbo.realtime_session_click_conversions;

create or replace transient table reporting_media_prod.dbo.realtime_click_performance as
select
    cc.store_id,
    coalesce(n.store_brand_name,n2.store_brand_name,n3.store_brand_name, n4.store_brand_name, n5.store_brand_name,n6.store_brand_name) as store_brand_name,
    st.store_region as region,
    st.store_country as country,
    cc.channel as channel,
    cc.subchannel as subchannel,
    cc.date,

    coalesce(cast(n.account_id as varchar),cast(n2.account_id as varchar),cast(n3.account_id as varchar),cast(n4.account_id as varchar),cast(n5.account_id as varchar),cast(n6.account_id as varchar)) as account_id,
    case when cc.channel = 'tiktok' then concat(store_brand, ' ',store_country,' Tiktok')
        when cc.channel = 'snapchat' then concat(store_brand,' ',store_country,' Snapchat')
        when cc.channel = 'twitter' then concat(store_brand,' ',store_country,' Twitter')
        when cc.channel = 'pinterest' then concat(store_brand,' ',store_country,' Pinterest')
    else coalesce(n.account_name,n4.account_name) end as account_name,

    coalesce(cast(n.campaign_id as varchar),cast(n2.campaign_id as varchar),cast(n3.campaign_id as varchar),cast(n4.campaign_id as varchar), cast(n5.campaign_id as varchar),cast(n6.campaign_id as varchar)) as campaign_id,
    coalesce(n.campaign_name,n2.campaign_name,n3.campaign_name,n4.campaign_name, n5.campaign_name, n6.campaign_name) as campaign_name,

    coalesce(cast(n.adgroup_id as varchar),cast(n2.adgroup_id as varchar),cast(n3.adgroup_id as varchar),cast(n4.adgroup_id as varchar),cast(n5.adgroup_id as varchar),cast(n6.adgroup_id as varchar)) as adgroup_id,
    coalesce(n.adgroup_name,n2.adgroup_name,n3.adgroup_name,n4.adgroup_name, n5.adgroup_name,n6.adgroup_name) as adgroup_name,

    cc.identifier as ad_id,
    coalesce(n.ad_name,n2.ad_name,n3.ad_name,n4.ad_name, n5.ad_name,n6.ad_name) as ad_name,

    -- spend
    spend_account_currency,
    spend_usd,
    spend_local,
    impressions,

    -- conversions from sessions
    click_through_sessions,
    leads,
    vips_from_leads_1d,
    vips_from_leads_7d,
    vips_from_leads_30d,

    pixel_spend_meta_update_datetime,
    $fb_meta_update_datetime as facebook_meta_update_datetime,
    $tt_meta_update_datetime as tiktok_meta_update_datetime,
    $s_meta_update_datetime as snapchat_meta_update_datetime,
    $g_meta_update_datetime as google_meta_update_datetime,
    $search_meta_update_datetime as search_meta_update_datetime,
    $tw_meta_update_datetime as twitter_meta_update_datetime,
    $p_meta_update_datetime as pinterest_meta_udpate_datetime,
    current_timestamp()::timestamp_ltz as process_meta_update_datetime

from _spend_conv_combo cc
join edw_prod.data_model.dim_store st on st.store_id = cc.store_id
left join reporting_media_prod.dbo.realtime_spend s on cast(s.identifier as varchar) = cast(cc.identifier as varchar)
        and s.date = cc.date
        and s.store_id = cc.store_id
        and s.channel = cc.channel
        and s.subchannel = cc.subchannel
left join reporting_media_base_prod.dbo.realtime_session_click_conversions c on cast(c.utm_identifier as varchar) = cast(cc.identifier as varchar)  --reporting_media_base_prod.dbo.session_click_conversions c on cast(c.ad_id as varchar) = cc.ad_id
        and cc.date = c.date
        and cc.store_id = c.store_id
        and cc.channel = c.channel
        and cc.subchannel = c.subchannel
left join reporting_media_base_prod.facebook.vw_facebook_naming_convention n on cast(n.ad_id as varchar) = cast(cc.identifier as varchar)
    and cc.channel = 'fb+ig'
left join reporting_media_base_prod.tiktok.vw_tiktok_naming_convention n2 on cast(n2.ad_id as varchar) = cast(cc.identifier as varchar)
    and cc.channel = 'tiktok'
left join reporting_media_base_prod.snapchat.vw_snapchat_naming_convention n3 on cast(n3.ad_id as varchar) = cast(cc.identifier as varchar)
    and cc.channel = 'snapchat'
left join reporting_media_base_prod.google_ads.vw_google_ads_naming_convention n4 on cast(lower(n4.ad_id) as varchar) = cast(lower(cc.identifier) as varchar)
    and cc.channel = 'youtube' and n4.channel = 'youtube'
left join (select distinct store_brand_name, account_name, account_id, campaign_name, campaign_id, adgroup_name, adgroup_id, ad_name, ad_id, ad_id_utm
            from reporting_media_base_prod.twitter.vw_twitter_naming_convention where lower(ad_id_utm) not in ('unclassified','none')) n5 on cast(lower(n5.ad_id_utm) as varchar) = cast(lower(cc.identifier) as varchar)
    and cc.channel = 'twitter'
left join reporting_media_base_prod.pinterest.vw_pinterest_naming_convention n6 on cast(n6.ad_id as varchar)  = cast(lower(cc.identifier) as varchar)
    and c.channel = 'pinterest'
where cc.subchannel != 'google' or cc.channel = 'youtube'

union

select
    cc.store_id,
    coalesce(n5.store_brand_name,n6.store_brand_name) as store_brand_name,
    st.store_region as region,
    st.store_country as country,

    coalesce(n5.channel,n6.channel) as channel,
    coalesce(n5.subchannel, n6.subchannel) as subchannel,
    cc.date,
    coalesce(cast(n5.account_id as varchar),cast(n6.account_id as varchar)) as account_id,
    coalesce(n5.account_name,n6.account_name) as account_name,
    coalesce(cast(n5.campaign_id as varchar),cast(n6.campaign_id as varchar)) as campaign_id,
    cc.identifier as campaign_name,

    -- adgroup / ad id metadata
    -- for google channels (besides youtube) we will only drill down to the campaign name
    -- campaign name is the deepest grain we can attribute UTM click sessions
    '' as adgroup_id,
    '' as adgroup_name,
    '' as ad_id,
    '' as ad_name,

    -- spend
    spend_account_currency,
    spend_usd,
    spend_local,
    impressions,

    -- conversions from sessions
    click_through_sessions,
    leads,
    vips_from_leads_1d,
    vips_from_leads_7d,
    vips_from_leads_30d,

    pixel_spend_meta_update_datetime,
    $fb_meta_update_datetime as facebook_meta_update_datetime,
    $tt_meta_update_datetime as tiktok_meta_update_datetime,
    $s_meta_update_datetime as snapchat_meta_update_datetime,
    $g_meta_update_datetime as google_meta_update_datetime,
    $search_meta_update_datetime as search_meta_update_datetime,
    $tw_meta_update_datetime as twitter_meta_update_datetime,
    $p_meta_update_datetime as pinterest_meta_udpate_datetime,
    current_timestamp()::timestamp_ltz as process_meta_update_datetime

from _spend_conv_combo cc
left join reporting_media_prod.dbo.realtime_spend s on cast(s.identifier as varchar) = cast(cc.identifier as varchar)
        and s.date = cc.date
        and s.store_id = cc.store_id
        and s.channel = cc.channel
        and s.subchannel = cc.subchannel
left join reporting_media_base_prod.dbo.realtime_session_click_conversions c on cast(c.utm_identifier as varchar) = cast(cc.identifier as varchar)
        and cc.date = c.date
        and cc.store_id = c.store_id
        and cc.channel = c.channel
        and cc.subchannel = c.subchannel
join edw_prod.data_model.dim_store st on st.store_id = cc.store_id
left join (select distinct store_id,store_brand_name, channel, subchannel, account_name, account_id, campaign_id, campaign_name
           from reporting_media_prod.google_ads.google_ads_optimization_dataset
            where channel != 'youtube' and date > current_date() - 30) n5
            on cast(lower(n5.campaign_name) as varchar) = cast(lower(cc.identifier) as varchar)
                and cc.store_id = n5.store_id
left join (select distinct store_id,store_brand_name,
                iff(contains(lower(campaign_name),'performancemax') or contains(lower(campaign_name),'_pmax'),'pmax',channel) as channel,
                subchannel, account_name, account_id, campaign_id, campaign_name
           from reporting_media_prod.google_search_ads_360.doubleclick_campaign_optimization_dataset
            where subchannel = 'google' and lower(channel) not in ('Unclassified') and date > current_date() - 30) n6
            on cast(lower(n6.campaign_name) as varchar) = cast(lower(cc.identifier) as varchar)
                and cc.store_id = n6.store_id
where cc.subchannel = 'google' and cc.channel != 'youtube';
