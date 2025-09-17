
create or replace transient table reporting_media_prod.dbo.realtime_hourly_sessions_from_ads as
select
       c.session_datetime as session_datetime,
       cast(c.session_datetime as date) as session_date,
       date_trunc(hour,session_datetime) as session_hour,
       coalesce(n.store_brand_name,n2.store_brand_name,n3.store_brand_name,n4.store_brand_name, n5.store_brand_name, n6.store_brand_name) as store_brand_name,
       st.store_region as region,
       st.store_country as country,
       c.channel,
       c.subchannel,
    case when c.channel = 'tiktok' then concat(store_brand, ' ',st.store_country,' Tiktok')
        when c.channel = 'snapchat' then concat(store_brand,' ',st.store_country,' Snapchat')
        when c.channel = 'twitter' then concat(store_brand,' ',st.store_country,' Twitter')
        when c.channel = 'pinterest' then concat(store_brand,' ',st.store_country,' Pinterest')
    else coalesce(n.account_name,n4.account_name) end as account_name,
    count(*) as sessions
from reporting_media_base_prod.dbo.realtime_sessions c
join edw_prod.data_model.dim_store st on st.store_id = c.store_id
left join reporting_media_base_prod.facebook.vw_facebook_naming_convention n on cast(n.ad_id as varchar) = cast(c.ad_id as varchar)
    and c.channel = 'fb+ig'
left join reporting_media_base_prod.tiktok.vw_tiktok_naming_convention n2 on cast(n2.ad_id as varchar) = cast(c.ad_id as varchar)
    and c.channel = 'tiktok'
left join reporting_media_base_prod.snapchat.vw_snapchat_naming_convention n3 on cast(n3.ad_id as varchar) = cast(c.ad_id as varchar)
    and c.channel = 'snapchat'
left join reporting_media_base_prod.google_ads.vw_google_ads_naming_convention n4 on cast(lower(n4.ad_id) as varchar) = cast(lower(c.ad_id) as varchar)
    and c.channel = 'youtube' and n4.channel = 'youtube'
left join (select distinct store_brand_name, account_name, account_id, campaign_name, campaign_id, adgroup_name, adgroup_id, ad_name, ad_id, ad_id_utm
            from reporting_media_base_prod.twitter.vw_twitter_naming_convention where lower(ad_id_utm) not in ('unclassified','none')) n5 on cast(lower(n5.ad_id_utm) as varchar) = cast(lower(c.ad_id) as varchar)
    and c.channel = 'twitter'
left join reporting_media_base_prod.pinterest.vw_pinterest_naming_convention n6 on cast(n6.ad_id as varchar) = cast(c.ad_id as varchar)
    and c.channel = 'pinterest'
where c.session_datetime >= current_date() - 30 and c.subchannel != 'google' or c.channel = 'youtube'
group by 1,2,3,4,5,6,7,8,9
union
select
       c.session_datetime as session_datetime,
       cast(c.session_datetime as date) as session_date,
       date_trunc(hour,session_datetime) as session_hour,
       coalesce(n5.store_brand_name,n6.store_brand_name) as store_brand_name,
       st.store_region as region,
       st.store_country as country,
       c.channel,
       c.subchannel,
    coalesce(n5.account_name,n6.account_name) as account_name,
    count(*)
from reporting_media_base_prod.dbo.realtime_sessions c
join edw_prod.data_model.dim_store st on st.store_id = c.store_id
left join (select distinct store_id,store_brand_name, channel, subchannel, account_name, account_id, campaign_id, campaign_name
           from reporting_media_prod.google_ads.google_ads_optimization_dataset
            where channel != 'youtube' and date > current_date() - 30) n5
            on cast(lower(n5.campaign_name) as varchar) = cast(lower(c.campaign_id) as varchar)
                and c.store_id = n5.store_id
left join (select distinct store_id,store_brand_name, channel, subchannel, account_name, account_id, campaign_id, campaign_name
           from reporting_media_prod.google_search_ads_360.doubleclick_campaign_optimization_dataset
            where channel in ('non branded search','branded search','shopping') and subchannel = 'google' and date > current_date() - 30) n6
            on cast(lower(n6.campaign_name) as varchar) = cast(lower(c.campaign_id) as varchar)
                and c.store_id = n6.store_id
where c.session_datetime >= current_date() - 30 and c.subchannel = 'google' and c.channel != 'youtube'
group by 1,2,3,4,5,6,7,8,9;
