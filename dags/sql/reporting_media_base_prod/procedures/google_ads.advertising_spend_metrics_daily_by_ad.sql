
/*
-- table definition
create or replace transient table reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad
(
        account_id number(38),
        campaign_id number(38),
        adgroup_id number(38),
        ad_id number(38),
        date date,
        spend_account_currency decimal(20,4),
        spend_usd decimal(20,4),
        spend_local decimal(20,4),
        impressions int,
        clicks int,
        video_views int,
        video_views_25pct int,
        video_views_50pct int,
        video_views_75pct int,
        video_views_100pct int,
        high_watermark_datetime timestamp_ltz,
        meta_create_datetime timestamp_ltz,
        meta_update_datetime timestamp_ltz
);
*/

/*
 get the latest date change from the source
 get the latest time this table has processed

 which ever one is earlier, choose the earliest day and reprocess 5 days before that
 */

set high_watermark_datetime = (select max(meta_update_datetime)::timestamp_ltz(9) from lake_view.google_ads.ad_spend);
set datetime_update_from = (select coalesce((select max(high_watermark_datetime) from reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad),'2010-01-01'));


merge into reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad target
using (
    select
        src.account_id,
        src.campaign_id,
        src.adgroup_id,
        src.ad_id,
        src.date,
        src.spend_account_currency,
        src.spend_usd,
        src.spend_local,
        src.impressions,
        src.clicks,
        src.video_views,
        src.video_views_25pct,
        src.video_views_50pct,
        src.video_views_75pct,
        src.video_views_100pct,
        $high_watermark_datetime as high_watermark_datetime,
        current_timestamp() as meta_create_datetime,
        current_timestamp() as meta_update_datetime
    from (
            select
                external_customer_id as account_id,
                campaign_id,
                ad_group_id as adgroup_id,
                ad_id,
                date,
                sum(cost/1000000) as spend_account_currency,
                sum((cost/1000000)*coalesce(lkpusd.exchange_rate,1)) as spend_usd,
                sum((cost/1000000)*coalesce(lkplocal.exchange_rate,1)) as spend_local,
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(video_views) as video_views,
                sum(video_views*video_quartile_25_rate) as video_views_25pct,
                sum(video_views*video_quartile_50_rate) as video_views_50pct,
                sum(video_views*video_quartile_75_rate) as video_views_75pct,
                sum(video_views*video_quartile_100_rate) as video_views_100pct
            from lake_view.google_ads.ad_spend g
            left join lake_view.sharepoint.med_account_mapping_media am on am.source_id = g.external_customer_id::string
                    and am.source ilike '%adwords%'
            left join edw_prod.data_model.dim_store st on st.store_id = am.store_id
            left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
                    and g.date = lkpusd.rate_date_pst
                    and lkpusd.dest_currency = 'USD'
            left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
                    and g.date = lkplocal.rate_date_pst
                    and lkplocal.dest_currency = st.store_currency
			where g.meta_update_datetime >= $datetime_update_from
            group by 1,2,3,4,5
        ) src
    ) as new on target.ad_id = new.ad_id
            and target.adgroup_id = new.adgroup_id
            and target.date = new.date
    when not matched then insert (account_id,campaign_id,adgroup_id,ad_id,date,spend_account_currency,spend_usd,spend_local,impressions,clicks,video_views,video_views_25pct,video_views_50pct,video_views_75pct,video_views_100pct,high_watermark_datetime,meta_create_datetime,meta_update_datetime)
            values (new.account_id,new.campaign_id,new.adgroup_id,new.ad_id,new.date,new.spend_account_currency,new.spend_usd,new.spend_local,new.impressions,new.clicks,new.video_views,new.video_views_25pct,new.video_views_50pct,new.video_views_75pct,new.video_views_100pct,new.high_watermark_datetime,new.meta_create_datetime,new.meta_update_datetime)
    when matched then update
        set spend_account_currency = coalesce(new.spend_account_currency,0),
            spend_usd = coalesce(new.spend_usd,0),
            spend_local = coalesce(new.spend_local,0),
            impressions = coalesce(new.impressions,0),
            clicks = coalesce(new.clicks,0),
            video_views = coalesce(new.video_views,0),
            video_views_25pct = coalesce(new.video_views_25pct,0),
            video_views_50pct = coalesce(new.video_views_50pct,0),
            video_views_75pct = coalesce(new.video_views_75pct,0),
            video_views_100pct = coalesce(new.video_views_100pct,0),
            high_watermark_datetime = $high_watermark_datetime,
            meta_update_datetime = current_timestamp();

-- create spend table with performance max
create or replace temporary table _performance_max_spend as
select
    customer_id as account_id,
    campaign_id,
    campaign_id as adgroup_id,
    campaign_id as ad_id,
    date,
    sum(cost_micros/1000000) as spend_account_currency,
    sum((cost_micros/1000000)*coalesce(lkpusd.exchange_rate,1)) as spend_usd,
    sum((cost_micros/1000000)*coalesce(lkplocal.exchange_rate,1)) as spend_local,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    null as video_views,
    null as video_views_25pct,
    null as video_views_50pct,
    null as video_views_75pct,
    null as video_views_100pct,
    $high_watermark_datetime as high_watermark_datetime,
    current_timestamp() as meta_create_datetime,
    current_timestamp() as meta_update_datetime
from lake_view.google_ads.performance_max_spend_by_campaign pmax
left join lake_view.sharepoint.med_account_mapping_media am on am.source_id = pmax.customer_id::string
        and am.source ilike '%adwords%'
left join edw_prod.data_model.dim_store st on st.store_id = am.store_id
left join edw_prod.reference.currency_exchange_rate_by_date lkpusd on am.currency = lkpusd.src_currency
        and pmax.date = lkpusd.rate_date_pst
        and lkpusd.dest_currency = 'USD'
left join edw_prod.reference.currency_exchange_rate_by_date lkplocal on am.currency = lkplocal.src_currency
        and pmax.date = lkplocal.rate_date_pst
        and lkplocal.dest_currency = st.store_currency
where lower(advertising_channel_type) ='performance_max'
group by 1,2,3,4,5;

create or replace transient table reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad_with_pmax as
select * from _performance_max_spend
union
select * from reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad;
