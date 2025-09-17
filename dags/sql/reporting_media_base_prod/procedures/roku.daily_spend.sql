create or replace transient table reporting_media_base_prod.roku.daily_spend as
select
    'tv+streaming' as channel,
    'roku streaming' as subchannel,
    ds.store_brand as store_brand_name,
    ds.store_brand_abbr,
    ds.store_id,
    date,
    advertiser_uid as account_id,
    tactic as tactic_name,
    campaign as campaign_name,
    campaign_uid as campaign_id,
    flight as flight_name,
    flight_uid as flight_id,
    creative_name as ad_name,
    concat(flight,'_',campaign,'_', creative_name) as ad_id,
    IFF(store_brand = 'Savage X','30DV','14DV') as optimization_window,
    --parse out creative_code
    reporting_media_base_prod.dbo.fn_text_to_underscore(iff(creative_name ilike '%mp4%',substring(creative_name,0,length(creative_name)-4),creative_name),
    '_Z-') as creative_code,

    sum(total_spend) as spend,
    sum(impressions) as impressions,
    sum(campaign_clicks) as clicks,

    current_timestamp()::timestamp_ltz as spend_meta_create_datetime,
    current_timestamp()::timestamp_ltz as spend_meta_update_datetime

from lake.roku.daily_spend r
join lake_view.sharepoint.med_account_mapping_media am on r.advertiser_uid = am.source_id
    and lower(am.source) ilike '%roku%'
join edw_prod.data_model.dim_store ds on am.store_id = ds.store_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;
