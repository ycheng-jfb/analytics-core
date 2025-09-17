
---------------------------
-- purpose: create cleansed spend that includes linear and streaming

--streaming
create or replace temporary table _streaming_spend as
select
    'tv+streaming' as channel,
    'streaming' as subchannel,
    case when lower(creative_name) ilike '%flm%' then 'Fabletics Men'
        when lower(creative_name) ilike '%yty%' then 'Yitty'
        when lower(creative_name) ilike '%scrubs%' then 'Fabletics Scrubs'
    else t3.store_brand end as store_brand_name,
    iff(store_brand_name='Fabletics Men','FLM',store_brand_abbr) as store_brand_abbr,
    t3.store_id,
    iff(right(campaign_id, 1) = '_', concat(campaign_id,creative_code), concat(campaign_id, '_', creative_code)) as ad_id,
    creative_name as ad_name,
    campaign_id,
    date,
    case when creative_code ilike 'SX%' then substring(creative_code,0,length(creative_code)-1)
        else creative_code
    end as creative_code,
    iff(platform is null,'Unknown',platform) as platform,
    'null' as network,
    'null' as program,
    sum(effective_spend) as spend,
    sum(impressions) as impressions
from lake.tatari.streaming_spend_and_impression t1
join lake_view.sharepoint.med_account_mapping_media t2 on t1.account_name = t2.source_id
    and lower(t2.source) ilike '%tatari%'
join edw_prod.data_model.dim_store t3 on t2.store_id = t3.store_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;

--linear
create or replace temporary table _linear_spend as
select
    'tv+streaming' as channel,
    'tv' as subchannel,
    case when lower(creative_name) ilike '%flm%' then 'Fabletics Men'
        when lower(creative_name) ilike '%yty%' then 'Yitty'
        when lower(creative_name) ilike '%scrubs%' then 'Fabletics Scrubs'
    else t3.store_brand end as store_brand_name,
    iff(store_brand_name='Fabletics Men','FLM',store_brand_abbr) as store_brand_abbr,
    t3.store_id,
    iff(right(spot_id, 1) = '_', concat(spot_id,creative_code), concat(spot_id, '_', creative_code)) as ad_id,
    creative_name as ad_name,
    spot_id as campaign_id,
    cast(spot_datetime as date) as date,
    case when creative_code ilike 'SX%' then substring(creative_code,0,length(creative_code)-1)
        else creative_code
    end as creative_code,
    'null' as platform,
    iff(network is null,'Unknown',network) as network,
    iff(program is null,'Unknown',program) as program,
    sum(spend) as spend,
    sum(impressions) as impressions
from lake.tatari.linear_spend_and_impressions t1
join lake_view.sharepoint.med_account_mapping_media t2 on t1.account_name = t2.source_id
    and lower(t2.source) ilike '%tatari%'
join edw_prod.data_model.dim_store t3 on t2.store_id = t3.store_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;

create or replace transient table reporting_media_base_prod.tatari.daily_spend_by_creative as
select
    *,
    current_timestamp()::timestamp_ltz as spend_meta_create_datetime,
    current_timestamp()::timestamp_ltz as spend_meta_update_datetime
from (
         select *
         from _streaming_spend
         union
         select *
         from _linear_spend
     );
