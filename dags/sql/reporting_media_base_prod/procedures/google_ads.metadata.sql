set data_update_datetime =
        (select least(
        (select max(meta_update_datetime) from lake_view.google_ads.account_metadata),
        (select max(meta_update_datetime) from lake_view.google_ads.campaign_metadata),
        (select max(meta_update_datetime) from lake_view.google_ads.ad_group_metadata),
        (select max(meta_update_datetime) from lake_view.google_ads.ad_metadata),
        (select max(meta_update_datetime) from reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad)));

create or replace temporary table _ids_to_name as
select distinct account_id, campaign_id, adgroup_id, ad_id,
                row_number() over (partition by ad_id,adgroup_id order by meta_create_datetime desc) as rn
from reporting_media_base_prod.google_ads.advertising_spend_metrics_daily_by_ad_with_pmax;

create or replace transient table reporting_media_base_prod.google_ads.metadata as
select
           id.account_id,
           id.campaign_id,
           id.adgroup_id,
           id.ad_id,
           ac.account_name,
           c.campaign_name,
           ag.adgroup_name,
           ad.ad_name,
           ad.ad_labels,
           ad.ad_type,
           ad.status,

           $data_update_datetime as latest_data_update_datetime,
           current_timestamp()   as meta_create_datetime,
           current_timestamp()   as meta_update_datetime

    from ((select * from _ids_to_name where rn = 1) as id
        left join (
        select distinct
               external_customer_id as account_id,
               accountdescriptivename as account_name,
               row_number() over (partition by external_customer_id order by meta_update_datetime desc) as rn_account
            from lake_view.google_ads.account_metadata) as ac
                       on ac.account_id = id.account_id and rn_account = 1
        left join (
        select distinct
               campaign_id,
               campaign_name,
               row_number() over (partition by campaign_id order by meta_update_datetime desc) as rn_campaign
            from lake_view.google_ads.campaign_metadata) as c
                       on c.campaign_id = id.campaign_id and rn_campaign = 1
        left join (
        select distinct
               ad_group_id as adgroup_id,
               ad_group_name as adgroup_name,
               row_number() over (partition by ad_group_id order by meta_update_datetime desc) as rn_adgroup
            from lake_view.google_ads.ad_group_metadata) as ag
                       on ag.adgroup_id = id.adgroup_id and rn_adgroup = 1
        left join (select am.ad_id,
                           coalesce(am.ad_type, l.ad_type) ad_type,
                           coalesce(am.image_creative_name, l.image_creative_name) as ad_name,
                           l.labels as ad_labels,
                           coalesce(am.status, l.status) status,
                           row_number() over (partition by am.ad_id order by am.meta_update_datetime desc) as rn_ad
                    from lake_view.google_ads.ad_metadata am
                           left join reporting_media_base_prod.google_ads.ad_metadata as l on l.ad_id = am.ad_id
                                and l.ad_group_id = am.ad_group_id
                   ) as ad on ad.ad_id = id.ad_id and rn_ad = 1);
