create or replace transient table reporting_media_base_prod.tiktok.metadata as
select
        account_id,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        ad_id,
        ad_name,
        ad_is_active_flag,
        adgroup_is_active_flag,
        embedded_image,
        video_url,
        current_timestamp()::timestamp_ltz as meta_create_datetime,
        current_timestamp()::timestamp_ltz as meta_update_datetime
    from (
        select distinct
            id.advertiser_id as account_id,
            id.campaign_id,
            id.adgroup_id,
            id.ad_id,
            adgroup.adgroup_name,
            campaign.campaign_name,
            ad.ad_name,
            ad.ad_is_active_flag,
            adgroup.adgroup_is_active_flag,
            adlink.embedded_image,
            adlink.video_url
        from LAKE_VIEW.TIKTOK.DAILY_SPEND id

        left join (
            select distinct
                t4.ad_id,
                replace(ad_name,'	','') as ad_name,
                ad_operation_status as ad_is_active_flag,
                row_number() over(partition by t4.ad_id order by date desc) as rn_ad
            from LAKE_VIEW.TIKTOK.DAILY_SPEND t4
        left join (select distinct ad_id, ad_operation_status from lake_view.tiktok.metadata_history) t3 on t4.ad_id = t3.ad_id
        )  as ad on ad.ad_id = id.ad_id and rn_ad = 1

        left join (
            select
                campaign_id,
                replace(campaign_name,'	','') as campaign_name,
                row_number() over(partition by campaign_id order by date desc) as rn_campaign
            from LAKE_VIEW.TIKTOK.DAILY_SPEND) as campaign
                  on campaign.campaign_id = id.campaign_id and rn_campaign = 1

        left join (
            select
            t1.adgroup_id,
            replace(adgroup_name,'  ','') as adgroup_name,
            adgroup_operation_status as adgroup_is_active_flag,
            row_number() over(partition by t1.adgroup_id order by date desc) as rn_adgroup
        from lake_view.tiktok.daily_spend t1
     left join (select distinct adgroup_id, adgroup_operation_status from lake_view.tiktok.METADATA_HISTORY) t2 on t1.adgroup_id = t2.adgroup_id
    ) as adgroup on adgroup.adgroup_id = id.adgroup_id and rn_adgroup = 1

        left join (
            select
                ad_id,
                poster_url as embedded_image,
                URL as video_url
            from lake_view.tiktok.ad_link) as adlink
                on adlink.ad_id=id.ad_id

where id.campaign_id != 0 and id.adgroup_id != 0
            );

