create or replace transient table reporting_media_base_prod.pinterest.metadata as
select distinct
account_id,
account_name,
campaign_id,
campaign_name,
adgroup_id,
adgroup_name,
ad_id,
ad_name,
conversion_event,
pin_promotion_url,
link_to_pin,
current_timestamp()::timestamp_ltz as meta_create_datetime,
current_timestamp()::timestamp_ltz as meta_update_datetime
from (
    select distinct
        spend.account_id,
        adv.account_name,
        spend.campaign_id,
        cam.campaign_name,
        spend.adgroup_id,
        adgr.adgroup_name,
        spend.ad_id,
        ad.ad_name,
        agmeta.conversion_event,
        pinmeta.pin_promotion_url,
        pinmeta.link_to_pin
    from reporting_media_base_prod.pinterest.spend_metrics_daily_by_ad spend

    left join (
        select distinct
            pin_promotion_name as ad_name,
            pin_promotion_id,
            row_number() over(partition by pin_promotion_id order by meta_update_datetime desc) as rn_ad
        from lake_view.pinterest.spend_ad1) as ad on ad.pin_promotion_id = spend.ad_id and rn_ad = 1

     left join (
        select distinct
            pin_promotion_ad_group_name as adgroup_name,
            pin_promotion_ad_group_id,
            row_number() over(partition by pin_promotion_ad_group_id order by meta_update_datetime desc) as rn_adgr
        from lake_view.pinterest.spend_ad1) as adgr on adgr.pin_promotion_ad_group_id = spend.adgroup_id and rn_adgr = 1

    left join (
        select distinct
            pin_promotion_campaign_name as campaign_name,
            pin_promotion_campaign_id,
            row_number() over(partition by pin_promotion_campaign_id order by meta_update_datetime desc) as rn_cam
        from lake_view.pinterest.spend_ad1) as cam on cam.pin_promotion_campaign_id = spend.campaign_id and rn_cam = 1

    left join (
        select distinct
            advertiser_name as account_name,
            advertiser_id,
            row_number() over(partition by advertiser_id order by meta_update_datetime desc) as rn_adv
        from lake_view.pinterest.spend_ad1) as adv on adv.advertiser_id = spend.account_id and rn_adv = 1
    left join (
        select distinct
            id as ad_group_id,
            type as entity_type,
            parse_json(replace(optimization_goal_metadata, 'None', '-1')):conversion_tag_v3_goal_metadata:conversion_event::varchar conversion_event
        from lake_view.pinterest.ad_groups_metadata) as agmeta on spend.adgroup_id = agmeta.ad_group_id
    left join (
        select
            id as pin_promotion_id,
            type as entity_type,
            destination_url pin_promotion_url,
            'https://www.pinterest.com/pin/' || pin_id || '/' as link_to_pin
        from lake_view.pinterest.ads_metadata) as pinmeta on spend.ad_id = pinmeta.pin_promotion_id
where spend.campaign_id is not null and spend.adgroup_id is not null
);
