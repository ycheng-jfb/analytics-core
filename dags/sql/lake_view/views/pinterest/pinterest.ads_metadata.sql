CREATE OR REPLACE VIEW LAKE_VIEW.PINTEREST.ADS_METADATA(
ad_group_id
    ,advertiser_id
    ,android_deep_link
    ,campaign_id
    ,campaign_objective_type
    ,carousel_android_deep_links
    ,carousel_destination_urls
    ,carousel_ios_deep_links
    ,click_tracking_url
    ,collection_items_destination_url_template
    ,created_time
    ,creation_context
    ,creative_type
    ,destination_url
    ,frequency_cap
    , id
    ,ios_deep_link
    ,is_pin_deleted
    ,is_removable
    ,lead_id
    , name
    ,pin_id
    ,rejected_reasons
    ,rejection_labels
    ,review_status
    , status
    ,summary_status
    ,tracking_urls
    , type
    ,updated_time
    ,view_tracking_url
    ,META_CREATE_DATETIME
    ,META_UPDATE_DATETIME
    ) AS
select ad_group_id
    , AD_ACCOUNT_ID
    ,android_deep_link
    ,campaign_id
    ,objective_type as campaign_objective_type
    ,android_deep_link
    ,destination_url
    ,ios_deep_link
    ,click_tracking_url
    ,collection_items_destination_url_template
    ,ph.created_time
    ,ph.creation_context
    ,creative_type
    ,destination_url
    ,frequency_cap
    ,ph.id
    ,ios_deep_link
    ,is_pin_deleted
    ,is_removable
    ,lead_id
    ,ph.name
    ,pin_id
    ,rejected_reasons
    ,lh.label as rejection_labels
    ,review_status
    ,ph.status
    ,ph.summary_status
    ,ph.tracking_urls
    ,'pinpromotion' as type
    ,ph.updated_time
    ,view_tracking_url
    ,convert_timezone('America/Los_Angeles',ph._FIVETRAN_SYNCED) AS META_CREATE_DATETIME
    ,convert_timezone('America/Los_Angeles',ph._FIVETRAN_SYNCED) as META_UPDATE_DATETIME
from  LAKE_FIVETRAN.MED_PINTEREST_AD30_V1.pin_promotion_history ph
left join LAKE_FIVETRAN.MED_PINTEREST_AD30_V1.campaign_history ch on ph.campaign_id=ch.id
left join lake_fivetran.med_pinterest_ad30_v1.ad_rejected_label_history lh on ph.id=lh.pin_promotion_id
;
