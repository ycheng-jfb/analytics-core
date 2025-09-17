CREATE OR REPLACE VIEW LAKE_VIEW.PINTEREST.AD_GROUPS_METADATA(
    advertiser_id
    ,auto_targeting_enabled
    ,bid_in_micro_currency
    ,bid_strategy_type
    ,billable_event
    ,budget_in_micro_currency
    ,budget_type
    ,campaign_id
    ,conversion_learning_mode_type
    ,created_time
    ,creation_context
    ,dca_assets
    ,end_time
    ,feed_profile_id
    ,id
    ,name
    ,optimization_goal_metadata
    ,pacing_delivery_type
    ,placement_group
    ,start_time
    ,status
    ,summary_status
    ,TARGETING_SPEC_INTEREST
    ,TARGETING_SPEC_APPTYPE
    ,TARGETING_SPEC_GENDER
    ,TARGETING_SPEC_LOCATION
    ,TARGETING_SPEC_AUDIENCE_EXCLUDE
    ,TARGETING_SPEC_AUDIENCE_INCLUDE
    ,TARGETING_SPEC_LOCALE
    ,TARGETING_SPEC_TARGETING_STRATEGY
    ,TARGETING_SPEC_CATEGORY
    ,TARGETING_SPEC_AGE_BUCKET
    ,TARGETING_SPEC_SHOPPING_RETARGETING
    ,TARGETING_SPEC_GEO
    ,tracking_urls
    ,type
    ,updated_time
    ,META_CREATE_DATETIME
    ,META_UPDATE_DATETIME
    ) AS
select ah.ad_account_id
    ,ah.auto_targeting_enabled
    ,ah.bid_in_micro_currency
    ,ah.bid_strategy_type
    ,ah.billable_event
    ,ah.budget_in_micro_currency
    ,ah.budget_type
    ,ah.campaign_id
    ,ah.conversion_learning_mode_type
    ,ah.created_time
    ,creation_context
    ,dca_assets
    ,ah.end_time
    ,ah.feed_profile_id
    ,ah.id
    ,ah.name
    ,optimization_goal_metadata
    ,ah.pacing_delivery_type
    ,ah.placement_group
    ,ah.start_time
    ,ah.status
    ,ah.summary_status
    ,TARGETING_SPEC_INTEREST
    ,TARGETING_SPEC_APPTYPE
    ,TARGETING_SPEC_GENDER
    ,TARGETING_SPEC_LOCATION
    ,TARGETING_SPEC_AUDIENCE_EXCLUDE
    ,TARGETING_SPEC_AUDIENCE_INCLUDE
    ,TARGETING_SPEC_LOCALE
    ,TARGETING_SPEC_TARGETING_STRATEGY
    ,TARGETING_SPEC_CATEGORY
    ,TARGETING_SPEC_AGE_BUCKET
    ,TARGETING_SPEC_SHOPPING_RETARGETING
    ,TARGETING_SPEC_GEO
    ,ph.tracking_url
    ,'adgroup' as type
    ,ah.updated_time
    ,convert_timezone('America/Los_Angeles',ah._FIVETRAN_SYNCED) AS META_CREATE_DATETIME
    ,convert_timezone('America/Los_Angeles',ah._FIVETRAN_SYNCED) as META_UPDATE_DATETIME
from  LAKE_FIVETRAN.MED_PINTEREST_AD30_V1.AD_GROUP_HISTORY ah
LEFT JOIN (select ad_group_id ,listagg(tracking_url,',') as tracking_url
            from LAKE_FIVETRAN.MED_PINTEREST_AD30_V1.product_group_history
            group by all) ph on ah.id=ph.ad_group_id
;
