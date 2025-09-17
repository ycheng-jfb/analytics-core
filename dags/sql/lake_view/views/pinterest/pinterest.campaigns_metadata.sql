CREATE OR REPLACE VIEW LAKE_VIEW.PINTEREST.CAMPAIGNS_METADATA(
      advertiser_id
    ,campaign_budget_optimization_enabled
    ,created_time
    ,creation_context
    ,daily_spend_cap
    ,default_ad_group_budget_in_micro_currency
    ,end_time
    ,holdout_experiment
    ,holdout_experiment_cell
    ,holdout_survey
    ,id
    ,integration_platform_equivalent_id
    ,integration_platform_type
    ,lifetime_spend_cap
    ,name
    ,objective_type
    ,order_line_id
    ,start_time
    ,status
    ,summary_status
    ,tracking_urls
    ,type
    ,updated_time
    ,META_CREATE_DATETIME
    ,META_UPDATE_DATETIME
) AS
SELECT advertiser_id
,IS_CAMPAIGN_BUDGET_OPTIMIZATION
,created_time
,creation_context
,daily_spend_cap
,default_ad_group_budget_in_micro_currency
,end_time
,holdout_experiment
,holdout_experiment_cell
,holdout_survey
,id
,integration_platform_equivalent_id
,integration_platform_type
,lifetime_spend_cap
,name
,objective_type
,order_line_id
,start_time
,status
,summary_status
,tracking_urls
,'campaign' as type
,updated_time
,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) AS META_CREATE_DATETIME
,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) as META_UPDATE_DATETIME
from  LAKE_FIVETRAN.MED_PINTEREST_AD30_V1.CAMPAIGN_HISTORY;
