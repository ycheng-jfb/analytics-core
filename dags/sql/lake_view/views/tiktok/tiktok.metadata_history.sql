CREATE OR REPLACE VIEW LAKE_VIEW.TIKTOK.METADATA_HISTORY AS
with ad as (
select ad_id,adgroup_id,campaign_id,advertiser_id,ad_name,operation_status
from lake_fivetran.MED_TIKTOK_V1.AD_HISTORY
QUALIFY ROW_NUMBER() OVER(PARTITION BY ad_id order by updated_at desc) = 1
), adgroup as (
select adgroup_id,adgroup_name, operation_status
from lake_fivetran.MED_TIKTOK_V1.ADGROUP_HISTORY
QUALIFY ROW_NUMBER() OVER(PARTITION BY adgroup_id order by updated_at desc) = 1
), campaign as (
select campaign_id,campaign_name, operation_status
from lake_fivetran.MED_TIKTOK_V1.CAMPAIGN_HISTORY
QUALIFY ROW_NUMBER() OVER(PARTITION BY campaign_id order by updated_at desc) = 1
)
select a.ad_id,
       a.ad_name,
       a.operation_status as ad_operation_status,
       a.adgroup_id, ag.adgroup_name,
       ag.operation_status as adgroup_operation_status,
       a.campaign_id,
       c.campaign_name,
       c.operation_status as campaign_operation_status
from ad as a
left join adgroup as ag on ag.adgroup_id = a.adgroup_id
left join campaign as c on c.campaign_id = a.campaign_id;
