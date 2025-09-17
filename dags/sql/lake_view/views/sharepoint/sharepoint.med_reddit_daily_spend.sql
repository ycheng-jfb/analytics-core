CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_REDDIT_DAILY_SPEND as
select date::date as date,
    business_unit as business_unit_abbr,
    country,
    store_id,
    advertiser_id as account_id,
    spend,
    impressions,
    clicks,
     _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.reddit_spend_flus
UNION
select date::date as date,
    business_unit as business_unit_abbr,
    country,
    store_id,
    advertiser_id as account_id,
    spend,
    impressions,
    clicks,
     _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.reddit_spend_flmus;
