CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_LIVEINTENT_SPEND as
select date::date as date,
    business_unit as business_unit_abbr,
    country,
    store_id,
    id,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.live_intent_spend_fkus
union
select date::date as date,
    business_unit as business_unit_abbr,
    country,
    store_id,
    id,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.live_intent_spend_jfus
union
select date::date as date,
    business_unit as business_unit_abbr,
    country,
    store_id,
    id,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.live_intent_spend_sdus
union
select date::date as date,
    business_unit as business_unit_abbr,
    country,
    store_id,
    id,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.live_intent_spend_flus
union
select date::date as date,
    business_unit as business_unit_abbr,
    country,
    store_id,
    id,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.live_intent_spend_sxfus;
