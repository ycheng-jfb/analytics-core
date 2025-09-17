CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_ROKU_SPEND as
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    is_mens_flag,
    advertiser_id,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.roku_spend_sxna
union
select date::date as date,
    business_unit,
    region,
    country,
    store_id,
    is_mens_flag,
    advertiser_id,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.roku_spend_flna;
