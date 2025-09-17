CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_ROKT_SPEND as
select date::date as date,
    business_unit,
    country,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.rokt_spend_jfus
union
select date::date as date,
    business_unit,
    country,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.rokt_spend_sdus
union
select date::date as date,
    business_unit,
    country,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.rokt_spend_flus;
