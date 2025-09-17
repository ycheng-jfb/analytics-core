CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_TABOOLA_SPEND as
select date::date as date,
    business_unit,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.taboola_spend_flus
union
select date::date as date,
    business_unit,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.taboola_spend_flmus
union
select date::date as date,
    business_unit,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.taboola_spend_ytyus
;
