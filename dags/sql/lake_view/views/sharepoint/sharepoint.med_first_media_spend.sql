CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_FIRST_MEDIA_SPEND as
select start_date::date as start_date,
    end_date::date as end_date,
    business_unit,
    region,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.first_media_spend_flw_facebook
union
select start_date::date as start_date,
    end_date::date as end_date,
    business_unit,
    region,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.first_media_spend_flm_facebook
union
select start_date::date as start_date,
    end_date::date as end_date,
    business_unit,
    region,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.first_media_spend_yitty_facebook
union
select start_date::date as start_date,
    end_date::date as end_date,
    business_unit,
    region,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.first_media_spend_flw_tik_tok
union
select start_date::date as start_date,
    end_date::date as end_date,
    business_unit,
    region,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.first_media_spend_flm_tik_tok
union
select start_date::date as start_date,
    end_date::date as end_date,
    business_unit,
    region,
    country,
    store_id,
    is_mens_flag,
    channel,
    subchannel,
    vendor,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.first_media_spend_yitty_tik_tok;
