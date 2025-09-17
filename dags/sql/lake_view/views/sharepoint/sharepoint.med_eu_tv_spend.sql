CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_EU_TV_SPEND as
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_jffr
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_flfr
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_jfde
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_flde
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_jfuk
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_fluk
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_flm_uk
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_flnl
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_sxfr
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_sxuk
UNION
select date::date as date,
    business_unit,
    is_mens_flag,
    country,
    region,
    channel,
    subchannel,
    vendor,
    currency,
    spend,
    impressions,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.eu_tv_spend_sxde
;
