CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_CRITEO_SPEND as
select date::date as date,
    business_unit,
    country,
    spend,
    impressions,
    clicks,
    currency_code,
    _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_spend_v1.criteo_spend_customer_data;
