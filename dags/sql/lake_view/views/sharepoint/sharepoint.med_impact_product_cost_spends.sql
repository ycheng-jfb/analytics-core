CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_IMPACT_PRODUCT_COST_SPENDS AS
select month_date::date as month_date,
    brand,
    country,
    channel,
    tier,
    spend,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.MED_SHAREPOINT_INFLUENCER_V1.INFLUENCER_SPREADHSEET_NEW_PRODUCT_COST_SPENDS;
