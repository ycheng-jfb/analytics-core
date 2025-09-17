CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_TIKTOK_CAMPAIGN_METADATA as
SELECT business_unit,
        country,
        store_id,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        ad_id,
        ad_name,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.tik_tok_tagging_generator_campaign_name_mapping;
