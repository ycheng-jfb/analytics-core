CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_IMPACTRADIUS_LKP_HDYH as
SELECT media_partner_id,
        how_did_you_hear_answer as howdidyouhear,
        store_id,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_influencer_v1.influencer_spend_hdyh_mapping
where brand is not null;
