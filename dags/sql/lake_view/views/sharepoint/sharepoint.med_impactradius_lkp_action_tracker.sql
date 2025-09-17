CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_IMPACTRADIUS_LKP_ACTION_TRACKER as
SELECT account_id,
        account_type,
        campaign_id,
        business_unit,
        country,
        action_tracker_id,
        action_tracker_label,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_influencer_v1.influencer_spend_lkp_action_tracker_events;
