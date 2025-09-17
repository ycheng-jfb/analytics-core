CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SX_LEAD_SURVEY_MATCHBACK as
SELECT customer_id,
        customer_email,
        membership_id,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.lead_data_for_survey_matchback_customer_data;
