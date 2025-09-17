CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_FL_TRAINERS_INTAKE as
SELECT customer_id,
        name,
        fl_account_email,
        start_date,
        source,
        country,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.trainer_intake_fabletics qualify row_number() OVER (
                PARTITION BY customer_id ORDER BY start_date DESC
                ) = 1;
