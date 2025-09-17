CREATE OR REPLACE VIEW LAKE_VIEW.FIVETRAN_LOG.USER as
SELECT ID,
        GIVEN_NAME,
        FAMILY_NAME,
        EMAIL,
        EMAIL_DISABLED,
        VERIFIED,
        CREATED_AT,
        PHONE,
        _fivetran_deleted,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    FROM LAKE_FIVETRAN.FIVETRAN_LOG_V1.USER;
