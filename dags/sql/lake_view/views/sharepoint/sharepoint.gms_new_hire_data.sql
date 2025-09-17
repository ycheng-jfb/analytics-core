CREATE OR REPLACE VIEW lake_view.sharepoint.gms_new_hire_data (
    BOND_USERNAME,
    FIRST_NAME,
    LAST_NAME,
    STATUS,
    CLASS_NAME,
    GENESYS_ID,
    START_TRAINING_DATE_Y_M_D_FORMAT,
    START_ACADEMY_DATE_Y_M_D_FORMAT,
    SITE,
    COUNTRY,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    BOND_USERNAME,
    FIRST_NAME,
    LAST_NAME,
    STATUS,
    CLASS_NAME,
    GENESYS_ID,
    START_TRAINING_DATE_Y_M_D_FORMAT_,
    START_ACADEMY_DATE_Y_M_D_FORMAT_,
    SITE,
    COUNTRY,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
FROM LAKE_FIVETRAN.GMS_SHAREPOINT_V1._30_60_90_RAW_DATA_GLOBAL_DATA;
