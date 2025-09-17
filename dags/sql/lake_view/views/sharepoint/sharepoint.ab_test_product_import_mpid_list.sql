create or replace view lake_view.sharepoint.ab_test_product_import_mpid_list(
    CAMPAIGN_CODE,
    TEST_KEY_VERSION,
    MASTER_PRODUCT_ID,
    PRODUCT_SKU,
    PRODUCT_EFFECTIVE_START_DATETIME_PST,
    PRODUCT_EFFECTIVE_END_DATETIME_PST,
    INACTIVE_TEST_PRODUCT_OUTCOME,
	META_ROW_HASH,
	META_CREATE_DATETIME,
	META_UPDATE_DATETIME
) as
select
    CAMPAIGN_CODE,
    TEST_KEY_VERSION,
    MASTER_PRODUCT_ID,
    PRODUCT_SKU,
    PRODUCT_EFFECTIVE_START_DATETIME_PST::TIMESTAMP_NTZ AS PRODUCT_EFFECTIVE_START_DATETIME_PST,
    PRODUCT_EFFECTIVE_END_DATETIME_PST::TIMESTAMP_NTZ AS PRODUCT_EFFECTIVE_END_DATETIME_PST,
    INACTIVE_TEST_PRODUCT_OUTCOME,
    hash(
        PRODUCT_SKU,
        INACTIVE_TEST_PRODUCT_OUTCOME,
        TEST_KEY_VERSION,
        CAMPAIGN_CODE,
        PRODUCT_EFFECTIVE_END_DATETIME_PST,
        MASTER_PRODUCT_ID,
        PRODUCT_EFFECTIVE_START_DATETIME_PST
    ) as meta_row_hash,
    _fivetran_synced :: timestamp as meta_create_datetime,
    _fivetran_synced :: timestamp as meta_update_datetime
from
    lake_fivetran.webanalytics_sharepoint_v1.ab_test_product_import_mpid_list;
