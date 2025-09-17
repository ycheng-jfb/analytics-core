use reporting_base_prod;
SET max_evaluation_batch_id = (
    SELECT MAX(evaluation_batch_id)
    FROM REPORTING_BASE_PROD.SHARED.PRODUCT_IMAGE_TEST_CONFIG_EXPORT);

SELECT $max_evaluation_batch_id;

CREATE OR REPLACE TEMPORARY TABLE _tests_with_dates as
SELECT
    it.master_test_number,
    it.context,
    it.requested_test_id,
    it.master_product_id,
    it.product_sku,
    it.master_store_group_id,
    it.membership_brand_id,
    it.sam_id_current,
    it.sam_id_test,
    it.sort_current,
    it.sort_test,
    it.aem_uuid_current,
    it.aem_uuid_test,
    it.config_json,
    it.datetime_added_sam_current,
    it.datetime_added_sam_test,
    it.datetime_added,
    dt.min_evaluation_batch_id,
    dt.max_evaluation_batch_id,
    dt.min_datetime_added_config_export,
    dt.max_datetime_added_config_export
FROM reporting_prod.data_science.image_testing_image_tests it
LEFT JOIN
    (SELECT
        master_test_number,
        MIN(evaluation_batch_id) AS min_evaluation_batch_id,
        MAX(evaluation_batch_id) AS max_evaluation_batch_id,
        MIN(datetime_added) AS min_datetime_added_config_export,
        CASE
            WHEN MAX(evaluation_batch_id) = $max_evaluation_batch_id THEN TO_TIMESTAMP_NTZ('9999-12-31 00:00:00')
            ELSE TO_TIMESTAMP_NTZ(MAX(datetime_added))
        END AS max_datetime_added_config_export,
        COUNT(*) AS record_count
    FROM
        reporting_base_prod.shared.product_image_test_config_export
    GROUP BY
        master_test_number
    ORDER BY
        master_test_number) dt ON it.master_test_number = dt.master_test_number
ORDER BY
    dt.master_test_number;
-- select * from _tests_with_dates order by MASTER_TEST_NUMBER

CREATE OR REPLACE TEMPORARY TABLE _product_test_times as
select
    EFFECTIVE_START_DATE as PRODUCT_EFFECTIVE_START_DATE_PST
    ,EFFECTIVE_END_DATE as PRODUCT_EFFECTIVE_END_DATE_PST
    ,EVALUATION_BATCH_ID
    ,IS_CURRENT_RECORD
    ,MASTER_PRODUCT_ID as meta_original_master_product_id
    ,MASTER_TEST_NUMBER
from LAKE_FL.ULTRA_ROLLUP.PRODUCT_IMAGE_TEST_CONFIG_HISTORY
where
    IMAGE_TEST_FLAG = TRUE
    and HVR_IS_DELETED = FALSE
    and IS_CURRENT_RECORD = TRUE;

CREATE OR REPLACE TEMPORARY TABLE _product_test_keys as
select
    greatest(tt.PRODUCT_EFFECTIVE_START_DATE_PST,tf.effective_start_datetime) as test_effective_start_datetime
    ,least(tt.PRODUCT_EFFECTIVE_END_DATE_PST,tf.effective_end_datetime) as test_effective_end_datetime
	,tt.EVALUATION_BATCH_ID
    ,tt.IS_CURRENT_RECORD
    ,meta_original_master_product_id
    ,tt.MASTER_TEST_NUMBER
	,tf.CODE as test_key
from _product_test_times tt
left join
    (select
        min(effective_start_datetime) as effective_start_datetime
        ,max(effective_end_datetime) as effective_end_datetime
        ,CODE
        ,'FLimagesort' as CAMPAIGN_CODE
    from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS_HISTORY.TEST_FRAMEWORK
    WHERE
        CAMPAIGN_CODE = 'FLimagesort'
        AND statuscode = 113
    group by CODE) as tf
ON GREATEST(tt.PRODUCT_EFFECTIVE_START_DATE_PST, tf.effective_start_datetime) < LEAST(tt.PRODUCT_EFFECTIVE_END_DATE_PST, tf.effective_end_datetime)
WHERE
    CAMPAIGN_CODE = 'FLimagesort'
    --AND statuscode = 113
order by
    tf.CAMPAIGN_CODE
    ,MASTER_TEST_NUMBER
    ,PRODUCT_EFFECTIVE_START_DATE_PST;

create or replace temp table _REPORTING_BASE_PROD_SHARED_IMAGE_TESTS_WITH_CURRENT_URLS AS
WITH ranked_sam AS (
    SELECT IMAGE_ID,
            SAM_ID,
            AEM_UUID,
            MEMBERSHIP_BRAND_ID,
            MASTER_STORE_GROUP_ID,
            PRODUCT_NAME,
            IS_ECAT,
            IS_PLUS,
            SORT,
            DATETIME_ADDED_SAM,
            DATETIME_MODIFIED_SAM,
            JSON_BLOB,
            _EFFECTIVE_FROM_DATE,
            _EFFECTIVE_TO_DATE,
            _IS_CURRENT,
            _IS_DELETED,
            META_ROW_HASH,
            META_CREATE_DATETIME,
            META_UPDATE_DATETIME,
            IS_TESTABLE,
           ROW_NUMBER() OVER (PARTITION BY SAM_ID, AEM_UUID ORDER BY SORT) AS rn
    FROM LAKE.ELASTICSEARCH.SAM_TOOL
    WHERE _is_current = 1
)
SELECT
    td.master_test_number,
    td.context,
    td.requested_test_id,
    td.master_product_id,
    td.product_sku,
    td.master_store_group_id,
    td.membership_brand_id,
    td.sam_id_current,
    td.sam_id_test,
    td.sort_current,
    td.sort_test,
    td.aem_uuid_current,
    td.aem_uuid_test,
    td.config_json,
    td.datetime_added_sam_current,
    td.datetime_added_sam_test,
    td.datetime_added,
    td.min_evaluation_batch_id,
    td.max_evaluation_batch_id,
    td.min_datetime_added_config_export,
    td.max_datetime_added_config_export,
    st.SORT AS SORT_NOW_CURRENT,
    st2.SORT AS SORT_NOW_TEST,
    CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', td.PRODUCT_SKU, '/', td.PRODUCT_SKU, '-', CAST(st.SORT AS NUMBER(38,0)), '_271x407.jpg') AS IMAGE_URL_NOW_CURRENT,
    CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', td.PRODUCT_SKU, '/', td.PRODUCT_SKU, '-', CAST(st2.SORT AS NUMBER(38,0)), '_271x407.jpg') AS IMAGE_URL_NOW_TEST,
ptt.PRODUCT_EFFECTIVE_START_DATE_PST,
ptt.PRODUCT_EFFECTIVE_END_DATE_PST
FROM
    _tests_with_dates AS td
LEFT JOIN ranked_sam AS st
    ON td.AEM_UUID_CURRENT = st.AEM_UUID
    AND td.SAM_ID_CURRENT = st.sam_id
    AND st.rn = 1
LEFT JOIN ranked_sam AS st2
    ON td.AEM_UUID_TEST = st2.AEM_UUID
    AND td.SAM_ID_TEST = st2.sam_id
    AND st2.rn = 1
left join _product_test_times ptt
    on td.MASTER_TEST_NUMBER=ptt.MASTER_TEST_NUMBER
ORDER BY
    td.MASTER_TEST_NUMBER;

--Selected im_data ouput for clearer exposition
create or replace temporary table _DM_IM_SELECTED_IM_DATA AS
WITH MAX_EVALUATION_BATCHES AS (
    SELECT
        MASTER_TEST_NUMBER,
        MAX(EVALUATION_BATCH_ID) AS MAX_EVALUATION_BATCH_ID
    FROM REPORTING_PROD.DATA_SCIENCE.IMAGE_TESTING_IM_DATA
    group by MASTER_TEST_NUMBER
    order by MASTER_TEST_NUMBER
)
    SELECT
    IMD.MASTER_TEST_NUMBER
    ,IMD.PRODUCT_SKU
    ,IMD.EVALUATION_BATCH_ID
    ,IMD.MAX_RECORD
    ,IMD.GRID_VIEWS_CONTROL_TOTAL
	,IMD.GRID_VIEWS_VARIANT_TOTAL
    ,IMD.GRID_TO_PDP_MV_MC_RATIO_TOTAL as MV_MC_RATIO_TOTAL
    ,IMD.GRID_TO_PDP_EVALUATION_MESSAGE_TOTAL AS ORIGINAL_EVALUATION_MESSAGE
    ,IMD.GRID_TO_PDP_POWER_TOTAL as PROB_v_GT_C
    ,IMD.GRID_TO_PDP_CALL_TEST_TOTAL AS CALL_TEST
from
    REPORTING_PROD.DATA_SCIENCE.IMAGE_TESTING_IM_DATA IMD
RIGHT JOIN
    MAX_EVALUATION_BATCHES ME
ON IMD.EVALUATION_BATCH_ID=ME.MAX_EVALUATION_BATCH_ID
    AND IMD.MASTER_TEST_NUMBER=ME.MASTER_TEST_NUMBER
order by IMD.MASTER_TEST_NUMBER;

--above first REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS
--and then _DM_IM_SELECTED_IM_DATA
--are created. Now they are combined.

--select count(*) from _DM_IM_SELECTED_IM_DATA
--359 (tests which have been evaluated)
--SELECT count(*) FROM REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS
--436 (some tests not evaluated)

create or replace temp table _REPORTING_BASE_PROD_SHARED_IMAGE_TESTS_WITH_CURRENT_URLS2 AS
SELECT
    ITU.MASTER_TEST_NUMBER
    ,ITU.CONTEXT
    ,ITU.REQUESTED_TEST_ID
    ,ITU.MASTER_PRODUCT_ID
    ,ITU.PRODUCT_SKU
    ,ITU.MASTER_STORE_GROUP_ID
    ,ITU.MEMBERSHIP_BRAND_ID
    ,ITU.SAM_ID_CURRENT
    ,ITU.SAM_ID_TEST
    ,ITU.SORT_CURRENT
    ,ITU.SORT_TEST
    ,ITU.AEM_UUID_CURRENT
    ,ITU.AEM_UUID_TEST
    ,ITU.CONFIG_JSON
    ,ITU.DATETIME_ADDED_SAM_CURRENT
    ,ITU.DATETIME_ADDED_SAM_TEST
    ,ITU.DATETIME_ADDED
    ,ITU.MIN_EVALUATION_BATCH_ID
    ,ITU.MAX_EVALUATION_BATCH_ID
    ,ITU.MIN_DATETIME_ADDED_CONFIG_EXPORT
    ,ITU.MAX_DATETIME_ADDED_CONFIG_EXPORT
    ,ITU.SORT_NOW_CURRENT
    ,ITU.SORT_NOW_TEST
    ,ITU.IMAGE_URL_NOW_CURRENT
    ,ITU.IMAGE_URL_NOW_TEST
    ,ITU.PRODUCT_EFFECTIVE_START_DATE_PST
    ,ITU.PRODUCT_EFFECTIVE_END_DATE_PST
    ,IMD.GRID_VIEWS_CONTROL_TOTAL
	,IMD.GRID_VIEWS_VARIANT_TOTAL
    ,IMD.MV_MC_RATIO_TOTAL
    ,100*(IMD.MV_MC_RATIO_TOTAL-1) as PERCENT_OUTCOME
    ,IMD.ORIGINAL_EVALUATION_MESSAGE
    ,IMD.PROB_V_GT_C
    ,IMD.CALL_TEST
from _REPORTING_BASE_PROD_SHARED_IMAGE_TESTS_WITH_CURRENT_URLS ITU
LEFT JOIN _DM_IM_SELECTED_IM_DATA IMD
    ON ITU.MASTER_TEST_NUMBER=IMD.MASTER_TEST_NUMBER
order by ITU.MASTER_TEST_NUMBER;

create or replace transient table REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS AS
WITH ranked_ce AS (
    SELECT  PRODUCT_IMAGE_TEST_CONFIG_EXPORT_ID,
            MASTER_STORE_GROUP_ID,
            MASTER_PRODUCT_ID,
            PRODUCT_SKU,
            MEMBERSHIP_BRAND_ID,
            EVALUATION_BATCH_ID,
            MASTER_TEST_NUMBER,
            CONFIG_JSON,
            EVALUATION_MESSAGE,
            EXTRA_PARAMS_JSON,
            DATETIME_ADDED,
            DATETIME_MODIFIED,
            ROW_NUMBER() OVER (PARTITION BY MASTER_TEST_NUMBER ORDER BY EVALUATION_BATCH_ID DESC) AS rn
    FROM REPORTING_BASE_PROD.SHARED.PRODUCT_IMAGE_TEST_CONFIG_EXPORT
)
SELECT
    ITU.MASTER_TEST_NUMBER
    ,ITU.CONTEXT
    ,ITU.REQUESTED_TEST_ID
    ,ITU.MASTER_PRODUCT_ID
    ,ITU.PRODUCT_SKU
    ,ITU.MASTER_STORE_GROUP_ID
    ,ITU.MEMBERSHIP_BRAND_ID
    ,ITU.SAM_ID_CURRENT
    ,ITU.SAM_ID_TEST
    ,ITU.SORT_CURRENT
    ,ITU.SORT_TEST
    ,ITU.AEM_UUID_CURRENT
    ,ITU.AEM_UUID_TEST
    ,ITU.CONFIG_JSON
    ,ITU.DATETIME_ADDED_SAM_CURRENT
    ,ITU.DATETIME_ADDED_SAM_TEST
    ,ITU.DATETIME_ADDED
    ,ITU.MIN_EVALUATION_BATCH_ID
    ,ITU.MAX_EVALUATION_BATCH_ID
    ,ITU.MIN_DATETIME_ADDED_CONFIG_EXPORT
    ,ITU.MAX_DATETIME_ADDED_CONFIG_EXPORT
    ,ITU.SORT_NOW_CURRENT
    ,ITU.SORT_NOW_TEST
    ,ITU.IMAGE_URL_NOW_CURRENT
    ,ITU.IMAGE_URL_NOW_TEST
    ,ITU.PRODUCT_EFFECTIVE_START_DATE_PST
    ,ITU.PRODUCT_EFFECTIVE_END_DATE_PST
    ,ITU.GRID_VIEWS_CONTROL_TOTAL
	,ITU.GRID_VIEWS_VARIANT_TOTAL
    ,ITU.MV_MC_RATIO_TOTAL
    ,ITU.PERCENT_OUTCOME
    ,ITU.ORIGINAL_EVALUATION_MESSAGE
    ,ITU.PROB_V_GT_C
    ,ITU.CALL_TEST
	,RCE.EVALUATION_MESSAGE as LATEST_EVALUATION_MESSAGE
from _REPORTING_BASE_PROD_SHARED_IMAGE_TESTS_WITH_CURRENT_URLS2 ITU
left join ranked_ce RCE
on ITU.MASTER_TEST_NUMBER=RCE.MASTER_TEST_NUMBER
where  rn=1
order by master_test_number
;
