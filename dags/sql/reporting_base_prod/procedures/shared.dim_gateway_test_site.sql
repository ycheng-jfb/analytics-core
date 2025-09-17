CREATE TABLE IF NOT EXISTS shared.dim_gateway_test_site(
    DM_GATEWAY_TEST_SITE_ID NUMBER(38,0),
    DM_GATEWAY_TEST_ID NUMBER(38,0),
    DM_SITE_ID NUMBER(38,0),
    GATEWAY_TEST_NAME VARCHAR,
    GATEWAY_TEST_ID NUMBER(38,0),
    GATEWAY_TEST_START TIMESTAMP_NTZ(3),
    GATEWAY_TEST_END TIMESTAMP_NTZ(3),
    GATEWAY_TEST_DESCRIPTION VARCHAR,
    GATEWAY_TEST_HYPOTHESIS VARCHAR,
    GATEWAY_TEST_RESULTS VARCHAR,
    GATEWAY_TEST_LP_TRAFFIC_SPLIT NUMBER(38,0),
    GATEWAY_TEST_LP_TYPE VARCHAR,
    GATEWAY_TEST_LP_CLASS VARCHAR,
    GATEWAY_TEST_LP_LOCATION VARCHAR,
    EFFECTIVE_START_DATETIME TIMESTAMP_NTZ(3),
    EFFECTIVE_END_DATETIME TIMESTAMP_NTZ(3),
    META_ORIGINAL_DM_GATEWAY_TEST_SITE_ID  NUMBER(38,0),
    META_ROW_HASH NUMBER(38,0),
	META_CREATE_DATETIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP,
	META_UPDATE_DATETIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TEMPORARY TABLE _gateway_test_site_hist AS
SELECT
    dm_gateway_test_site_id,
    dm_gateway_test_id,
    dm_site_id,
    weight,
    event_datetime AS effective_start_datetime,
    lead(event_datetime, 1, '2999-12-31') OVER (PARTITION BY dm_gateway_test_site_id ORDER BY event_datetime) AS effective_end_datetime,
    meta_original_dm_gateway_test_site_id
FROM (
    SELECT
        dm_gateway_test_site_id,
        dm_gateway_test_id,
        dm_site_id,
        weight,
        hash(dm_gateway_test_site_id,dm_gateway_test_id, dm_site_id,weight) AS row_hash,
        datetime_modified AS event_datetime,
        lag(row_hash) OVER (PARTITION BY DM_GATEWAY_TEST_SITE_ID ORDER BY event_datetime) AS prev_row_hash,
        meta_original_dm_gateway_test_site_id
    FROM  lake_consolidated.ultra_merchant_history.DM_GATEWAY_TEST_SITE s
) a
WHERE
    row_hash IS DISTINCT FROM prev_row_hash;

CREATE OR REPLACE TEMPORARY TABLE _dim_gateway_test_site AS
SELECT
    g.dm_gateway_test_site_id,
    g.dm_gateway_test_id,
    g.dm_site_id,
    dmg.label AS gateway_test_name,
    dmg.dm_gateway_test_id AS gateway_test_id,
    dmg.datetime_start AS gateway_test_start,
    dmg.datetime_end AS gateway_test_end,
    dmg.description AS gateway_test_description,
    dmg.hypothesis AS gateway_test_hypothesis,
    dmg.results AS gateway_test_results,
    g.weight AS gateway_test_lp_traffic_split,
    tst.label AS gateway_test_lp_type,
    tstc.label AS gateway_test_lp_class,
    tsl.label AS gateway_test_lp_location,
    g.effective_start_datetime,
    g.effective_end_datetime,
    g.meta_original_dm_gateway_test_site_id,
    HASH(g.dm_gateway_test_site_id, g.dm_gateway_test_id, g.dm_site_id, gateway_test_name,gateway_test_id, gateway_test_start, gateway_test_end,
    gateway_test_description, gateway_test_hypothesis, gateway_test_results, gateway_test_lp_traffic_split, gateway_test_lp_type, gateway_test_lp_class,
    gateway_test_lp_location, effective_start_datetime, effective_end_datetime, g.meta_original_dm_gateway_test_site_id ) AS meta_row_hash
FROM _gateway_test_site_hist g
JOIN lake_consolidated_view.ultra_merchant.dm_gateway_test_site dms
    ON dms.dm_gateway_test_site_id = g.dm_gateway_test_site_id
JOIN lake_consolidated_view.ultra_merchant.dm_gateway_test_site_location tsl
    ON tsl.dm_gateway_test_site_location_id = dms.dm_gateway_test_site_location_id
JOIN lake_consolidated_view.ultra_merchant.dm_gateway_test dmg
    ON dmg.dm_gateway_test_id = dms.dm_gateway_test_id
LEFT JOIN lake_consolidated_view.ultra_merchant.dm_gateway_test_site_type tst
    ON tst.dm_gateway_test_site_type_id = dms.dm_gateway_test_site_type_id
LEFT JOIN lake_consolidated_view.ultra_merchant.dm_gateway_test_site_class tstc
    ON tstc.dm_gateway_test_site_class_id = dms.dm_gateway_test_site_class_id;

TRUNCATE TABLE shared.dim_gateway_test_site;

INSERT INTO shared.dim_gateway_test_site
    (dm_gateway_test_site_id, dm_gateway_test_id, dm_site_id, gateway_test_name,gateway_test_id, gateway_test_start, gateway_test_end,
    gateway_test_description, gateway_test_hypothesis, gateway_test_results, gateway_test_lp_traffic_split, gateway_test_lp_type, gateway_test_lp_class,
    gateway_test_lp_location, effective_start_datetime, effective_end_datetime, meta_original_dm_gateway_test_site_id, meta_row_hash)
SELECT
    dm_gateway_test_site_id,
    dm_gateway_test_id,
    dm_site_id,
    gateway_test_name,gateway_test_id,
    gateway_test_start,
    gateway_test_end,
    gateway_test_description,
    gateway_test_hypothesis,
    gateway_test_results,
    gateway_test_lp_traffic_split,
    gateway_test_lp_type,
    gateway_test_lp_class,
    gateway_test_lp_location,
    effective_start_datetime,
    effective_end_datetime,
    meta_original_dm_gateway_test_site_id,
    meta_row_hash
FROM _dim_gateway_test_site;
