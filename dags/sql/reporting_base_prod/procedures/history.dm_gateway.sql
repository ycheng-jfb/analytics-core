CREATE TABLE IF NOT EXISTS history.dm_gateway (
    dm_gateway_id               NUMBER(38,0),
    gateway_type                VARCHAR,
    gateway_sub_type            VARCHAR,
    gateway_name                VARCHAR,
    gateway_code                VARCHAR,
    gateway_statuscode          NUMBER(38,0),
    gateway_status              VARCHAR,
    redirect_dm_gateway_id      NUMBER(38,0),
    store_id                    NUMBER(38,0),
    store_brand_name            VARCHAR,
    store_country               VARCHAR,
    store_region                VARCHAR,
    tracking_type               VARCHAR,
    effective_start_datetime    TIMESTAMP_NTZ(3),
    effective_end_datetime      TIMESTAMP_NTZ(3),
    meta_is_current             BOOLEAN,
    meta_original_dm_gateway_id NUMBER(38,0),
    meta_create_datetime        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TEMPORARY TABLE _new_gateway_history_tracking AS
SELECT
    gth.dm_gateway_id,
    gt.label AS gateway_type,
    COALESCE(gst.label, 'None') AS gateway_sub_type,
    gth.label as gateway_name,
    gth.code as gateway_code,
    gth.statuscode AS gateway_statuscode,
    sc.label AS gateway_status,
    gth.redirect_dm_gateway_id,
    gth.store_id,
    st.store_brand,
    st.store_country,
    st.store_region,
    gth.effective_start_datetime,
    gth.effective_end_datetime,
    'SNOWFLAKE' AS tracking_type,
    gth.meta_original_dm_gateway_id,
    gth.meta_original_redirect_dm_gateway_id
FROM lake_consolidated.ultra_merchant_history.dm_gateway gth
LEFT JOIN edw_prod.data_model.dim_store st ON st.store_id = gth.store_id
LEFT JOIN lake_consolidated.ultra_merchant_history.dm_gateway_type gt
    ON gt.dm_gateway_type_id = gth.dm_gateway_type_id
    AND gt.effective_start_datetime < gth.effective_start_datetime
    AND gt.effective_end_datetime >= gth.effective_start_datetime
LEFT JOIN lake_consolidated.ultra_merchant_history.dm_gateway_sub_type gst
    ON gst.dm_gateway_sub_type_id = gth.dm_gateway_sub_type_id
    AND gst.effective_start_datetime < gth.effective_start_datetime
    AND gst.effective_end_datetime >= gth.effective_start_datetime
LEFT JOIN lake_consolidated_view.ultra_merchant.statuscode sc
    ON sc.statuscode = gth.statuscode;

CREATE OR REPLACE TEMPORARY TABLE _combine_new_and_historical AS
SELECT *
FROM _new_gateway_history_tracking
UNION
SELECT *
FROM reporting_prod.shared.historical_gateway_history_tracking
where dm_gateway_id not in (1301920,1302020,1302120)
and effective_start_datetime < '2020-01-13 04:41:43.657';  --this is when Snowflake tracking began <—new


CREATE OR REPLACE TEMPORARY TABLE _update_effective_to_datetime AS
SELECT *,
    LAG(tracking_type) OVER (PARTITION BY dm_gateway_id ORDER BY effective_end_datetime) lag_tracking_type,
    LAG(effective_end_datetime) OVER (PARTITION BY dm_gateway_id ORDER BY effective_end_datetime) AS last_eff_to
FROM _combine_new_and_historical;

CREATE OR REPLACE TEMPORARY TABLE _in_both AS
SELECT
    DISTINCT c1.dm_gateway_id
FROM _update_effective_to_datetime c1
JOIN _update_effective_to_datetime c2 on c1.dm_gateway_id = c2.dm_gateway_id
    AND c2.tracking_type = 'SNOWFLAKE'
WHERE c1.tracking_type = 'EDW01';

UPDATE _update_effective_to_datetime u
    SET effective_start_datetime = IFF(lag_tracking_type='EDW01', dateadd(s,1,u.last_eff_to), dateadd(ms,1,u.last_eff_to))
FROM _in_both b
WHERE u.last_eff_to IS NOT NULL
AND b.dm_gateway_id = u.dm_gateway_id
AND effective_start_datetime != IFF(lag_tracking_type='EDW01', dateadd(s,1,u.last_eff_to), dateadd(ms,1,u.last_eff_to));


CREATE OR REPLACE TEMPORARY TABLE _meta_is_current_update AS
SELECT *,
    lead(dateadd(ms,-1,effective_start_datetime), 1, '9999-12-31') OVER (PARTITION BY dm_gateway_id ORDER BY effective_start_datetime) AS effective_end_datetime_final,
    row_number() OVER (PARTITION BY dm_gateway_id ORDER BY effective_start_datetime DESC) AS RN
FROM _update_effective_to_datetime
where (effective_start_datetime <> '9999-12-31 00:00:00.001000000 -08:00' and effective_end_datetime <> '9999-12-31 00:00:00.001000000 -08:00');

TRUNCATE TABLE history.dm_gateway;

INSERT INTO history.dm_gateway(
    dm_gateway_id,
    gateway_type,
    gateway_sub_type,
    gateway_name,
    gateway_code,
    gateway_statuscode,
    gateway_status,
    redirect_dm_gateway_id,
    store_id,
    store_brand_name,
    store_country,
    store_region,
    tracking_type,
    effective_start_datetime,
    effective_end_datetime,
    meta_is_current,
    meta_original_dm_gateway_id,
    meta_original_redirect_dm_gateway_id
)
SELECT
    dm_gateway_id,
    gateway_type,
    gateway_sub_type,
    gateway_name,
    gateway_code,
    gateway_statuscode,
    gateway_status,
    redirect_dm_gateway_id,
    store_id,
    store_brand,
    store_country,
    store_region,
    tracking_type,
    effective_start_datetime,
    effective_end_datetime_final,
    iff(rn = 1, 1,0) AS meta_is_current,
    meta_original_dm_gateway_id,
    meta_original_redirect_dm_gateway_id
FROM _meta_is_current_update;

UPDATE history.dm_gateway
SET gateway_status = 'Unknown'
WHERE tracking_type = 'EDW01'
AND meta_is_current != 1;
