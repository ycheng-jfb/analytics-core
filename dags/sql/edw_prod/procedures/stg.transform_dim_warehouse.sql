SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_warehouse';

SET wm_lake_ultra_warehouse_warehouse = (SELECT stg.udf_get_watermark($target_table,'lake.ultra_warehouse.warehouse'));
SET wm_lake_ultra_warehouse_code = (SELECT stg.udf_get_watermark($target_table,'lake.ultra_warehouse.code'));
SET wm_lake_ultra_warehouse_address = (SELECT stg.udf_get_watermark($target_table,'lake.ultra_warehouse.address'));
SET wm_lake_ultra_warehouse_carrier_service = (SELECT stg.udf_get_watermark($target_table,'lake.ultra_warehouse.carrier_service'));

--SET low_watermark_ltz = '2020-08-01';

CREATE OR REPLACE TEMP TABLE _dim_warehouse_base (warehouse_id NUMBER(38,0));

INSERT INTO _dim_warehouse_base (warehouse_id)
SELECT DISTINCT warehouse_id
FROM (
    -- new rows
    SELECT warehouse_id
    FROM lake.ultra_warehouse.warehouse
    WHERE hvr_change_time > $wm_lake_ultra_warehouse_warehouse --) AS w
    UNION
    -- rows from dependent tables
    SELECT w.warehouse_id
    FROM lake.ultra_warehouse.warehouse AS w
        JOIN lake.ultra_warehouse.code AS c
	    	ON c.code_id IN (w.type_code_id, w.wms_type_code_id)
    WHERE c.hvr_change_time > $wm_lake_ultra_warehouse_code
    UNION
    SELECT w.warehouse_id
    FROM lake.ultra_warehouse.warehouse AS w
        JOIN lake.ultra_warehouse.address AS a
            ON a.address_id = w.address_id
    WHERE a.hvr_change_time > $wm_lake_ultra_warehouse_address
    UNION
    SELECT w.warehouse_id
    FROM lake.ultra_warehouse.warehouse AS w
        JOIN lake.ultra_warehouse.carrier_service AS cs
            ON cs.carrier_service_id = w.carrier_service_id
    WHERE cs.hvr_change_time > $wm_lake_ultra_warehouse_carrier_service
    UNION
    -- previously errored rows
    SELECT warehouse_id
    FROM excp.dim_warehouse
    WHERE meta_is_current_excp = 1
        AND meta_data_quality = 'error'
) AS w;

INSERT INTO stg.dim_warehouse_stg (
	warehouse_id,
    warehouse_code,
    warehouse_name,
    warehouse_type,
    wms_type,
    is_active,
    address1,
    address2,
    city,
    state,
    zip_code,
    country_code,
    phone,
    coast,
    region,
    is_retail,
    is_consignment,
    is_wholesale,
    carrier_service,
    time_zone,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    base.warehouse_id,
    w.code AS warehouse_code,
    w.name AS warehouse_name,
    c1.label AS warehouse_type,
    c2.label AS wms_type,
    IFF(w.status_code_id = 4, TRUE, FALSE) AS is_active,
    a.address1,
    a.address2,
    a.city,
    a.state,
    a.zip AS zip_code,
    a.country_code,
    a.phone,
    CASE w.region_id
        WHEN 1 THEN 'EC'
        WHEN 3 THEN 'WC'
        ELSE unk.coast END AS coast,
    CASE
        WHEN w.region_id IN (1, 3, 5) THEN 'NA'
        WHEN w.region_id = 7 THEN 'EU'
        ELSE unk.region END AS region,
    IFF(w.region_id = 8, TRUE, FALSE) AS is_retail,
    IFF(w.region_id = 9, TRUE, FALSE) AS is_consignment,
    IFF(base.warehouse_id = 568, TRUE, FALSE) AS is_wholesale,
    cs.label AS carrier_service,
    w.timezone AS time_zone,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_warehouse_base AS base
    LEFT JOIN stg.dim_warehouse AS unk
        ON unk.warehouse_id = -1
    LEFT JOIN lake.ultra_warehouse.warehouse AS w
		ON w.warehouse_id = base.warehouse_id
    LEFT JOIN lake.ultra_warehouse.code AS c1
	    ON c1.code_id = w.type_code_id
	LEFT JOIN lake.ultra_warehouse.code AS c2
	    ON c2.code_id = w.wms_type_code_id
	LEFT JOIN lake.ultra_warehouse.address AS a
		ON a.address_id = w.address_id
    LEFT JOIN lake.ultra_warehouse.carrier_service AS cs
        ON cs.carrier_service_id = w.carrier_service_id;
