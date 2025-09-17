ALTER SESSION SET TIMEZONE='America/Los_Angeles';

SET target_table = 'reporting_base_prod.gsc.vw_inventory_log';
SET execution_start_time = current_timestamp()::TIMESTAMP_LTZ(3);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (
        SELECT
            $target_table AS table_name,
            NULLIF(dependent_table_name,$target_table) AS dependent_table_name,
            high_watermark_datetime AS new_high_watermark_datetime
            FROM (
            SELECT
                'lake_view.ultra_warehouse.inventory_log' AS dependent_table_name,
                MAX(datetime_modified) AS high_watermark_datetime
            FROM lake_view.ultra_warehouse.inventory_log
            UNION ALL
            SELECT
                'lake_view.ultra_warehouse.location' AS dependent_table_name,
                MAX(datetime_modified) AS high_watermark_datetime
            FROM lake_view.ultra_warehouse.location
            UNION ALL
            SELECT
                'lake_view.ultra_warehouse.case' AS dependent_table_name,
                MAX(datetime_modified) AS high_watermark_datetime
            FROM lake_view.ultra_warehouse.case
            UNION ALL
            SELECT
                'lake_view.ultra_warehouse.lpn' AS dependent_table_name,
                MAX(datetime_modified) AS high_watermark_datetime
            FROM lake_view.ultra_warehouse.lpn
            UNION ALL
            SELECT
                'lake_view.ultra_warehouse.item' AS dependent_table_name,
                MAX(datetime_modified) AS high_watermark_datetime
            FROM lake_view.ultra_warehouse.item
            UNION ALL
            SELECT
                'lake_view.ultra_warehouse.administrator' AS dependent_table_name,
                MAX(datetime_modified) AS high_watermark_datetime
            FROM lake_view.ultra_warehouse.administrator
            ) AS h
        ) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
            t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = $execution_start_time
    WHEN NOT MATCHED
    THEN INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime
    );

SET wm_lake_view_ultra_warehouse_inventory_log = public.udf_get_watermark($target_table,'lake_view.ultra_warehouse.inventory_log');
SET wm_lake_view_ultra_warehouse_location = public.udf_get_watermark($target_table,'lake_view.ultra_warehouse.location');
SET wm_lake_view_ultra_warehouse_case = public.udf_get_watermark($target_table,'lake_view.ultra_warehouse.case');
SET wm_lake_view_ultra_warehouse_lpn = public.udf_get_watermark($target_table,'lake_view.ultra_warehouse.lpn');
SET wm_lake_view_ultra_warehouse_item = public.udf_get_watermark($target_table,'lake_view.ultra_warehouse.item');
SET wm_lake_view_ultra_warehouse_administrator = public.udf_get_watermark($target_table,'lake_view.ultra_warehouse.administrator');

CREATE OR REPLACE TEMP TABLE _inventory_log_base AS
SELECT DISTINCT
    inventory_log_id
FROM (
    SELECT inventory_log_id
    FROM lake_view.ultra_warehouse.inventory_log AS il
    WHERE il.HVR_CHANGE_TIME > $wm_lake_view_ultra_warehouse_inventory_log
    UNION ALL
    SELECT inventory_log_id
    FROM lake_view.ultra_warehouse.inventory_log  AS il
    JOIN lake_view.ultra_warehouse.location l
        ON il.location_id = l.location_id
    WHERE l.HVR_CHANGE_TIME > $wm_lake_view_ultra_warehouse_location
    UNION ALL
    SELECT inventory_log_id
    FROM lake_view.ultra_warehouse.inventory_log AS il
    JOIN lake_view.ultra_warehouse.case AS cs
        ON il.case_id = cs.case_id
    WHERE cs.HVR_CHANGE_TIME > $wm_lake_view_ultra_warehouse_case
    UNION ALL
    SELECT inventory_log_id
    FROM lake_view.ultra_warehouse.inventory_log AS il
    JOIN lake_view.ultra_warehouse.lpn AS lp
        ON il.lpn_id = lp.lpn_id
    WHERE lp.HVR_CHANGE_TIME > $wm_lake_view_ultra_warehouse_lpn
    UNION ALL
    SELECT inventory_log_id
    FROM lake_view.ultra_warehouse.inventory_log AS il
    JOIN lake_view.ultra_warehouse.item AS i
        ON il.item_id = i.item_id
    WHERE i.HVR_CHANGE_TIME > $wm_lake_view_ultra_warehouse_item
    UNION ALL
    SELECT inventory_log_id
    FROM lake_view.ultra_warehouse.inventory_log AS il
    JOIN lake_view.ultra_warehouse.administrator AS a
        ON il.administrator_id = a.administrator_id
    WHERE a.HVR_CHANGE_TIME > $wm_lake_view_ultra_warehouse_administrator
    ) AS A
ORDER BY inventory_log_id ASC;

CREATE OR REPLACE TEMPORARY TABLE _inventory_final AS
SELECT
    il.inventory_log_id,
    il.parent_inventory_log_id,
    il.warehouse_id,
    cd1.code_id AS type_code_id,
    cd1.label AS type_code_label,
    l.location_id,
    l.label AS location_label,
    z.zone_id,
    z.label AS zone_label,
    z.type_code_id AS zone_type_code_id,
    z2.zone_id AS pick_zone_id,
    z2.label AS pick_zone_label,
    z2.type_code_id AS pick_zone_type_code_id,
    c.container_id as parent_container_id,
    c.label as parent_container_label,
    cd2.code_id as parent_container_type_code_id,
    cd2.label as parent_container_type_code_label,
    c2.container_id as container_id,
    c2.label as container_label,
    cd3.code_id as container_type_code_id,
    cd3.label as container_type_code_label,
    cs.case_id,
    cs.case_code,
    lp.lpn_id,
    lp.lpn_code,
    i.item_id,
    i.item_number,
    i.description AS item_description,
    i.wms_class,
    il.quantity,
    il.object,
    il.object_id,
    il.datetime_added,
    il.datetime_modified,
    a.administrator_id,
    a.firstname AS administrator_firstname,
    a.lastname AS administrator_lastname
FROM lake_view.ultra_warehouse.inventory_log AS il
JOIN _inventory_log_base AS ib
    ON ib.inventory_log_id = il.inventory_log_id
JOIN lake_view.ultra_warehouse.location AS l
    ON il.location_id = l.location_id
JOIN lake_view.ultra_warehouse.zone AS z
    ON l.zone_id = z.zone_id
LEFT JOIN lake_view.ultra_warehouse.zone AS z2
    ON l.pick_zone_id = z2.zone_id
LEFT JOIN lake_view.ultra_warehouse.container AS c
    ON il.parent_container_id = c.container_id
LEFT JOIN lake_view.ultra_warehouse.container AS c2
    ON il.container_id = c2.container_id
JOIN lake_view.ultra_warehouse.code AS cd1
    ON il.type_code_id = cd1.code_id
LEFT JOIN LAKE_VIEW.ultra_warehouse.CODE AS cd2
    ON c.type_code_id = cd2.code_id
LEFT JOIN LAKE_VIEW.ultra_warehouse.CODE AS cd3
    ON c2.type_code_id = cd3.code_id
LEFT JOIN lake_view.ultra_warehouse.case AS cs
    ON il.case_id = cs.case_id
LEFT JOIN lake_view.ultra_warehouse.lpn AS lp
    ON il.lpn_id = lp.lpn_id
LEFT JOIN lake_view.ultra_warehouse.item AS i
    ON il.item_id = i.item_id
LEFT JOIN lake_view.ultra_warehouse.administrator
    AS a ON il.administrator_id = a.administrator_id;

MERGE INTO REPORTING_BASE_PROD.GSC.VW_INVENTORY_LOG t
USING (
       SELECT
            INVENTORY_LOG_ID,
            PARENT_INVENTORY_LOG_ID,
            WAREHOUSE_ID,
            TYPE_CODE_ID,
            TYPE_CODE_LABEL,
            LOCATION_ID,
            LOCATION_LABEL,
            ZONE_ID,
            ZONE_LABEL,
            ZONE_TYPE_CODE_ID,
            PICK_ZONE_ID,
            PICK_ZONE_LABEL,
            PICK_ZONE_TYPE_CODE_ID,
            PARENT_CONTAINER_ID,
            PARENT_CONTAINER_LABEL,
            PARENT_CONTAINER_TYPE_CODE_ID,
            PARENT_CONTAINER_TYPE_CODE_LABEL,
            CONTAINER_ID,
            CONTAINER_LABEL,
            CONTAINER_TYPE_CODE_ID,
            CONTAINER_TYPE_CODE_LABEL,
            CASE_ID,
            CASE_CODE,
            LPN_ID,
            LPN_CODE,
            ITEM_ID,
            ITEM_NUMBER,
            ITEM_DESCRIPTION,
            WMS_CLASS,
            QUANTITY,
            OBJECT,
            OBJECT_ID,
            DATETIME_ADDED,
            DATETIME_MODIFIED,
            ADMINISTRATOR_ID,
            ADMINISTRATOR_FIRSTNAME,
            ADMINISTRATOR_LASTNAME
        FROM _inventory_final
    ) s
    ON equal_null(t.INVENTORY_LOG_ID, s.INVENTORY_LOG_ID)
WHEN NOT MATCHED THEN INSERT (
    INVENTORY_LOG_ID,
    PARENT_INVENTORY_LOG_ID,
    WAREHOUSE_ID,
    TYPE_CODE_ID,
    TYPE_CODE_LABEL,
    LOCATION_ID,
    LOCATION_LABEL,
    ZONE_ID,
    ZONE_LABEL,
    ZONE_TYPE_CODE_ID,
    PICK_ZONE_ID,
    PICK_ZONE_LABEL,
    PICK_ZONE_TYPE_CODE_ID,
    PARENT_CONTAINER_ID,
    PARENT_CONTAINER_LABEL,
    PARENT_CONTAINER_TYPE_CODE_ID,
    PARENT_CONTAINER_TYPE_CODE_LABEL,
    CONTAINER_ID,
    CONTAINER_LABEL,
    CONTAINER_TYPE_CODE_ID,
    CONTAINER_TYPE_CODE_LABEL,
    CASE_ID,
    CASE_CODE,
    LPN_ID,
    LPN_CODE,
    ITEM_ID,
    ITEM_NUMBER,
    ITEM_DESCRIPTION,
    WMS_CLASS,
    QUANTITY,
    OBJECT,
    OBJECT_ID,
    DATETIME_ADDED,
    DATETIME_MODIFIED,
    ADMINISTRATOR_ID,
    ADMINISTRATOR_FIRSTNAME,
    ADMINISTRATOR_LASTNAME
)
VALUES (
    INVENTORY_LOG_ID,
    PARENT_INVENTORY_LOG_ID,
    WAREHOUSE_ID,
    TYPE_CODE_ID,
    TYPE_CODE_LABEL,
    LOCATION_ID,
    LOCATION_LABEL,
    ZONE_ID,
    ZONE_LABEL,
    ZONE_TYPE_CODE_ID,
    PICK_ZONE_ID,
    PICK_ZONE_LABEL,
    PICK_ZONE_TYPE_CODE_ID,
    PARENT_CONTAINER_ID,
    PARENT_CONTAINER_LABEL,
    PARENT_CONTAINER_TYPE_CODE_ID,
    PARENT_CONTAINER_TYPE_CODE_LABEL,
    CONTAINER_ID,
    CONTAINER_LABEL,
    CONTAINER_TYPE_CODE_ID,
    CONTAINER_TYPE_CODE_LABEL,
    CASE_ID,
    CASE_CODE,
    LPN_ID,
    LPN_CODE,
    ITEM_ID,
    ITEM_NUMBER,
    ITEM_DESCRIPTION,
    WMS_CLASS,
    QUANTITY,
    OBJECT,
    OBJECT_ID,
    DATETIME_ADDED,
    DATETIME_MODIFIED,
    ADMINISTRATOR_ID,
    ADMINISTRATOR_FIRSTNAME,
    ADMINISTRATOR_LASTNAME
)
WHEN MATCHED AND (
        COALESCE(t.PARENT_INVENTORY_LOG_ID, -1) <> COALESCE(s.PARENT_INVENTORY_LOG_ID, -1)
        OR COALESCE(t.WAREHOUSE_ID, -1) <> COALESCE(s.WAREHOUSE_ID,-1)
        OR COALESCE(t.TYPE_CODE_ID, -1) <> COALESCE(s.TYPE_CODE_ID, -1)
        OR COALESCE(t.TYPE_CODE_LABEL, '') <> COALESCE(s.TYPE_CODE_LABEL, '')
        OR COALESCE(t.LOCATION_ID, -1) <> COALESCE(s.LOCATION_ID, -1)
        OR COALESCE(t.LOCATION_LABEL, '') <> COALESCE(s.LOCATION_LABEL, '')
        OR COALESCE(t.ZONE_ID, -1) <> COALESCE(s.ZONE_ID, -1)
        OR COALESCE(t.ZONE_LABEL, '') <> COALESCE(s.ZONE_LABEL, '')
        OR COALESCE(t.ZONE_TYPE_CODE_ID, -1) <> COALESCE(s.ZONE_TYPE_CODE_ID, -1)
        OR COALESCE(t.PICK_ZONE_ID, -1) <> COALESCE(s.PICK_ZONE_ID, -1)
        OR COALESCE(t.PICK_ZONE_LABEL, '') <> COALESCE(s.PICK_ZONE_LABEL, '')
        OR COALESCE(t.PICK_ZONE_TYPE_CODE_ID, -1) <> COALESCE(s.PICK_ZONE_TYPE_CODE_ID, -1)
        OR COALESCE(t.PARENT_CONTAINER_ID, -1) <> COALESCE(s.PARENT_CONTAINER_ID, -1)
        OR COALESCE(t.PARENT_CONTAINER_LABEL, '') <> COALESCE(s.PARENT_CONTAINER_LABEL, '')
        OR COALESCE(t.PARENT_CONTAINER_TYPE_CODE_ID, -1) <> COALESCE(s.PARENT_CONTAINER_TYPE_CODE_ID, -1)
        OR COALESCE(t.PARENT_CONTAINER_TYPE_CODE_LABEL, '') <> COALESCE(s.PARENT_CONTAINER_TYPE_CODE_LABEL, '')
        OR COALESCE(t.CONTAINER_ID, -1) <> COALESCE(s.CONTAINER_ID, -1)
        OR COALESCE(t.CONTAINER_LABEL, '') <> COALESCE(s.CONTAINER_LABEL, '')
        OR COALESCE(t.CONTAINER_TYPE_CODE_ID, -1) <> COALESCE(s.CONTAINER_TYPE_CODE_ID, -1)
        OR COALESCE(t.CONTAINER_TYPE_CODE_LABEL, '') <> COALESCE(s.CONTAINER_TYPE_CODE_LABEL, '')
        OR COALESCE(t.CASE_ID, -1) <> COALESCE(s.CASE_ID, -1)
        OR COALESCE(t.CASE_CODE, '') <> COALESCE(s.CASE_CODE, '')
        OR COALESCE(t.LPN_ID, -1) <> COALESCE(s.LPN_ID, -1)
        OR COALESCE(t.LPN_CODE, '') <> COALESCE(s.LPN_CODE, '')
        OR COALESCE(t.ITEM_ID, -1) <> COALESCE(s.ITEM_ID, -1)
        OR COALESCE(t.ITEM_NUMBER, '') <> COALESCE(s.ITEM_NUMBER, '')
        OR COALESCE(t.ITEM_DESCRIPTION, '') <> COALESCE(s.ITEM_DESCRIPTION, '')
        OR COALESCE(t.WMS_CLASS, '') <> COALESCE(s.WMS_CLASS, '')
        OR COALESCE(t.QUANTITY, -1) <> COALESCE(s.QUANTITY, -1)
        OR COALESCE(t.OBJECT, '') <> COALESCE(s.OBJECT, '')
        OR COALESCE(t.OBJECT_ID, -1) <> COALESCE(s.OBJECT_ID, -1)
        OR COALESCE(t.DATETIME_ADDED, '1970-01-01') <> COALESCE(s.DATETIME_ADDED, '1970-01-01')
        OR COALESCE(t.DATETIME_MODIFIED, '1970-01-01') <> COALESCE(s.DATETIME_MODIFIED, '1970-01-01')
        OR COALESCE(t.ADMINISTRATOR_ID, -1) <> COALESCE(s.ADMINISTRATOR_ID, -1)
        OR COALESCE(t.ADMINISTRATOR_FIRSTNAME, '') <> COALESCE(s.ADMINISTRATOR_FIRSTNAME, '')
        OR COALESCE(t.ADMINISTRATOR_LASTNAME, '') <> COALESCE(s.ADMINISTRATOR_LASTNAME, '')
    )
    THEN UPDATE SET
        t.INVENTORY_LOG_ID = s.INVENTORY_LOG_ID,
        t.PARENT_INVENTORY_LOG_ID = s.PARENT_INVENTORY_LOG_ID,
        t.WAREHOUSE_ID = s.WAREHOUSE_ID,
        t.TYPE_CODE_ID = s.TYPE_CODE_ID,
        t.TYPE_CODE_LABEL = s.TYPE_CODE_LABEL,
        t.LOCATION_ID = s.LOCATION_ID,
        t.LOCATION_LABEL = s.LOCATION_LABEL,
        t.ZONE_ID = s.ZONE_ID,
        t.ZONE_LABEL = s.ZONE_LABEL,
        t.ZONE_TYPE_CODE_ID = s.ZONE_TYPE_CODE_ID,
        t.PICK_ZONE_ID = s.PICK_ZONE_ID,
        t.PICK_ZONE_LABEL = s.PICK_ZONE_LABEL,
        t.PICK_ZONE_TYPE_CODE_ID = s.PICK_ZONE_TYPE_CODE_ID,
        t.PARENT_CONTAINER_ID = s.PARENT_CONTAINER_ID,
        t.PARENT_CONTAINER_LABEL = s.PARENT_CONTAINER_LABEL,
        t.PARENT_CONTAINER_TYPE_CODE_ID = s.PARENT_CONTAINER_TYPE_CODE_ID,
        t.PARENT_CONTAINER_TYPE_CODE_LABEL = s.PARENT_CONTAINER_TYPE_CODE_LABEL,
        t.CONTAINER_ID = s.CONTAINER_ID,
        t.CONTAINER_LABEL = s.CONTAINER_LABEL,
        t.CONTAINER_TYPE_CODE_ID = s.CONTAINER_TYPE_CODE_ID,
        t.CONTAINER_TYPE_CODE_LABEL = s.CONTAINER_TYPE_CODE_LABEL,
        t.CASE_ID = s.CASE_ID,
        t.CASE_CODE = s.CASE_CODE,
        t.LPN_ID = s.LPN_ID,
        t.LPN_CODE = s.LPN_CODE,
        t.ITEM_ID = s.ITEM_ID,
        t.ITEM_NUMBER = s.ITEM_NUMBER,
        t.ITEM_DESCRIPTION = s.ITEM_DESCRIPTION,
        t.WMS_CLASS = s.WMS_CLASS,
        t.QUANTITY = s.QUANTITY,
        t.OBJECT = s.OBJECT,
        t.OBJECT_ID = s.OBJECT_ID,
        t.DATETIME_ADDED = s.DATETIME_ADDED,
        t.DATETIME_MODIFIED = s.DATETIME_MODIFIED,
        t.ADMINISTRATOR_ID = s.ADMINISTRATOR_ID,
        t.ADMINISTRATOR_FIRSTNAME = s.ADMINISTRATOR_FIRSTNAME,
        t.ADMINISTRATOR_LASTNAME = s.ADMINISTRATOR_LASTNAME,
        t.meta_update_datetime = $execution_start_time;

UPDATE public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = $execution_start_time
WHERE table_name = $target_table;
