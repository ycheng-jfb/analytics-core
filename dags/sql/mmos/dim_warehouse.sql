truncate table EDW_PROD.NEW_STG.dim_warehouse;

insert into EDW_PROD.NEW_STG.dim_warehouse
SELECT
    t1.id AS warehouse_id,
    logicWarehouseCode AS warehouse_code,
    warehouseName AS warehouse_name,
    warehouseType AS warehouse_type_org,
    t2.warehouse_class AS warehouse_type,
    1 as is_active,
    JSON_EXTRACT_PATH_TEXT(address, 'addressLine1') AS address,
    JSON_EXTRACT_PATH_TEXT(address, 'districtOrCounty') AS address2,
    JSON_EXTRACT_PATH_TEXT(address, 'city') AS city,
    JSON_EXTRACT_PATH_TEXT(address, 'state') AS state,
    JSON_EXTRACT_PATH_TEXT(address, 'zip_code') AS zip_code,
    JSON_EXTRACT_PATH_TEXT(address, 'country') AS country_code,
    JSON_EXTRACT_PATH_TEXT(address, 'phone') AS phone,
    JSON_EXTRACT_PATH_TEXT(address, 'coast') AS coast,
    areaType AS region,
    false AS is_retail,
    false AS is_consignment,
    false AS is_wholesale,
    null AS carrier_service,
    timeZone AS time_zone
FROM
    lake.mmt.ods_ims_t_warehouse_df t1
    LEFT JOIN (
        SELECT warehouse_name, max(warehouse_class) warehouse_class
        FROM LAKE.MMT.ods_data_filling_warehouse_class
        GROUP BY warehouse_name
    ) t2 ON t1.warehouseName = t2.warehouse_name
WHERE t1.delFlag = 0 and t1.PT_DAY = current_date;




