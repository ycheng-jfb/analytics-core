CREATE TABLE IF NOT EXISTS lake_stg.excel.fk_merch_outfit_attributes_stg (
    product_id STRING,
    outfit_alias STRING,
    site_name STRING,
    vip_retail STRING,
    type STRING,
    gender STRING,
    active_inactive STRING,
    outfit_vs_box_vs_pack STRING,
    number_of_components STRING,
    item1 STRING,
    item2 STRING,
    item3 STRING,
    item4 STRING,
    item5 STRING,
    item6 STRING,
    item7 STRING,
    item8 STRING,
    total_cost_of_outfit STRING,
    total_cost_of_outfit_tariff STRING,
    outfit_imu_percent STRING,
    outfit_imu_percent_tariff STRING,
    contains_shoes STRING
);


CREATE TABLE IF NOT EXISTS lake.excel.fk_merch_outfit_attributes (
    product_id NUMBER(38,0),
    outfit_alias STRING,
    site_name STRING,
    vip_retail NUMBER(19,2),
    type STRING,
    gender STRING,
    active_inactive STRING,
    outfit_vs_box_vs_pack STRING,
    number_of_components NUMBER(38,0),
    item1 STRING,
    item2 STRING,
    item3 STRING,
    item4 STRING,
    item5 STRING,
    item6 STRING,
    item7 STRING,
    item8 STRING,
    total_cost_of_outfit NUMBER(38,2),
    total_cost_of_outfit_tariff NUMBER(38,2),
    outfit_imu_percent NUMBER(38,2),
    outfit_imu_percent_tariff NUMBER(38,2),
    contains_shoes STRING,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    PRIMARY KEY (product_id, outfit_alias, site_name, vip_retail, type, gender, active_inactive, outfit_vs_box_vs_pack, number_of_components, item1, item2, item3, item4, item5, item6, item7, item8, total_cost_of_outfit, total_cost_of_outfit_tariff, outfit_imu_percent, outfit_imu_percent_tariff, contains_shoes)
);

DELETE FROM lake_stg.excel.fk_merch_outfit_attributes_stg;

COPY INTO lake_stg.excel.fk_merch_outfit_attributes_stg (product_id, outfit_alias, site_name, vip_retail, type, gender, active_inactive, outfit_vs_box_vs_pack, number_of_components, item1, item2, item3, item4, item5, item6, item7, item8, total_cost_of_outfit, total_cost_of_outfit_tariff, outfit_imu_percent, outfit_imu_percent_tariff, contains_shoes)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fk_merch_outfit_attributes/v2/Outfit Master.csv.gz'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%';

MERGE INTO lake.excel.fk_merch_outfit_attributes t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY product_id, outfit_alias, site_name, vip_retail, type, gender, active_inactive, outfit_vs_box_vs_pack, number_of_components, item1, item2, item3, item4, item5, item6, item7, item8, total_cost_of_outfit, total_cost_of_outfit_tariff, outfit_imu_percent, outfit_imu_percent_tariff, contains_shoes ORDER BY ( SELECT NULL ) ) AS rn
        FROM lake_stg.excel.fk_merch_outfit_attributes_stg
        WHERE COALESCE(product_id,'')!=''
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.product_id, s.product_id)
    AND equal_null(t.outfit_alias, s.outfit_alias)
    AND equal_null(t.site_name, s.site_name)
    AND equal_null(t.vip_retail, s.vip_retail)
    AND equal_null(t.type, s.type)
    AND equal_null(t.gender, s.gender)
    AND equal_null(t.active_inactive, s.active_inactive)
    AND equal_null(t.outfit_vs_box_vs_pack, s.outfit_vs_box_vs_pack)
    AND equal_null(t.number_of_components, s.number_of_components)
    AND equal_null(t.item1, s.item1)
    AND equal_null(t.item2, s.item2)
    AND equal_null(t.item3, s.item3)
    AND equal_null(t.item4, s.item4)
    AND equal_null(t.item5, s.item5)
    AND equal_null(t.item6, s.item6)
    AND equal_null(t.item7, s.item7)
    AND equal_null(t.item8, s.item8)
    AND equal_null(t.total_cost_of_outfit, s.total_cost_of_outfit)
    AND equal_null(t.total_cost_of_outfit_tariff, s.total_cost_of_outfit_tariff)
    AND equal_null(t.outfit_imu_percent, s.outfit_imu_percent)
    AND equal_null(t.outfit_imu_percent_tariff, s.outfit_imu_percent_tariff)
    AND equal_null(t.contains_shoes, s.contains_shoes)
WHEN NOT MATCHED THEN INSERT (
    product_id, outfit_alias, site_name, vip_retail, type, gender, active_inactive, outfit_vs_box_vs_pack, number_of_components, item1, item2, item3, item4, item5, item6, item7, item8, total_cost_of_outfit, total_cost_of_outfit_tariff, outfit_imu_percent, outfit_imu_percent_tariff, contains_shoes, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    product_id, outfit_alias, site_name, vip_retail, type, gender, active_inactive, outfit_vs_box_vs_pack, number_of_components, item1, item2, item3, item4, item5, item6, item7, item8, total_cost_of_outfit, total_cost_of_outfit_tariff, outfit_imu_percent, outfit_imu_percent_tariff, contains_shoes, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN UPDATE
SET t.product_id = s.product_id,
    t.outfit_alias = s.outfit_alias,
    t.site_name = s.site_name,
    t.vip_retail = s.vip_retail,
    t.type = s.type,
    t.gender = s.gender,
    t.active_inactive = s.active_inactive,
    t.outfit_vs_box_vs_pack = s.outfit_vs_box_vs_pack,
    t.number_of_components = s.number_of_components,
    t.item1 = s.item1,
    t.item2 = s.item2,
    t.item3 = s.item3,
    t.item4 = s.item4,
    t.item5 = s.item5,
    t.item6 = s.item6,
    t.item7 = s.item7,
    t.item8 = s.item8,
    t.total_cost_of_outfit = s.total_cost_of_outfit,
    t.total_cost_of_outfit_tariff = s.total_cost_of_outfit_tariff,
    t.outfit_imu_percent = s.outfit_imu_percent,
    t.outfit_imu_percent_tariff = s.outfit_imu_percent_tariff,
    t.contains_shoes = s.contains_shoes,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;
