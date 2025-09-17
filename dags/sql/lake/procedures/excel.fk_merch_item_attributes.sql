CREATE TABLE IF NOT EXISTS lake_stg.excel.fk_merch_item_attributes_stg (
    style_color STRING,
    prod_id STRING,
    category STRING,
    gender STRING,
    class STRING,
    subclass STRING,
    attribute STRING,
    taxonomy_global_category STRING,
    taxonomy_class STRING,
    site_name STRING,
    color STRING,
    size_model STRING,
    number_of_sizes STRING,
    true_cost STRING,
    tariff_cost STRING,
    landed_cost STRING,
    sale_price STRING,
    vip_price STRING,
    retail STRING,
    imu_percent STRING,
    new_season_code STRING,
    ro_rcvd_date STRING,
    age STRING,
    retail_status STRING,
    initial_showroom STRING,
    year STRING,
    month STRING,
    launch_date STRING,
    reintro_launch_date STRING,
    ttl_rcvd_qty STRING,
    total_oh_qty STRING,
    tariff_flag STRING,
    active_inactive STRING,
    notes STRING
);


CREATE TABLE IF NOT EXISTS lake.excel.fk_merch_item_attributes (
    style_color STRING,
    prod_id NUMBER(38,0),
    category STRING,
    gender STRING,
    class STRING,
    subclass STRING,
    attribute STRING,
    taxonomy_global_category STRING,
    taxonomy_class STRING,
    site_name STRING,
    color STRING,
    size_model STRING,
    number_of_sizes NUMBER(38,0),
    true_cost NUMBER(38,2),
    tariff_cost NUMBER(38,2),
    landed_cost NUMBER(38,2),
    sale_price NUMBER(38,2),
    vip_price NUMBER(38,2),
    retail NUMBER(38,2),
    imu_percent NUMBER(38,2),
    new_season_code STRING,
    ro_rcvd_date STRING,
    age NUMBER(38,0),
    retail_status STRING,
    initial_showroom STRING,
    year NUMBER(38,0),
    month STRING,
    launch_date DATE,
    reintro_launch_date STRING,
    ttl_rcvd_qty NUMBER(38,0),
    total_oh_qty NUMBER(38,0),
    tariff_flag STRING,
    active_inactive STRING,
    notes STRING,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    PRIMARY KEY (style_color, prod_id, category, gender, class, subclass, attribute, taxonomy_global_category, taxonomy_class, site_name, color, size_model, number_of_sizes, true_cost, tariff_cost, landed_cost, sale_price, vip_price, retail, imu_percent, new_season_code, ro_rcvd_date, age, retail_status, initial_showroom, year, month, launch_date, reintro_launch_date, ttl_rcvd_qty, total_oh_qty, tariff_flag, active_inactive, notes)
);

DELETE FROM lake_stg.excel.fk_merch_item_attributes_stg;

COPY INTO lake_stg.excel.fk_merch_item_attributes_stg (style_color, prod_id, category, gender, class, subclass, attribute, taxonomy_global_category, taxonomy_class, site_name, color, size_model, number_of_sizes, true_cost, tariff_cost, landed_cost, sale_price, vip_price, retail, imu_percent, new_season_code, ro_rcvd_date, age, retail_status, initial_showroom, year, month, launch_date, reintro_launch_date, ttl_rcvd_qty, total_oh_qty, tariff_flag, active_inactive, notes)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fk_merch_item_attributes/v2/Item Master.csv.gz'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%'
;

MERGE INTO lake.excel.fk_merch_item_attributes t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY style_color, prod_id, category, gender, class, subclass, attribute, taxonomy_global_category, taxonomy_class, site_name, color, size_model, number_of_sizes, true_cost , tariff_cost, landed_cost, sale_price, vip_price, retail, imu_percent, new_season_code, ro_rcvd_date, age, retail_status, initial_showroom, year, month, launch_date, reintro_launch_date, ttl_rcvd_qty, total_oh_qty, tariff_flag, active_inactive, notes ORDER BY ( SELECT NULL ) ) AS rn
        FROM lake_stg.excel.fk_merch_item_attributes_stg
        WHERE COALESCE(NULLIF(style_color,'TEMPLATE LINES - DON''T DELETE'),'') != ''
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.style_color, s.style_color)
    AND equal_null(t.prod_id, s.prod_id)
    AND equal_null(t.category, s.category)
    AND equal_null(t.gender, s.gender)
    AND equal_null(t.class, s.class)
    AND equal_null(t.subclass, s.subclass)
    AND equal_null(t.attribute, s.attribute)
    AND equal_null(t.taxonomy_global_category, s.taxonomy_global_category)
    AND equal_null(t.taxonomy_class, s.taxonomy_class)
    AND equal_null(t.site_name, s.site_name)
    AND equal_null(t.color, s.color)
    AND equal_null(t.size_model, s.size_model)
    AND equal_null(t.number_of_sizes, s.number_of_sizes)
    AND equal_null(t.true_cost, s.true_cost)
    AND equal_null(t.tariff_cost, s.tariff_cost)
    AND equal_null(t.landed_cost, s.landed_cost)
    AND equal_null(t.sale_price, s.sale_price)
    AND equal_null(t.vip_price, s.vip_price)
    AND equal_null(t.retail, s.retail)
    AND equal_null(t.imu_percent, s.imu_percent)
    AND equal_null(t.new_season_code, s.new_season_code)
    AND equal_null(t.ro_rcvd_date, s.ro_rcvd_date)
    AND equal_null(t.age, s.age)
    AND equal_null(t.retail_status, s.retail_status)
    AND equal_null(t.initial_showroom, s.initial_showroom)
    AND equal_null(t.year, s.year)
    AND equal_null(t.month, s.month)
    AND equal_null(t.launch_date, s.launch_date)
    AND equal_null(t.reintro_launch_date, s.reintro_launch_date)
    AND equal_null(t.ttl_rcvd_qty, s.ttl_rcvd_qty)
    AND equal_null(t.total_oh_qty, s.total_oh_qty)
    AND equal_null(t.tariff_flag, s.tariff_flag)
    AND equal_null(t.active_inactive, s.active_inactive)
    AND equal_null(t.notes, s.notes)
WHEN NOT MATCHED THEN INSERT (
    style_color, prod_id, category, gender, class, subclass, attribute, taxonomy_global_category, taxonomy_class, site_name, color, size_model, number_of_sizes, true_cost, tariff_cost, landed_cost, sale_price, vip_price, retail, imu_percent, new_season_code, ro_rcvd_date, age, retail_status, initial_showroom, year, month, launch_date, reintro_launch_date, ttl_rcvd_qty, total_oh_qty, tariff_flag, active_inactive, notes, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    style_color, prod_id, category, gender, class, subclass, attribute, taxonomy_global_category, taxonomy_class, site_name, color, size_model, number_of_sizes, true_cost, tariff_cost, landed_cost, sale_price, vip_price, retail, imu_percent, new_season_code, ro_rcvd_date, age, retail_status, initial_showroom, year, month, launch_date, reintro_launch_date, ttl_rcvd_qty, total_oh_qty, tariff_flag, active_inactive, notes, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN UPDATE
SET t.style_color = s.style_color,
    t.prod_id = s.prod_id,
    t.category = s.category,
    t.gender = s.gender,
    t.class = s.class,
    t.subclass = s.subclass,
    t.attribute = s.attribute,
    t.taxonomy_global_category = s.taxonomy_global_category,
    t.taxonomy_class = s.taxonomy_class,
    t.site_name = s.site_name,
    t.color = s.color,
    t.size_model = s.size_model,
    t.number_of_sizes = s.number_of_sizes,
    t.true_cost = s.true_cost,
    t.tariff_cost = s.tariff_cost,
    t.landed_cost = s.landed_cost,
    t.sale_price = s.sale_price,
    t.vip_price = s.vip_price,
    t.retail = s.retail,
    t.imu_percent = s.imu_percent,
    t.new_season_code = s.new_season_code,
    t.ro_rcvd_date = s.ro_rcvd_date,
    t.age = s.age,
    t.retail_status = s.retail_status,
    t.initial_showroom = s.initial_showroom,
    t.year = s.year,
    t.month = s.month,
    t.launch_date = s.launch_date,
    t.reintro_launch_date = s.reintro_launch_date,
    t.ttl_rcvd_qty = s.ttl_rcvd_qty,
    t.total_oh_qty = s.total_oh_qty,
    t.tariff_flag = s.tariff_flag,
    t.active_inactive = s.active_inactive,
    t.notes = s.notes,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;
