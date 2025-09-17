CREATE OR REPLACE TABLE reference.gfc_inventory_cost
(
    warehouse_id            INT,
    region_id               INT,
    sku                     VARCHAR,
    inventory_cost          NUMBER(38,6),
    is_deleted              BOOLEAN,
    meta_row_hash           INT,
    meta_create_datetime    TIMESTAMP_LTZ,
    meta_update_datetime    TIMESTAMP_LTZ
);
