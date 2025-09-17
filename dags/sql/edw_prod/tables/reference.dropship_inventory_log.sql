CREATE OR REPLACE TABLE reference.dropship_inventory_log
(
    date                                      DATE,
    item_id                                   NUMBER(38, 0),
    warehouse_id                              NUMBER(38, 0),
    partner_item_number                       VARCHAR(50),
    warehouse_available_quantity              NUMBER(38, 0),
    datetime_added                            TIMESTAMP_NTZ(3),
    next_log_datetime                         TIMESTAMP_NTZ(3),
    meta_row_hash                             NUMBER(19, 0),
    meta_create_datetime                      TIMESTAMP_LTZ(9),
    meta_update_datetime                      TIMESTAMP_LTZ(9)
);
