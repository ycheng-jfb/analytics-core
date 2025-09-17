CREATE OR REPLACE TABLE reference.order_lines_with_jfca_store
(
    order_line_id        NUMBER(38, 0),
    store_id             NUMBER(38, 0),
    meta_create_datetime TIMESTAMPLTZ DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime TIMESTAMPLTZ DEFAULT CURRENT_TIMESTAMP
);
