CREATE TABLE IF NOT EXISTS reference.po_sku_cost_history
(
    po_sku_cost_id NUMBER,
    po_number VARCHAR,
    sku VARCHAR,
    brand VARCHAR,
    po_status VARCHAR,
    start_date DATE,
    currency VARCHAR,
    quantity INT,
    freight NUMBER(19,4),
    duty NUMBER(19,4),
    cmt NUMBER(19,4),
    cost NUMBER(19,4),
    landed_cost NUMBER(19,4),
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ
);
