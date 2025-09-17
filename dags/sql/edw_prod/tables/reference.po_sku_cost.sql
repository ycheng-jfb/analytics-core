CREATE TABLE IF NOT EXISTS reference.po_sku_cost
(
    po_sku_cost_id INT IDENTITY (1,1),
    po_number VARCHAR,
    sku VARCHAR,
    po_line_number NUMBER(38,0),
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
