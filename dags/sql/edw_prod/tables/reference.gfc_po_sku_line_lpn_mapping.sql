CREATE TABLE IF NOT EXISTS reference.gfc_po_sku_line_lpn_mapping
(
    lpn_id INT,
    lpn_code VARCHAR,
    item_id INT,
    sku VARCHAR,
    po_number VARCHAR,
    po_line_number INT,
    is_deleted BOOLEAN,
    meta_row_hash INT,
    meta_create_datetime timestamp_ltz(9),
    meta_update_datetime timestamp_ltz(9)
);
