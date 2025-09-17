CREATE TABLE IF NOT EXISTS reference.landed_cost_manual_refresh
(
    po_dtl_id            NUMBER(38, 0),
    meta_create_datetime TIMESTAMP_LTZ(9),
    to_be_processed      BOOLEAN DEFAULT TRUE,
    record_type          VARCHAR(10)
);
