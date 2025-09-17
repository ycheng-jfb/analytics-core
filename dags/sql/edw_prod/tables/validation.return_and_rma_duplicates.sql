CREATE OR REPLACE TRANSIENT TABLE validation.return_and_rma_duplicates
(
    rma_id NUMBER,
    rma_product_id NUMBER,
    return_line_id NUMBER,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ
);