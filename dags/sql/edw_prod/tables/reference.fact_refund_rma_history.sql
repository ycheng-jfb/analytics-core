CREATE TABLE IF NOT EXISTS reference.fact_refund_rma_history
(
    refund_id                     NUMBER(38, 0),
    meta_original_refund_id       NUMBER(38, 0),
    return_id                     NUMBER(38, 0),
    rma_id                        NUMBER(38, 0),
    refund_request_local_datetime TIMESTAMP_TZ(3),
    meta_create_datetime          TIMESTAMP_TZ(3),
    meta_update_datetime          TIMESTAMP_TZ(3)
)
