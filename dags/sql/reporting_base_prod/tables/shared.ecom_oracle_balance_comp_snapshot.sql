CREATE TRANSIENT TABLE IF NOT EXISTS reporting_base_prod.shared.ecom_oracle_balance_comp_snapshot
(
    store_id              NUMBER(38, 0),
    store                 VARCHAR(50),
    currency              VARCHAR(16777216),
    credit_id             NUMBER(38, 0),
    credit_type           VARCHAR(17),
    credit_reason         VARCHAR(50),
    date_issued           DATE,
    vat_prepaid_flag      VARCHAR(16777216),
    receivable_account    VARCHAR(16777216),
    customer_trx_id       VARCHAR(16777216),
    exchange_rate         NUMBER(18, 5),
    trx_type              VARCHAR(16777216),
    rollforward_balance   NUMBER(38, 2),
    calc_amount_remaining NUMBER(20, 2),
    diff                  NUMBER(38, 2),
    match_status          VARCHAR(16777216),
    ecom_balance          NUMBER(19, 4),
    edw_ecom_diff         NUMBER(38, 4),
    oracle_ecom_diff      NUMBER(23, 4),
    edw_ecom_status       VARCHAR(11),
    oracle_ecom_status    VARCHAR(11),
    snapshot_datetime     TIMESTAMP_LTZ(3)
);

ALTER TABLE reporting_base_prod.shared.ecom_oracle_balance_comp_snapshot SET DATA_RETENTION_TIME_IN_DAYS = 0;
