CREATE TRANSIENT TABLE IF NOT EXISTS reporting_base_prod.shared.ecom_oracle_noncash_balance_comp_snapshot
(
    store_id              NUMBER(38, 0),
    store                 VARCHAR(50),
    currency              VARCHAR(10),
    credit_id             NUMBER(38, 0),
    credit_type           VARCHAR(17),
    credit_reason         VARCHAR(50),
    date_issued           DATE,
    rollforward_balance   NUMBER(32, 2),
    calc_amount_remaining FLOAT,
    diff                  FLOAT,
    match_status          VARCHAR(16777216),
    ecom_balance          NUMBER(19, 4),
    edw_ecom_diff         NUMBER(35, 4),
    oracle_ecom_diff      FLOAT,
    edw_ecom_status       VARCHAR(11),
    oracle_ecom_status    VARCHAR(11)
);
