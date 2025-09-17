CREATE OR REPLACE TRANSIENT TABLE validation.fact_credit_event_historical_snapshot_comparison_count
(
    credit_store_id            NUMBER(38, 0),
    redemption_store_id        NUMBER(38, 0),
    credit_activity_type       VARCHAR(60),
    credit_activity_local_date DATE,
    metric                     VARCHAR,
    current_snapshot_value     NUMBER(38, 10),
    previous_snapshot_value    NUMBER(38, 10),
    current_snapshot_datetime  TIMESTAMP_NTZ(9),
    previous_snapshot_datetime VARCHAR,
    variance                   NUMBER(38, 10)
);
