CREATE OR REPLACE TRANSIENT TABLE validation.finance_assumptions_fact_order_mismatch
(
    bu                    VARCHAR(255),
    order_month           DATE,
    variance_record_count NUMBER(13, 0),
    total_record_count    NUMBER(18, 0)
);
