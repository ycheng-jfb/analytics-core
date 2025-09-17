CREATE TABLE IF NOT EXISTS reporting_prod.shared.credit_activity_waterfalls_original_cohort
(
    brand                         VARCHAR(20),
    country                       VARCHAR(50),
    region                        VARCHAR(32),
    finance_specialty_store       VARCHAR(20),
    original_issued_month         DATE,
    activity_month                TIMESTAMP_NTZ(9),
    activity_type                 VARCHAR(16777216),
    original_credit_tender        VARCHAR(7),
    original_credit_reason        VARCHAR(50),
    original_credit_type          VARCHAR(15),
    activity_amount               NUMBER(38, 4),
    local_net_vat_activity_amount NUMBER(38, 10),
    usd_gross_vat_activity_amount NUMBER(38, 10),
    usd_net_vat_activity_amount   NUMBER(38, 12),
    snapshot_datetime             TIMESTAMP_LTZ(9)
);
