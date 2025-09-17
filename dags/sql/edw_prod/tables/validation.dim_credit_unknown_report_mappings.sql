CREATE OR REPLACE TRANSIENT TABLE validation.dim_credit_unknown_report_mappings
(
    db                           VARCHAR(9),
    original_credit_type         VARCHAR(25),
    original_credit_reason       VARCHAR(50),
    original_credit_tender       VARCHAR(25),
    original_credit_match_reason VARCHAR(50),
    row_count                    NUMBER(18, 0)
);
