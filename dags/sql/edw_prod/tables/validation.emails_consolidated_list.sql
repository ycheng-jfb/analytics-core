CREATE OR REPLACE TRANSIENT TABLE validation.emails_consolidated_list
(
    alert_name    VARCHAR,
    validation_table_name VARCHAR(54),
    count      NUMBER(18, 0)
);
