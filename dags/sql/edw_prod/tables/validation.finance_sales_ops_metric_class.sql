CREATE OR REPLACE TABLE validation.finance_sales_ops_metric_class
(
    table_name VARCHAR(250),
    column_name VARCHAR(250),
    metric_source VARCHAR(250),
    metric_class VARCHAR(250),
    validation_inclusion BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT current_timestamp

);
