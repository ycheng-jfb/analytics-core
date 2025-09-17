CREATE OR REPLACE TRANSIENT TABLE validation.edw_exception_count
(
    table_name   VARCHAR,
    row_count    NUMBER(38, 0),
    table_schema VARCHAR
);
