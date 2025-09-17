CREATE OR REPLACE TABLE reference.object_relationships
(
    object_type          VARCHAR(255),
    pk_schema_name       VARCHAR(255),
    pk_table_name        VARCHAR(255),
    pk_column_name       VARCHAR(255),
    fk_schema_name       VARCHAR(255),
    fk_table_name        VARCHAR(255),
    fk_column_name       VARCHAR(255),
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT current_timestamp
);
