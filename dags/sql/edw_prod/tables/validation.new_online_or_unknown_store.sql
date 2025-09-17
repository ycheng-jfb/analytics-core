CREATE OR REPLACE TRANSIENT TABLE validation.new_online_or_unknown_store
(
    store_id             NUMBER(38,0),
    store_name           VARCHAR(50),
    alias                VARCHAR(30),
    store_group          VARCHAR(50),
    code                 VARCHAR(80),
    store_type           VARCHAR(20),
    meta_create_datetime TIMESTAMP_LTZ(9),
    meta_update_datetime TIMESTAMP_LTZ(9)
);

