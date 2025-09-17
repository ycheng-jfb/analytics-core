CREATE OR REPLACE TRANSIENT TABLE validation.new_store_in_dim_store
(
    store_id        NUMBER(38, 0),
    store_full_name VARCHAR(50),
    store_type      VARCHAR(20),
    store_sub_type  VARCHAR(50),
    store_brand     VARCHAR(20),
    store_country   VARCHAR(50),
    store_region    VARCHAR(32)
);
