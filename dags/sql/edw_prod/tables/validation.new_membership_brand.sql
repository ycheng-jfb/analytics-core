CREATE OR REPLACE TABLE validation.new_membership_brand
(
    membership_brand_id  NUMBER(38, 0),
    label                VARCHAR(50),
    datetime_added       TIMESTAMP_NTZ(3),
    meta_create_datetime TIMESTAMP_LTZ(3)
);
