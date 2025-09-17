CREATE TABLE reference.variant_price_test_price_correction
(
    order_id INT,
    order_line_id INT,
    meta_original_order_line_id INT,
    product_type_id INT,
    variant_price NUMBER(19,4),
    control_price NUMBER(19,3),
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);
