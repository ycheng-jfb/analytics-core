CREATE OR REPLACE TABLE reference.store_product_category_tag (
    store_product_category_tag_id INT IDENTITY UNIQUE ,
    store_id INT,
    product_category_id INT,
    meta_original_product_category_id INT,
    tag_id INT,
    meta_original_tag_id INT,
    level VARCHAR(20),
    label VARCHAR(20),
    meta_create_datetime TIMESTAMPLTZ(3) DEFAULT CURRENT_TIMESTAMP(3),
    meta_update_datetime TIMESTAMPLTZ(3) DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (store_id, tag_id)
);
