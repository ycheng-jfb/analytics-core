 CREATE OR REPLACE TABLE reference.order_processing_statuscode_rank
(
    statuscode_rank                      INT,
    processing_statuscode                INT,
    order_processing_status              VARCHAR(100),
    rank_category                        VARCHAR(25),
    meta_create_datetime DATETIME default current_timestamp,
    meta_update_datetime DATETIME default current_timestamp
);
