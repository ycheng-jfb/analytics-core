CREATE OR REPLACE TABLE reference.order_payment_statuscode_rank
(
    statuscode_rank                         INT,
    payment_statuscode                      INT,
    order_payment_status                    VARCHAR(100),
    rank_category                           VARCHAR(25),
    meta_create_datetime DATETIME default   current_timestamp,
    meta_update_datetime DATETIME default   current_timestamp
);
