CREATE TRANSIENT TABLE IF NOT EXISTS reporting_prod.credit.loyalty_points
(
    store_id                              INT,
    membership_reward_plan_id             INT,
    membership_reward_tier_id             INT,
    membership_reward_tier                VARCHAR(55),
    membership_reward_transaction_id      INT,
    membership_id                         INT,
    customer_id                           INT,
    membership_reward_transaction_type_id INT,
    object_id                             BIGINT,
    points                                BIGINT,
    datetime_added                        TIMESTAMP,
    date_added                            DATE,
    meta_create_datetime                  TIMESTAMP_LTZ NOT NULL,
    meta_update_datetime                  TIMESTAMP_LTZ NOT NULL
);
