CREATE OR REPLACE TABLE reference.iterable_subscription_log
(
    customer_id              NUMBER,
    sms_opt_out              BOOLEAN,
    email_mkt_opt_out        BOOLEAN,
    email_trg_opt_out        BOOLEAN,
    meta_event_datetime      TIMESTAMPLTZ(3),
    effective_start_datetime TIMESTAMPLTZ(3),
    effective_end_datetime   TIMESTAMPLTZ(3),
    is_current               BOOLEAN,
    is_deleted               BOOLEAN,
    meta_type_1_hash         NUMBER,
    meta_type_2_hash         NUMBER,
    meta_create_datetime     TIMESTAMPLTZ(3),
    meta_update_datetime     TIMESTAMPLTZ(3)
);
