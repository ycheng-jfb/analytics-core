CREATE TABLE IF NOT EXISTS action_from_billable_vips
(
    order_month                       DATE,
    store_name                        VARCHAR(50),
    region                            VARCHAR(32),
    country                           VARCHAR(50),
    MAX_DAY                           NUMBER(2, 0),
    BOP_VIPs                          NUMBER(18, 0),
    Billable_VIPs                     NUMBER(18, 0),
    customer_action_category          VARCHAR(75),
    creditcard_type                   VARCHAR(55),
    successful_first_billing_attempts NUMBER(30, 0),
    successful_retries                NUMBER(30, 0),
    first_time_success_rate_daily     NUMBER(30, 2),
    retry_success_rate_daily          NUMBER(30, 2)
);
