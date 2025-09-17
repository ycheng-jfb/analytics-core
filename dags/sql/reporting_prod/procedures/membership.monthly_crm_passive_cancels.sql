CREATE TABLE IF NOT EXISTS reporting_prod.membership.monthly_crm_passive_cancels
(
    membership_id NUMBER(38,0),
    customer_id NUMBER(38,0),
    passive_month_date DATE,
    datetime_added TIMESTAMP_NTZ(3),
    is_current_month BOOLEAN,
    is_pushed_db50 BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_LTZ
);

SET LowWatermarkDate = current_date;

SET first_of_the_curr_month = DATE_TRUNC('MONTH', $LowWatermarkDate); --first day of the month
SET first_of_the_next_month = DATEADD(mm, 1, $first_of_the_curr_month); --first day of the next month
SET upper_date = DATEADD(YEAR, -1, $first_of_the_next_month); --look back 365 days in time from the first of next month

CREATE OR REPLACE TEMP TABLE _activity_base AS
SELECT
    fo.customer_id AS customer_id
FROM edw_prod.data_model.fact_order fo
JOIN edw_prod.data_model.dim_customer dc
    ON dc.customer_id = fo.customer_id
LEFT JOIN edw_prod.data_model.dim_order_sales_channel dosc
    ON dosc.order_sales_channel_key = fo.order_sales_channel_key
WHERE dosc.order_classification_l2 NOT IN ('Credit Billing', 'Exchange', 'Reship')
    AND fo.order_local_datetime >= $upper_date
    AND fo.order_local_datetime < $first_of_the_next_month

UNION

SELECT
    fo.customer_id AS customer_id
FROM edw_prod.data_model.fact_order fo
JOIN edw_prod.data_model.dim_customer dc
    ON dc.customer_id = fo.customer_id
LEFT JOIN edw_prod.data_model.dim_order_sales_channel dosc
    ON dosc.order_sales_channel_key = fo.order_sales_channel_key
LEFT JOIN edw_prod.data_model.dim_order_payment_status dops
    ON dops.order_payment_status_key = fo.order_payment_status_key
WHERE dops.order_payment_status IN ('Paid', 'Refunded', 'Refunded (Partial)')
    AND dosc.order_classification_l2 = 'Credit Billing'
    AND fo.order_local_datetime >= $upper_date
    AND fo.order_local_datetime < $first_of_the_next_month

UNION

SELECT
    s.customer_id AS customer_id
FROM lake_consolidated_view.ultra_merchant.session s
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = s.customer_id
WHERE s.datetime_added >= $upper_date
    AND s.datetime_added < $first_of_the_next_month

UNION

SELECT
    fca.customer_id
FROM edw_prod.data_model.fact_customer_action fca
JOIN edw_prod.data_model.dim_customer dc
    ON fca.customer_id = dc.customer_id
LEFT JOIN edw_prod.data_model.dim_customer_action_type dcat
    ON fca.customer_action_type_key = dcat.customer_action_type_key
WHERE dcat.customer_action_type = 'Skipped Month'
    AND fca.customer_action_local_datetime >= $upper_date
    AND fca.customer_action_local_datetime < $first_of_the_next_month
;

CREATE OR REPLACE TEMP TABLE _current_month_vips AS
SELECT
    fme.customer_id AS customer_id
FROM edw_prod.data_model.fact_membership_event fme
JOIN edw_prod.data_model.dim_customer dc
    ON fme.customer_id = dc.customer_id
WHERE $first_of_the_next_month >= fme.event_start_local_datetime
    AND $first_of_the_next_month < fme.event_end_local_datetime
    AND fme.membership_event_type = 'Activation';

DELETE FROM reporting_prod.membership.monthly_crm_passive_cancels
WHERE passive_month_date = $first_of_the_next_month;

INSERT INTO reporting_prod.membership.monthly_crm_passive_cancels
(
    membership_id,
    customer_id,
    passive_month_date,
    datetime_added,
    is_current_month
)
SELECT
    m.membership_id,
    cs.customer_id,
    $first_of_the_next_month AS passive_month_date,
    current_date AS datetime_added,
    1 AS is_current_month
FROM _current_month_vips cs
JOIN lake_consolidated_view.ultra_merchant.membership m
    ON cs.customer_id = m.customer_id
LEFT JOIN _activity_base ab
    ON cs.customer_id = ab.customer_id
WHERE ab.customer_id IS NULL
    AND m.membership_type_id <> 2;  -- To exclude membership fee(Annual subscriptions) customers

UPDATE reporting_prod.membership.monthly_crm_passive_cancels
SET is_current_month = 0
WHERE passive_month_date = $first_of_the_curr_month
    AND is_current_month = 1; --Set last month records to is_current_month flag to 0
