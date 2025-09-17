ALTER SESSION SET QUERY_TAG ='fact_credit_event_historical_snapshot_comparison';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET current_date_time = CURRENT_TIMESTAMP::DATE;

CREATE OR REPLACE TEMP TABLE _fact_credit_event_stg AS
SELECT credit_event_key
      ,credit_key
      ,credit_id
      ,customer_id
      ,activation_key
      ,first_activation_key
      ,is_bop_vip
      ,administrator_id
      ,credit_activity_type
      ,original_credit_activity_type_action
      ,credit_activity_type_reason
      ,credit_activity_local_datetime
      ,credit_activity_source
      ,credit_activity_source_reason
      ,credit_store_id
      ,redemption_order_id
      ,redemption_store_id
      ,vat_rate_ship_to_country
      ,credit_activity_vat_rate
      ,credit_activity_usd_conversion_rate
      ,credit_activity_equivalent_count
      ,credit_activity_gross_vat_local_amount
      ,credit_activity_local_amount
      ,activity_amount_local_amount_issuance_date
      ,is_deleted
      ,credit_issued_hq_datetime
FROM  stg.fact_credit_event
WHERE NOT is_deleted AND
      NOT NVL(is_test_customer,False);

SET delete_7_prior_date = DATEADD(day,-7,current_date);
DELETE FROM snapshot.fact_credit_event WHERE snapshot_datetime < $delete_7_prior_date;

INSERT INTO snapshot.fact_credit_event(
       credit_event_key
      ,credit_key
      ,credit_id
      ,customer_id
      ,activation_key
      ,first_activation_key
      ,is_bop_vip
      ,administrator_id
      ,credit_activity_type
      ,original_credit_activity_type_action
      ,credit_activity_type_reason
      ,credit_activity_local_datetime
      ,credit_activity_source
      ,credit_activity_source_reason
      ,credit_store_id
      ,redemption_order_id
      ,redemption_store_id
      ,vat_rate_ship_to_country
      ,credit_activity_vat_rate
      ,credit_activity_usd_conversion_rate
      ,credit_activity_equivalent_count
      ,credit_activity_gross_vat_local_amount
      ,credit_activity_local_amount
      ,activity_amount_local_amount_issuance_date
      ,is_deleted
      ,credit_issued_hq_datetime
      ,snapshot_datetime)
SELECT credit_event_key
      ,credit_key
      ,credit_id
      ,customer_id
      ,activation_key
      ,first_activation_key
      ,is_bop_vip
      ,administrator_id
      ,credit_activity_type
      ,original_credit_activity_type_action
      ,credit_activity_type_reason
      ,credit_activity_local_datetime
      ,credit_activity_source
      ,credit_activity_source_reason
      ,credit_store_id
      ,redemption_order_id
      ,redemption_store_id
      ,vat_rate_ship_to_country
      ,credit_activity_vat_rate
      ,credit_activity_usd_conversion_rate
      ,credit_activity_equivalent_count
      ,credit_activity_gross_vat_local_amount
      ,credit_activity_local_amount
      ,activity_amount_local_amount_issuance_date
      ,is_deleted
      ,credit_issued_hq_datetime
      ,$execution_start_time AS snapshot_datetime
FROM _fact_credit_event_stg;


CREATE OR REPLACE TEMP TABLE _snapshot_order AS
SELECT snapshot_datetime, row_number()OVER(ORDER BY snapshot_datetime DESC) AS row_num
FROM (
         SELECT max(snapshot_datetime) AS snapshot_datetime
         FROM snapshot.fact_credit_event
         WHERE to_time(snapshot_datetime) < '13:00:00'
         GROUP BY cast(snapshot_datetime AS DATE)
         ORDER BY cast(snapshot_datetime AS DATE) DESC
) QUALIFY row_num <= 2;

SET max_snapshot_datetime = (SELECT snapshot_datetime FROM _snapshot_order WHERE row_num = 1);
SET previous_snapshot_datetime = (SELECT snapshot_datetime FROM _snapshot_order WHERE row_num = 2);

/* creating table to make all metrics have same data type so we can unpivot*/
CREATE OR REPLACE TEMP TABLE _snapshot_aggregate (
    SNAPSHOT_DATETIME	TIMESTAMP_NTZ(9),
    CREDIT_ACTIVITY_LOCAL_DATE	DATE,
    CREDIT_STORE_ID	NUMBER(38,0),
    REDEMPTION_STORE_ID	NUMBER(38,0),
    CREDIT_ACTIVITY_TYPE VARCHAR(60),
    CREDIT_ACTIVITY_EQUIVALENT_COUNT	NUMBER(38,10),
    CREDIT_ACTIVITY_GROSS_VAT_LOCAL_AMOUNT	NUMBER(38,10),
    CREDIT_ACTIVITY_LOCAL_AMOUNT	NUMBER(38,10),
    ACTIVITY_AMOUNT_LOCAL_AMOUNT_ISSUANCE_DATE	NUMBER(38,10)

);

INSERT INTO _snapshot_aggregate
(
    snapshot_datetime
    ,credit_activity_local_date
    ,credit_store_id
    ,redemption_store_id
    ,credit_activity_type
    ,credit_activity_equivalent_count
    ,credit_activity_gross_vat_local_amount
    ,credit_activity_local_amount
    ,activity_amount_local_amount_issuance_date
)

SELECT
    snapshot_datetime
    ,DATE_TRUNC('month',credit_activity_local_datetime::date) AS credit_activity_local_date
    ,credit_store_id
    ,redemption_store_id
    ,credit_activity_type
    ,SUM(credit_activity_equivalent_count) AS credit_activity_equivalent_count
    ,SUM(credit_activity_gross_vat_local_amount) AS credit_activity_gross_vat_local_amount
    ,SUM(credit_activity_local_amount) AS credit_activity_local_amount
    ,SUM(activity_amount_local_amount_issuance_date) AS activity_amount_local_amount_issuance_date
FROM snapshot.fact_credit_event AS fce
WHERE
    snapshot_datetime IN ($previous_snapshot_datetime,$max_snapshot_datetime)
    AND credit_activity_local_date < dateadd(month, -2, $previous_snapshot_datetime::date) -- to consider the same amount of days and compare data of past 2 months;
 GROUP BY
    snapshot_datetime
    ,DATE_TRUNC('month',credit_activity_local_datetime::date)
    ,credit_store_id
    ,redemption_store_id
    ,credit_activity_type;

CREATE OR REPLACE TEMP TABLE _snapshot_aggregate_pivot AS
SELECT * FROM _snapshot_aggregate
    UNPIVOT ( value FOR metric IN (
    credit_activity_equivalent_count,
    credit_activity_gross_vat_local_amount,
    credit_activity_local_amount,
    activity_amount_local_amount_issuance_date
    ));


INSERT INTO validation.fact_credit_event_historical_snapshot_comparison
(
    credit_store_id,
    redemption_store_id,
    credit_activity_type,
    credit_activity_local_date,
    metric,
    current_snapshot_value,
    previous_snapshot_value,
    variance,
    current_snapshot_datetime,
    previous_snapshot_datetime,
    meta_create_datetime,
    meta_update_datetime
)


WITH _previous_snapshot AS (
SELECT
    snapshot_datetime,
    credit_activity_local_date,
    credit_store_id,
    redemption_store_id,
    credit_activity_type,
    metric,
    value
FROM _snapshot_aggregate_pivot
WHERE snapshot_datetime = $previous_snapshot_datetime
)

, _max_snapshot AS (
SELECT
    snapshot_datetime,
    credit_activity_local_date,
    credit_store_id,
    redemption_store_id,
    credit_activity_type,
    metric,
    value
FROM _snapshot_aggregate_pivot
WHERE snapshot_datetime = $max_snapshot_datetime
 )

SELECT
    COALESCE(ms.credit_store_id,ps.credit_store_id) AS credit_store_id,
    COALESCE(ms.redemption_store_id,ps.redemption_store_id) AS redemption_store_id,
    COALESCE(ms.credit_activity_type,ps.credit_activity_type) AS credit_activity_type,
    COALESCE(ms.credit_activity_local_date,ps.credit_activity_local_date) AS credit_activity_local_date,
    COALESCE(ms.metric,ps.metric) AS metric,
    COALESCE(ms.value,0) AS current_snapshot_value,
    COALESCE(ps.value,0) AS previous_snapshot_value,
    COALESCE(ms.value,0) - COALESCE(ps.value,0) AS variance,
    $max_snapshot_datetime AS current_snapshot_datetime,
    $previous_snapshot_datetime AS previous_snapshot_datetime,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _max_snapshot AS ms
FULL JOIN _previous_snapshot AS ps
    ON ms.credit_store_id = ps.credit_store_id
    AND ms.redemption_store_id = ps.redemption_store_id
    AND ms.credit_activity_local_date = ps.credit_activity_local_date
    AND ms.credit_activity_type = ps.credit_activity_type
    AND ms.metric = ps.metric
WHERE COALESCE(ps.value,0) <> COALESCE(ms.value,0);

SET delete_prior_date = DATEADD(day,-30,current_date);
DELETE FROM validation.fact_credit_event_historical_snapshot_comparison WHERE meta_create_datetime < $delete_prior_date;

-- Email body for the alert
TRUNCATE TABLE validation.fact_credit_event_historical_snapshot_comparison_count;
INSERT INTO validation.fact_credit_event_historical_snapshot_comparison_count
SELECT
    credit_store_id,
    redemption_store_id,
    credit_activity_type,
    credit_activity_local_date,
    metric,
    current_snapshot_value,
    previous_snapshot_value,
    current_snapshot_datetime,
    previous_snapshot_datetime,
    variance
FROM validation.fact_credit_event_historical_snapshot_comparison
WHERE ABS(variance) > 50 /* ignore rounding issue variances that have variances less than a penny */
AND current_snapshot_datetime::DATE = $current_date_time
AND DATEDIFF('year',credit_activity_local_date,$current_date_time)<=2
ORDER BY current_snapshot_datetime DESC, ABS(variance) DESC
LIMIT 100;
