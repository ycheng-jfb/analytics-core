SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- compare activities till yesterday as reporting_base and edw are not being refreshed in same schedule
SET max_credit_activity_local_datetime = (SELECT DATEADD(DAY, -1, MAX(credit_activity_local_datetime))
                                      FROM reporting_base_prod.shared.fact_credit_event);

CREATE OR REPLACE TEMP TABLE _fact_credit_event_comparison__stg AS
SELECT dc.credit_id,
       dc.store_id,
       CAST(credit_activity_local_datetime AS TIMESTAMP_TZ) AS credit_activity_local_datetime,
       credit_activity_type,
       credit_issued_hq_datetime,
       IFNULL(redemption_order_id, -1)                      AS redemption_order_id,
       IFNULL(fce.administrator_id, -1)                     AS administrator_id,
       original_credit_activity_type_action,
       IFNULL(credit_activity_type_reason, '')              AS credit_activity_type_reason,
       credit_activity_source,
       credit_activity_source_reason,
       IFNULL(redemption_store_id, -1)                      AS redemption_store_id,
       vat_rate_ship_to_country,
       credit_activity_vat_rate,
--      IFNULL(credit_activity_usd_conversion_rate, 1)      AS credit_activity_usd_conversion_rate,
       IFNULL(credit_activity_equivalent_count, -1)        AS credit_activity_equivalent_count,
       credit_activity_gross_vat_local_amount,
       credit_activity_local_amount,
       activity_amount_local_amount_issuance_date
FROM reporting_base_prod.shared.dim_credit dc
         JOIN reporting_base_prod.shared.fact_credit_event fce
              ON dc.credit_key = fce.credit_key
         JOIN stg.dim_customer dcc
              ON dc.customer_id = dcc.customer_id
WHERE dcc.is_test_customer = FALSE AND
      fce.credit_activity_local_datetime <= $max_credit_activity_local_datetime
AND NOT EXISTS (
      SELECT 1
      FROM reference.credit_ids_with_issue AS cwi
      WHERE cwi.store_credit_id = dc.credit_id
      AND dc.source_credit_id_type = 'store_credit_id'
)

EXCEPT

SELECT fce.credit_id,
       fce.credit_store_id,
       CAST(fce.credit_activity_local_datetime AS TIMESTAMP_TZ) AS credit_activity_local_datetime,
       fce.credit_activity_type,
       fce.credit_issued_hq_datetime,
       IFNULL(fce.redemption_order_id, -1),
       IFNULL(fce.administrator_id, -1)                         AS administrator_id,
       fce.original_credit_activity_type_action,
       fce.credit_activity_type_reason,
       fce.credit_activity_source,
       fce.credit_activity_source_reason,
       fce.redemption_store_id,
       fce.vat_rate_ship_to_country,
       fce.credit_activity_vat_rate,
--     IFNULL(fce.credit_activity_usd_conversion_rate, 1)       AS credit_activity_usd_conversion_rate,
       IFNULL(fce.credit_activity_equivalent_count, -1)         AS credit_activity_equivalent_count,
       fce.credit_activity_gross_vat_local_amount,
       fce.credit_activity_local_amount,
       fce.activity_amount_local_amount_issuance_date
FROM stg.fact_credit_event as fce
    JOIN stg.dim_credit as dc on dc.credit_key = fce.credit_key
WHERE NOT fce.is_deleted
  AND NOT fce.is_test_customer
  AND fce.credit_activity_local_datetime <= $max_credit_activity_local_datetime
  AND NOT EXISTS (
      SELECT 1
      FROM reference.credit_ids_with_issue AS cwi
      WHERE cwi.store_credit_id = dc.credit_id
      AND dc.source_credit_id_type = 'store_credit_id'
);


--Removing credit ids with splitting amounts in reporting but not in edw with credit_activity_local_datetime from 2016,2017
DELETE
FROM _fact_credit_event_comparison__stg
WHERE credit_id IN
      (
       5016475910, 4936269110, 5969770910, 4361079510, 5225063610, 4361574110, 3972774010, 5237543210, 3495513610,
       5018213910, 4852197110, 5340998110, 5141467210, 4445718510, 3705849110, 4914258610);


TRUNCATE TABLE validation.fact_credit_event_comparison;

INSERT INTO validation.fact_credit_event_comparison (
       credit_id,
       store_id,
       credit_activity_local_datetime,
       credit_activity_type,
       credit_issued_hq_datetime,
       redemption_order_id,
       administrator_id,
       original_credit_activity_type_action,
       credit_activity_type_reason,
       credit_activity_source,
       credit_activity_source_reason,
       redemption_store_id,
       vat_rate_ship_to_country,
       credit_activity_vat_rate,
       credit_activity_equivalent_count,
       credit_activity_gross_vat_local_amount,
       credit_activity_local_amount,
       activity_amount_local_amount_issuance_date,
       meta_create_datetime,
       meta_update_datetime
)
SELECT credit_id,
       store_id,
       credit_activity_local_datetime,
       credit_activity_type,
       credit_issued_hq_datetime,
       redemption_order_id,
       administrator_id,
       original_credit_activity_type_action,
       credit_activity_type_reason,
       credit_activity_source,
       credit_activity_source_reason,
       redemption_store_id,
       vat_rate_ship_to_country,
       credit_activity_vat_rate,
       credit_activity_equivalent_count,
       credit_activity_gross_vat_local_amount,
       credit_activity_local_amount,
       activity_amount_local_amount_issuance_date,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _fact_credit_event_comparison__stg
WHERE (SELECT count(*) FROM _fact_credit_event_comparison__stg) >= 5000;

-- SELECT mismatching_row_count
-- FROM (SELECT COUNT(*) AS mismatching_row_count
--       FROM validation.fact_credit_event_comparison)
-- WHERE mismatching_row_count > 1000;
