SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- compare activities till yesterday as reporting_base and edw are not being refreshed in same schedule
SET max_credit_issued_local_datetime = (SELECT DATEADD(DAY, -1, MAX(credit_issued_local_datetime))
                                      FROM reporting_base_prod.shared.dim_credit);

-- remove credit ids with issue from comparison
CREATE OR REPLACE TEMP TABLE _dim_credit_validation__credits_with_issue AS
SELECT DISTINCT credit_id, credit_type
FROM stg.dim_credit
WHERE original_credit_key IN (SELECT credit_key
                              FROM stg.dim_credit
                              WHERE credit_id IN (SELECT store_credit_id FROM reference.credit_ids_with_issue)
                                AND credit_type ILIKE '%credit%');

CREATE OR REPLACE TEMP TABLE _dim_credit_validation__difference AS
SELECT credit_id,
       dc.store_id,
       dc.customer_id,
       COALESCE(administrator_id, -1) AS administrator_id,
       credit_order_id,
       source_credit_id_type,
       credit_report_mapping,
       credit_report_sub_mapping,
       credit_type,
       credit_tender,
       credit_reason,
       credit_issuance_reason,
       credit_issued_hq_datetime,
       credit_issued_local_datetime,
       original_credit_tender,
       original_credit_type,
       original_credit_issued_local_datetime,
       original_credit_reason,
       original_credit_match_reason,
       deferred_recognition_label_token,
       credit_issued_usd_conversion_rate,
       credit_issued_local_gross_vat_amount,
       credit_issued_equivalent_count,
       CAST(credit_issued_local_amount AS NUMBER(19, 4)) AS credit_issued_local_amount
FROM reporting_base_prod.shared.dim_credit dc
         JOIN stg.dim_customer dcc
              ON dc.customer_id = dcc.customer_id
WHERE dcc.is_test_customer = FALSE
  AND NOT(source_credit_id_type = 'store_credit_id' AND dc.credit_id IN (SELECT credit_id FROM _dim_credit_validation__credits_with_issue))
  AND dc.credit_issued_local_datetime <= $max_credit_issued_local_datetime
EXCEPT
SELECT credit_id,
       dc.store_id,
       dc.customer_id,
       administrator_id,
       credit_order_id,
       source_credit_id_type,
       credit_report_mapping,
       credit_report_sub_mapping,
       credit_type,
       credit_tender,
       credit_reason,
       credit_issuance_reason,
       credit_issued_hq_datetime,
       credit_issued_local_datetime,
       original_credit_tender,
       original_credit_type,
       original_credit_issued_local_datetime,
       original_credit_reason,
       original_credit_match_reason,
       deferred_recognition_label_token,
       credit_issued_usd_conversion_rate,
       credit_issued_local_gross_vat_amount,
       credit_issued_equivalent_count,
       credit_issued_local_amount
FROM stg.dim_credit dc
JOIN stg.dim_customer dcc
              ON dc.customer_id = dcc.customer_id
WHERE NOT COALESCE(dcc.is_deleted, FALSE)
  AND dcc.is_test_customer = FALSE
  AND NOT(source_credit_id_type = 'store_credit_id' AND dc.credit_id IN (SELECT credit_id FROM _dim_credit_validation__credits_with_issue))
  AND dc.credit_issued_local_datetime <= $max_credit_issued_local_datetime;

TRUNCATE TABLE validation.dim_credit_comparison;

INSERT INTO validation.dim_credit_comparison (
       credit_id,
       store_id,
       customer_id,
       administrator_id,
       credit_order_id,
       source_credit_id_type,
       credit_report_mapping,
       credit_report_sub_mapping,
       credit_type,
       credit_tender,
       credit_reason,
       credit_issuance_reason,
       credit_issued_hq_datetime,
       credit_issued_local_datetime,
       original_credit_tender,
       original_credit_type,
       original_credit_issued_local_datetime,
       original_credit_reason,
       original_credit_match_reason,
       deferred_recognition_label_token,
       credit_issued_usd_conversion_rate,
       credit_issued_local_gross_vat_amount,
       credit_issued_equivalent_count,
       credit_issued_local_amount,
       meta_create_datetime,
       meta_update_datetime
)
SELECT credit_id,
       store_id,
       customer_id,
       administrator_id,
       credit_order_id,
       source_credit_id_type,
       credit_report_mapping,
       credit_report_sub_mapping,
       credit_type,
       credit_tender,
       credit_reason,
       credit_issuance_reason,
       credit_issued_hq_datetime,
       credit_issued_local_datetime,
       original_credit_tender,
       original_credit_type,
       original_credit_issued_local_datetime,
       original_credit_reason,
       original_credit_match_reason,
       deferred_recognition_label_token,
       credit_issued_usd_conversion_rate,
       credit_issued_local_gross_vat_amount,
       credit_issued_equivalent_count,
       credit_issued_local_amount,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _dim_credit_validation__difference
WHERE (SELECT count(*) FROM _dim_credit_validation__difference) >= 5000;

-- SELECT mismatching_row_count
-- FROM (SELECT COUNT(*) AS mismatching_row_count
--       FROM validation.dim_credit_comparison)
-- WHERE mismatching_row_count > 1000;
