CREATE OR REPLACE TEMP TABLE _fact_return_line_base AS
SELECT frl.return_id,
       st.store_brand,
       st.store_region,
       st.store_country,
       IFF(st.store_brand = 'Fabletics' AND st.store_region = 'NA' AND st.store_type IN ('R', 'Retail'), 'Retail',
           'Online')                                                         AS store_type,
       DATE_TRUNC(MONTH, CAST(frl.return_completion_local_datetime AS DATE)) AS return_completion_month,
       SUM(estimated_return_shipping_cost_local_amount)                      AS estimated_return_shipping_cost_sum,
       SUM(estimated_returned_product_resaleable_pct) / COUNT(*)             AS estimated_returned_product_resaleable_pct
FROM stg.fact_return_line frl
         JOIN stg.dim_store AS st
              ON st.store_id = frl.store_id
         JOIN stg.dim_return_status AS rs
              ON rs.return_status_key = frl.return_status_key
WHERE rs.return_status IN ('Resolved')
  AND st.store_brand <> 'Legacy'
  AND frl.is_test_customer = FALSE
  AND frl.is_deleted = FALSE
GROUP BY frl.return_id, st.store_brand, st.store_region, st.store_country, store_type, return_completion_month;

CREATE OR REPLACE TEMP TABLE _fact_return_line_assumption_diff AS
SELECT frlb.return_id,
       fa.bu,
       frlb.return_completion_month,
       fa.financial_date                                   AS order_month,
       frlb.estimated_return_shipping_cost_sum,
       fa.return_shipping_cost_per_order,
       ABS(IFNULL(fa.return_shipping_cost_per_order, 0) -
           frlb.estimated_return_shipping_cost_sum)        AS diff_return_shipping_cost,
       ABS(IFNULL(fa.returned_product_resaleable_percent, 0) -
           frlb.estimated_returned_product_resaleable_pct) AS diff_return_product_res_pct
FROM _fact_return_line_base frlb
         LEFT JOIN reference.finance_assumption AS fa
                   ON fa.brand = frlb.store_brand
                       AND
                      IFF(fa.region_type = 'Region', frlb.store_region, frlb.store_country) = fa.region_type_mapping
                       AND fa.financial_date = frlb.return_completion_month
                       AND fa.store_type = frlb.store_type;

TRUNCATE TABLE validation.finance_assumptions_fact_return_line_mismatch;
INSERT INTO validation.finance_assumptions_fact_return_line_mismatch
SELECT bu,
       return_completion_month,
       SUM(IFF(diff_return_shipping_cost > 0.05
                   OR diff_return_product_res_pct <> 0, 1, 0)) AS variance_record_count,
       COUNT(*)                                                AS total_record_count
FROM _fact_return_line_assumption_diff
GROUP BY bu, return_completion_month
HAVING variance_record_count > 0
ORDER BY bu, return_completion_month;


