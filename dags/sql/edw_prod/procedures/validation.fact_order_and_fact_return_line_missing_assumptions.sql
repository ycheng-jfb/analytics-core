TRUNCATE TABLE validation.fact_order_and_fact_return_line_missing_assumptions;

CREATE OR REPLACE TEMP TABLE _fact_order_assumptions AS
SELECT DISTINCT st.store_brand,
                st.store_region,
                st.store_country,
                CASE
                    WHEN st.store_type = 'Retail' AND
                         dosc.is_retail_ship_only_order = TRUE THEN 'Online'
                    WHEN st.store_type IN ('Mobile App', 'Group Order') THEN 'Online'
                    ELSE st.store_type END                                                   AS store_type,
                dosc.order_classification_l2,
                DATE_TRUNC(MONTH, CAST(
                    COALESCE(stg.shipped_local_datetime, stg.order_local_datetime) AS DATE)) AS order_month
FROM data_model.fact_order AS stg
         JOIN stg.dim_order_status AS p
              ON p.order_status_key = stg.order_status_key
         JOIN stg.dim_store AS st
              ON st.store_id = stg.store_id
         JOIN stg.dim_order_sales_channel AS dosc
              ON dosc.order_sales_channel_key = stg.order_sales_channel_key
WHERE p.order_status IN ('Success', 'Pending')
  AND stg.meta_update_datetime >= DATEADD(DAY, -30, CURRENT_DATE);

CREATE OR REPLACE TEMP TABLE _fact_return_line_assumptions AS
SELECT DISTINCT st.store_brand,
                st.store_region,
                st.store_country,
                IFF(st.store_brand = 'Fabletics' AND st.store_region = 'NA' AND
                    st.store_type IN ('R', 'Retail'), 'Retail', 'Online')      AS store_type,
                DATE_TRUNC(MONTH,
                           CAST(frl.return_completion_local_datetime AS DATE)) AS return_completion_local_date
FROM data_model.fact_return_line AS frl
         JOIN stg.dim_store AS st
              ON st.store_id = frl.store_id
         JOIN stg.dim_return_status AS rs
              ON rs.return_status_key = frl.return_status_key
WHERE rs.return_status IN ('Resolved')
  AND frl.meta_update_datetime >= DATEADD(DAY, -30, CURRENT_DATE);



INSERT INTO validation.fact_order_and_fact_return_line_missing_assumptions
(edw_table, store_brand, store_region, store_country, store_type, order_month)
SELECT DISTINCT 'fact_order' AS edw_table,
                stg.store_brand,
                stg.store_region,
                stg.store_country,
                stg.store_type,
                stg.order_month
FROM _fact_order_assumptions AS stg
         LEFT JOIN reference.finance_assumption AS fa
                   ON fa.brand = stg.store_brand
                       AND IFF(fa.region_type = 'Region', stg.store_region, stg.store_country) = fa.region_type_mapping
                       AND stg.order_month = fa.financial_date
                       AND fa.store_type = stg.store_type
WHERE fa.store_type IS NULL
UNION ALL
SELECT DISTINCT 'fact_return_line'               AS edw_table,
                stg.store_brand,
                stg.store_region,
                stg.store_country,
                stg.store_type,
                stg.return_completion_local_date AS return_completion_month
FROM _fact_return_line_assumptions AS stg
         LEFT JOIN reference.finance_assumption AS fa
                   ON fa.brand = stg.store_brand
                       AND IFF(fa.region_type = 'Region', stg.store_region, stg.store_country) = fa.region_type_mapping
                       AND fa.financial_date = stg.return_completion_local_date
                       AND fa.store_type = stg.store_type
WHERE fa.store_type IS NULL
