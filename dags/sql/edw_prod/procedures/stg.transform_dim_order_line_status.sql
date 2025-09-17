SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE temp TABLE _order_line_status_base AS
SELECT DISTINCT s.statuscode order_line_status_code
              , s.label AS   order_line_status
FROM lake_consolidated.ultra_merchant.statuscode s
JOIN lake_consolidated.ultra_merchant.statuscode_category sc ON s.statuscode >= sc.range_start
    AND s.statuscode <= sc.range_end
WHERE sc.label = 'Orderline Codes'
UNION
SELECT order_line_status_code
     , order_line_status
FROM excp.dim_order_line_status
WHERE meta_is_current_excp
AND meta_data_quality = 'error';

INSERT INTO stg.dim_order_line_status_stg
(
  order_line_status_code
  ,order_line_status
  ,meta_create_datetime
  ,meta_update_datetime
)
SELECT order_line_status_code
     , order_line_status
     , $execution_start_time
     , $execution_start_time
FROM _order_line_status_base;
