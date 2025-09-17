SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);


CREATE OR REPLACE TEMP TABLE cross_border_fee__combined AS
SELECT 'ecomm shipment'                                         AS shipment_type,
       CAST(order_id AS VARCHAR(250))                           AS po_number_order_id,
       sku,
       shipment_date,
       DATE_TRUNC('month', shipment_date)                       AS shipment_month,
       total_quantity,
       total_duty_amount_per_unit * total_quantity              AS total_duty_amount,
       china_cost_per_unit * total_quantity                     AS total_china_cost_amount,
       total_duty_amount + total_china_cost_amount              AS total_duty_amount_inc_china,
       cotton_cost_per_unit * total_quantity                    AS total_cotton_cost_amount,
       merchant_processing_fee_amount_per_unit * total_quantity AS total_mpf_cost_amount,
       total_cotton_cost_amount + total_mpf_cost_amount         AS total_other_duty_amount
FROM reporting_prod.gsc.cross_border_fee_ecomm
WHERE brand IN ('Fabletics', 'Yitty')
  AND duty_rate IS NOT NULL
  AND sku IS NOT NULL
UNION
SELECT IFF(order_source = 'AMZ-CONS', 'amazon shipment', 'other bulk shipment') AS shipment_type,
       po_number,
       sku,
       shipment_date,
       DATE_TRUNC('month', shipment_date)                                       AS shipment_month,
       total_quantity,
       total_duty_amount_per_unit * total_quantity                              AS total_duty_amount,
       china_cost_per_unit * total_quantity                                     AS total_china_cost_amount,
       total_duty_amount + total_china_cost_amount                              AS total_duty_amount_inc_china,
       cotton_cost_per_unit * total_quantity                                    AS total_cotton_cost_amount,
       merchant_processing_fee_amount_per_unit * total_quantity                 AS total_merchant_processing_cost,
       total_cotton_cost_amount + total_merchant_processing_cost                AS total_other_duty_amount
FROM reporting_prod.gsc.cross_border_fee_bulk_shipment
WHERE brand IN ('Fabletics', 'Yitty')
  AND duty_rate IS NOT NULL
  AND sku IS NOT NULL;



CREATE OR REPLACE TEMP TABLE _cross_border_fee__combined_ratio AS
WITH _total AS (SELECT shipment_month,
                       SUM(total_duty_amount_inc_china) AS agg_total_duty,
                       SUM(total_other_duty_amount)     AS agg_total_duty_other
                FROM cross_border_fee__combined
                GROUP BY shipment_month)
SELECT u.shipment_type,
       u.po_number_order_id,
       u.sku,
       u.shipment_date,
       u.shipment_month,
       u.total_quantity,
       u.total_duty_amount,
       u.total_china_cost_amount,
       u.total_duty_amount_inc_china,
       u.total_cotton_cost_amount,
       u.total_mpf_cost_amount,
       u.total_other_duty_amount,
       u.total_duty_amount_inc_china / t.agg_total_duty   AS total_duty_ratio,
       u.total_other_duty_amount / t.agg_total_duty_other AS total_other_ratio
FROM cross_border_fee__combined u
         JOIN _total t
              ON u.shipment_month = t.shipment_month;


CREATE OR REPLACE TEMP TABLE _cross_border_fee__gglobal_combined AS
SELECT 'Fabletics' AS brand, release_date, duty, other, total
FROM lake_view.excel.cross_border_fee_entry_list
where brand='FL'
UNION
SELECT 'Yitty' AS brand, release_date, duty, other, total
FROM lake_view.excel.cross_border_fee_entry_list
where brand='YT';

CREATE OR REPLACE TEMP TABLE _cross_border_fee__gglobal_actual_paid AS
SELECT DATE_TRUNC('month', release_date) AS release_month, SUM(duty) AS duty, SUM(other) AS other, SUM(total) AS total
FROM _cross_border_fee__gglobal_combined
GROUP BY DATE_TRUNC('month', release_date);


CREATE OR REPLACE TRANSIENT TABLE gsc.cross_border_fee_combined AS
SELECT r.shipment_type,
       r.po_number_order_id,
       r.sku,
       r.shipment_date,
       r.shipment_month,
       r.total_quantity,
       r.total_duty_ratio * g.duty / total_quantity   AS duty_cost_per_unit,
       r.total_other_ratio * g.other / total_quantity AS other_duty_cost_per_unit,
       $execution_start_time as meta_create_datetime,
       $execution_start_time as meta_udpdate_datetime
FROM _cross_border_fee__combined_ratio r
         JOIN _cross_border_fee__gglobal_actual_paid g
              ON r.shipment_month = g.release_month;



-- validation scripts
/*
 -- the below makes sure the totals in reporting_base_prod
WITH _cross_border_agg AS (SELECT shipment_month,
                                  SUM(duty_cost_per_unit * total_quantity)       AS total_duty,
                                  SUM(other_duty_cost_per_unit * total_quantity) AS total_other
                           FROM reporting_prod.gsc.cross_border_fee_combined
                           GROUP BY shipment_month
                           ORDER BY shipment_month),
     _global_combined AS (SELECT DATE_TRUNC('month', release_date) AS rel_month,
                                 SUM(duty)                     AS total_duty,
                                 SUM(other)                    AS total_other
                          FROM _cross_border_fee__gglobal_combined
                          GROUP BY DATE_TRUNC('month', release_date)
                          ORDER BY DATE_TRUNC('month', release_date))
SELECT *
FROM _cross_border_agg cba
         JOIN _global_combined gc
              ON cba.shipment_month = gc.rel_month
WHERE ABS(cba.total_duty - gc.total_duty) > 1
   OR ABS(cba.total_other - gc.total_other) > 1;


-- compare percentages
WITH _total AS (SELECT SUM(duty_cost_per_unit * total_quantity)       AS total_duty,
                       SUM(other_duty_cost_per_unit * total_quantity) AS total_other
                FROM reporting_prod.gsc.cross_border_fee_combined),
     _brand AS (SELECT shipment_type,
                       SUM(duty_cost_per_unit * total_quantity)       AS total_duty,
                       SUM(other_duty_cost_per_unit * total_quantity) AS total_other
                FROM reporting_prod.gsc.cross_border_fee_combined
                GROUP BY shipment_type)

SELECT b.shipment_type,
       b.total_duty,
       t.total_duty,
       100 * b.total_duty / t.total_duty,
       b.total_other,
       t.total_other,
       100 * b.total_other / t.total_other
FROM _brand b,
     _total t;

 */
