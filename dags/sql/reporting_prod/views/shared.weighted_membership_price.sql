CREATE OR REPLACE VIEW reporting_prod.shared.weighted_membership_price AS
WITH _bop_customers AS (SELECT DISTINCT customer_id, store_id
                        FROM edw_prod.data_model.fact_activation
                        WHERE activation_local_datetime <= DATE_TRUNC('MONTH', CURRENT_DATE())
                          AND cancellation_local_datetime > DATE_TRUNC('MONTH', CURRENT_DATE())),
     _bop_vips AS (SELECT store_id, COUNT(1) AS bop_vips
                   FROM _bop_customers
                   GROUP BY store_id),
     _membership_price AS (SELECT bc.store_id, m.price, COUNT(1) AS customer_count
                           FROM lake_consolidated_view.ultra_merchant.membership m
                                    JOIN _bop_customers bc ON m.customer_id = bc.customer_id
                               AND bc.store_id = m.store_id
                           GROUP BY bc.store_id, m.price),
     _membership_weighted_price AS (SELECT bv.store_id,
                                           price,
                                           customer_count,
                                           bop_vips,
                                           price * ((customer_count * 1.0) / bv.bop_vips) AS weighted_membership_price
                                    FROM _bop_vips bv
                                             JOIN _membership_price mp ON mp.store_id = bv.store_id)
SELECT store_id,
       SUM(weighted_membership_price) AS weighted_membership_price
FROM _membership_weighted_price
GROUP BY store_id;

