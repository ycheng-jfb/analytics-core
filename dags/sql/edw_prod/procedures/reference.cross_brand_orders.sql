CREATE OR REPLACE TRANSIENT TABLE reference.cross_brand_orders AS
SELECT c.customer_id AS c_customer_id, o.customer_id AS o_customer_id, o.order_id
FROM lake_consolidated.ultra_merchant.customer c
         JOIN lake_consolidated.ultra_merchant."ORDER" o
              ON c.meta_original_customer_id = stg.udf_unconcat_brand(o.customer_id)
WHERE c.customer_id != o.customer_id;
