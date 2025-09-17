TRUNCATE TABLE reference.order_lines_with_jfca_store;

INSERT INTO reference.order_lines_with_jfca_store(order_line_id, store_id)
SELECT
    DISTINCT ol.order_line_id, o.store_id
FROM lake_consolidated.ultra_merchant.order_line ol
JOIN lake_consolidated.ultra_merchant."ORDER" o
    ON ol.order_id = o.order_id
WHERE o.store_id = 41;
