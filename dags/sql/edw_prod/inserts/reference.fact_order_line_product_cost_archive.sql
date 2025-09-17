
ALTER TABLE reference.fact_order_line_product_cost_archive RENAME COLUMN order_line_id TO meta_original_order_line_id;
ALTER TABLE reference.fact_order_line_product_cost_archive ADD COLUMN order_line_id INT;

UPDATE reference.fact_order_line_product_cost_archive AS folpc
SET folpc.order_line_id = ol.order_line_id
FROM lake_consolidated.ultra_merchant.order_line AS ol
WHERE ol.meta_original_order_line_id = folpc.meta_original_order_line_id;
