
--CREATE TABLE edw_prod.reference.historical_po_landed_cost CLONE reference.historical_po_landed_cost;
ALTER TABLE reference.historical_po_landed_cost ADD COLUMN meta_original_order_line_id INT;

UPDATE reference.historical_po_landed_cost as h
SET h.order_line_id = ol.order_line_id,
    h.meta_original_order_line_id = ol.meta_original_order_line_id
FROM lake_consolidated.ultra_merchant.order_line AS ol
WHERE ol.meta_original_order_line_id = h.order_line_id;
