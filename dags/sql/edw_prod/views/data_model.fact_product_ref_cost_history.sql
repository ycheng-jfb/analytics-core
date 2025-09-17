-- Suppress this view per DA-15409
/*
CREATE OR REPLACE VIEW data_model.fact_product_ref_cost_history
AS
SELECT product_ref_cost_history_key,
       geography_key,
       currency_key,
       sku,
       cost_start_datetime,
       cost_end_datetime,
       cost_type,
       is_current_cost,
       total_cost_amount,
       product_cost_amount,
       cmt_cost_amount,
       freight_cost_amount,
       duty_cost_amount,
       commission_cost_amount,
       meta_row_hash,
       meta_create_datetime,
       meta_update_datetime
FROM stg.fact_product_ref_cost_history;
*/

-- Suppress this view per DA-15409
DROP VIEW IF EXISTS data_model.fact_product_ref_cost_history;
-- PLEASE DO NOT DELETE THE ABOVE LINE AS WE NEED TO ENSURE THIS VIEW DOES NOT GET GENERATED.
