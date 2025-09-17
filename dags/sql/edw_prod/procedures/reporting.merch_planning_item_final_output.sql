SET target_table = 'reporting.merch_planning_item_final_output';
SET is_full_refresh = (SELECT CASE
                                  WHEN stg.udf_get_watermark($target_table, NULL) = '1900-01-01' THEN TRUE
                                  WHEN MAX(full_date) = CURRENT_DATE THEN TRUE
                                  ELSE FALSE END
                       FROM data_model.dim_date
                       WHERE month_date = DATE_TRUNC('month', CURRENT_DATE)
                         AND day_name_of_week = 'Friday');

MERGE INTO stg.meta_table_dependency_watermark AS w
USING (
    SELECT
        $target_table AS table_name,
        NULL AS dependent_table_name,
        (
            SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
            FROM reporting.merch_planning_item_final_output
        ) AS new_high_watermark_datetime
    ) AS s
    ON w.table_name = s.table_name
    AND w.dependent_table_name IS NOT DISTINCT FROM s.dependent_table_name
WHEN NOT MATCHED THEN
    INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01', -- current high_watermark_datetime
        s.new_high_watermark_datetime
        )
WHEN MATCHED AND w.new_high_watermark_datetime IS DISTINCT FROM s.new_high_watermark_datetime THEN
    UPDATE
    SET w.new_high_watermark_datetime = s.new_high_watermark_datetime,
        w.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

/*
Incremental Logic:
    last_update_datetime: Maximum meta_update_datetime from merch_planning_item_final_output. 1900-01-01 if null
    processing_dates: Month date of MIN(date) from merch_planning_item_product_sku where
                        meta_update_datetime > last_update_datetime. 2019-01-01 if null
*/


SET last_update_datetime = (
    SELECT CASE WHEN $is_full_refresh = TRUE THEN '1900-01-01'
        ELSE COALESCE(MAX(meta_update_datetime), '1900-01-01'::TIMESTAMP) END
    FROM reporting.merch_planning_item_final_output
);

SET warehouse_to_be_used = IFF($last_update_datetime = '1900-01-01', 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

SET processing_dates = (
    SELECT DATE_TRUNC('MONTH', MIN(date))
    FROM reporting.merch_planning_item_product_sku
    WHERE meta_update_datetime > $last_update_datetime
);

BEGIN TRANSACTION;

CREATE OR REPLACE TEMPORARY TABLE _dim_date_week AS
SELECT date_key, full_date, week_of_year
FROM data_model.dim_date
WHERE day_of_week = 1;

CREATE OR REPLACE TEMPORARY TABLE _dim_date_base AS
SELECT *, LEAD(full_date, 1) OVER(ORDER BY full_date) AS next_week
FROM _dim_date_week
ORDER BY full_date;

CREATE OR REPLACE TEMPORARY TABLE _period AS
SELECT 'Weekly' AS source,
    dd.full_date,
    ddo.full_date AS start_date,
    DATEADD(DAY, -1, ddo.next_week) AS end_date
FROM data_model.dim_date dd
JOIN _dim_date_base ddo ON dd.full_date >= ddo.full_date
    AND dd.full_date < ddo.next_week
WHERE dd.full_date >= DATEADD('DAY', -1, DATE_TRUNC('WEEK', $processing_dates))
    AND dd.full_date < CURRENT_DATE()

UNION ALL

SELECT 'Monthly' AS source,
    full_date,
    month_date AS start_date,
    IFF(DATEADD('day', -1, DATEADD('month', 1, month_date)) > CURRENT_DATE() - 1, CURRENT_DATE() - 1, DATEADD('day', -1, DATEADD('month', 1, month_date))) AS end_date
FROM data_model.dim_date
WHERE full_date >= DATE_TRUNC('MONTH', $processing_dates)
    AND full_date < CURRENT_DATE()

UNION ALL

SELECT 'Quarterly' AS source, full_date, DATE_TRUNC('quarter', full_date) AS start_date,
    IFF(DATEADD('day', -1, DATEADD('quarter', 1, start_date)) > CURRENT_DATE() - 1, CURRENT_DATE() - 1, DATEADD('day', -1, DATEADD('quarter', 1, start_date))) AS end_date
FROM data_model.dim_date
WHERE full_date >= DATE_TRUNC('QUARTER', $processing_dates)
    AND full_date < CURRENT_DATE()

UNION ALL

SELECT 'Yearly' AS source,
    full_date,
    DATE_TRUNC('year', full_date) AS start_date,
    IFF(DATEADD('day', -1, DATEADD('year', 1, start_date)) > CURRENT_DATE() - 1, CURRENT_DATE() - 1, DATEADD('day', -1, DATEADD('year', 1, start_date))) AS end_date
FROM data_model.dim_date
WHERE full_date >= DATE_TRUNC('YEAR', $processing_dates)
    AND full_date < CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _product_sku AS
SELECT store_brand,
    country,
    product_brand,
    product_sku,
    wms_class
FROM (
    SELECT UPPER(ds.store_brand) AS store_brand,
        ds.store_country AS country,
        product_brand,
        product_sku,
        wms_class,
        ROW_NUMBER() OVER(PARTITION BY UPPER(ds.store_brand), ds.store_country, product_brand, product_sku ORDER BY IFF(dp.is_active = 'TRUE', 1, 0) DESC, dp.current_showroom_date DESC, dp.meta_create_datetime DESC, dp.meta_update_datetime DESC, wms_class) AS rnk
    FROM data_model.dim_product dp
    JOIN data_model.dim_store ds ON ds.store_id = dp.store_id
    JOIN (SELECT DISTINCT i.item_number,
                CASE WHEN UPPER(label) = 'FABLETICS' AND UPPER(department) = 'SHAPEWEAR' THEN 'YITTY' ELSE UPPER(label) END AS product_brand
FROM lake_view.ultra_warehouse.item i
         JOIN lake_view.ultra_warehouse.company c ON i.company_id = c.company_id) pb ON dp.sku = pb.item_number
)a
WHERE rnk = 1;

DELETE FROM reporting.merch_planning_item_final_output ifo
USING (
    SELECT source,
        MIN(start_date) AS start_date
    FROM _period
    GROUP BY source
) p WHERE p.source = ifo.period
    AND ifo.date >= start_date;

SET meta_create_datetime = (SELECT MAX(meta_create_datetime) FROM reporting.merch_planning_item_product_sku);
SET meta_update_datetime = (SELECT MAX(meta_update_datetime) FROM reporting.merch_planning_item_product_sku);

INSERT INTO reporting.merch_planning_item_final_output(
    date,
    period,
    date_type,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    country,
    finance_specialty_store,
    region,
    product_brand,
    product_sku,
    base_sku,
    is_retail,
    wms_class,
    ty_total_unit_count,
    ty_unit_count_activating,
    ty_unit_count_nonactivating,
    ty_unit_count_repeat,
    ty_unit_count_guest,
    ty_total_product_subtotal,
    ty_product_subtotal_activating,
    ty_product_subtotal_nonactivating,
    ty_product_subtotal_repeat,
    ty_product_subtotal_guest,
    ty_total_product_discount,
    ty_product_discount_activating,
    ty_product_discount_nonactivating,
    ty_product_discount_repeat,
    ty_product_discount_guest,
    ty_total_product_gross_revenue_excl_shipping,
    ty_product_gross_revenue_excl_shipping_activating,
    ty_product_gross_revenue_excl_shipping_nonactivating,
    ty_product_gross_revenue_excl_shipping_repeat,
    ty_product_gross_revenue_excl_shipping_guest,
    ty_total_product_margin_pre_return_excl_shipping,
    ty_product_margin_pre_return_excl_shipping_activating,
    ty_product_margin_pre_return_excl_shipping_nonactivating,
    ty_product_margin_pre_return_excl_shipping_repeat,
    ty_product_margin_pre_return_excl_shipping_guest,
    ty_total_product_cost,
    ty_product_cost_activating,
    ty_product_cost_nonactivating,
    ty_product_cost_repeat,
    ty_product_cost_guest,
    ty_total_product_cost_c,
    ty_product_cost_activating_c,
    ty_product_cost_nonactivating_c,
    ty_product_cost_repeat_c,
    ty_product_cost_guest_c,
    ty_total_non_reduced_vip_price,
    ty_non_reduced_vip_price_activating,
    ty_non_reduced_vip_price_nonactivating,
    ty_non_reduced_vip_price_repeat,
    ty_non_reduced_vip_price_guest,
    ty_total_return_unit_count,
    ty_return_unit_count_activating,
    ty_return_unit_count_nonactivating,
    ty_return_unit_count_repeat,
    ty_return_unit_count_guest,
    ty_total_returned_product_gross_rev_excluding_shipping,
    ty_returned_product_gross_rev_excluding_shipping_activating,
    ty_returned_product_gross_rev_excluding_shipping_nonactivating,
    ty_returned_product_gross_rev_excluding_shipping_repeat,
    ty_returned_product_gross_rev_excluding_shipping_guest,
    ty_bop_onhand_qty,
    ty_bop_available_to_sell_qty,
    ty_bop_open_to_buy_qty,
    ty_bop_open_to_buy_qty_incl_in_transit,
    ty_bop_retail_reserve,
    ty_bop_inventory_cost,
    ty_eop_onhand_qty,
    ty_eop_available_to_sell_qty,
    ty_eop_open_to_buy_qty,
    ty_eop_open_to_buy_qty_incl_in_transit,
    ty_eop_retail_reserve,
    ty_eop_inventory_cost,
    mtd_total_unit_count,
    mtd_unit_count_activating,
    mtd_unit_count_nonactivating,
    mtd_unit_count_repeat,
    mtd_unit_count_guest,
    mtd_total_product_subtotal,
    mtd_product_subtotal_activating,
    mtd_product_subtotal_nonactivating,
    mtd_product_subtotal_repeat,
    mtd_product_subtotal_guest,
    mtd_total_product_discount,
    mtd_product_discount_activating,
    mtd_product_discount_nonactivating,
    mtd_product_discount_repeat,
    mtd_product_discount_guest,
    mtd_total_product_gross_revenue_excl_shipping,
    mtd_product_gross_revenue_excl_shipping_activating,
    mtd_product_gross_revenue_excl_shipping_nonactivating,
    mtd_product_gross_revenue_excl_shipping_repeat,
    mtd_product_gross_revenue_excl_shipping_guest,
    mtd_total_product_margin_pre_return_excl_shipping,
    mtd_product_margin_pre_return_excl_shipping_activating,
    mtd_product_margin_pre_return_excl_shipping_nonactivating,
    mtd_product_margin_pre_return_excl_shipping_repeat,
    mtd_product_margin_pre_return_excl_shipping_guest,
    mtd_total_product_cost,
    mtd_product_cost_activating,
    mtd_product_cost_nonactivating,
    mtd_product_cost_repeat,
    mtd_product_cost_guest,
    mtd_total_product_cost_c,
    mtd_product_cost_activating_c,
    mtd_product_cost_nonactivating_c,
    mtd_product_cost_repeat_c,
    mtd_product_cost_guest_c,
    mtd_total_non_reduced_vip_price,
    mtd_non_reduced_vip_price_activating,
    mtd_non_reduced_vip_price_nonactivating,
    mtd_non_reduced_vip_price_repeat,
    mtd_non_reduced_vip_price_guest,
    mtd_total_return_unit_count,
    mtd_return_unit_count_activating,
    mtd_return_unit_count_nonactivating,
    mtd_return_unit_count_repeat,
    mtd_return_unit_count_guest,
    mtd_total_returned_product_gross_rev_excluding_shipping,
    mtd_returned_product_gross_rev_excluding_shipping_activating,
    mtd_returned_product_gross_rev_excluding_shipping_nonactivating,
    mtd_returned_product_gross_rev_excluding_shipping_repeat,
    mtd_returned_product_gross_rev_excluding_shipping_guest,
    ly_total_unit_count,
    ly_unit_count_activating,
    ly_unit_count_nonactivating,
    ly_unit_count_repeat,
    ly_unit_count_guest,
    ly_total_product_subtotal,
    ly_product_subtotal_activating,
    ly_product_subtotal_nonactivating,
    ly_product_subtotal_repeat,
    ly_product_subtotal_guest,
    ly_total_product_discount,
    ly_product_discount_activating,
    ly_product_discount_nonactivating,
    ly_product_discount_repeat,
    ly_product_discount_guest,
    ly_total_product_gross_revenue_excl_shipping,
    ly_product_gross_revenue_excl_shipping_activating,
    ly_product_gross_revenue_excl_shipping_nonactivating,
    ly_product_gross_revenue_excl_shipping_repeat,
    ly_product_gross_revenue_excl_shipping_guest,
    ly_total_product_margin_pre_return_excl_shipping,
    ly_product_margin_pre_return_excl_shipping_activating,
    ly_product_margin_pre_return_excl_shipping_nonactivating,
    ly_product_margin_pre_return_excl_shipping_repeat,
    ly_product_margin_pre_return_excl_shipping_guest,
    ly_total_product_cost,
    ly_product_cost_activating,
    ly_product_cost_nonactivating,
    ly_product_cost_repeat,
    ly_product_cost_guest,
    ly_total_product_cost_c,
    ly_product_cost_activating_c,
    ly_product_cost_nonactivating_c,
    ly_product_cost_repeat_c,
    ly_product_cost_guest_c,
    ly_total_non_reduced_vip_price,
    ly_non_reduced_vip_price_activating,
    ly_non_reduced_vip_price_nonactivating,
    ly_non_reduced_vip_price_repeat,
    ly_non_reduced_vip_price_guest,
    ly_total_return_unit_count,
    ly_return_unit_count_activating,
    ly_return_unit_count_nonactivating,
    ly_return_unit_count_repeat,
    ly_return_unit_count_guest,
    ly_total_returned_product_gross_rev_excluding_shipping,
    ly_returned_product_gross_rev_excluding_shipping_activating,
    ly_returned_product_gross_rev_excluding_shipping_nonactivating,
    ly_returned_product_gross_rev_excluding_shipping_repeat,
    ly_returned_product_gross_rev_excluding_shipping_guest,
    ly_bop_onhand_qty,
    ly_bop_available_to_sell_qty,
    ly_bop_open_to_buy_qty,
    ly_bop_open_to_buy_qty_incl_in_transit,
    ly_bop_retail_reserve,
    ly_bop_inventory_cost,
    ly_eop_onhand_qty,
    ly_eop_available_to_sell_qty,
    ly_eop_open_to_buy_qty,
    ly_eop_open_to_buy_qty_incl_in_transit,
    ly_eop_retail_reserve,
    ly_eop_inventory_cost,
    lymtd_total_unit_count,
    lymtd_unit_count_activating,
    lymtd_unit_count_nonactivating,
    lymtd_unit_count_repeat,
    lymtd_unit_count_guest,
    lymtd_total_product_subtotal,
    lymtd_product_subtotal_activating,
    lymtd_product_subtotal_nonactivating,
    lymtd_product_subtotal_repeat,
    lymtd_product_subtotal_guest,
    lymtd_total_product_discount,
    lymtd_product_discount_activating,
    lymtd_product_discount_nonactivating,
    lymtd_product_discount_repeat,
    lymtd_product_discount_guest,
    lymtd_total_product_gross_revenue_excl_shipping,
    lymtd_product_gross_revenue_excl_shipping_activating,
    lymtd_product_gross_revenue_excl_shipping_nonactivating,
    lymtd_product_gross_revenue_excl_shipping_repeat,
    lymtd_product_gross_revenue_excl_shipping_guest,
    lymtd_total_product_margin_pre_return_excl_shipping,
    lymtd_product_margin_pre_return_excl_shipping_activating,
    lymtd_product_margin_pre_return_excl_shipping_nonactivating,
    lymtd_product_margin_pre_return_excl_shipping_repeat,
    lymtd_product_margin_pre_return_excl_shipping_guest,
    lymtd_total_product_cost,
    lymtd_product_cost_activating,
    lymtd_product_cost_nonactivating,
    lymtd_product_cost_repeat,
    lymtd_product_cost_guest,
    lymtd_total_product_cost_c,
    lymtd_product_cost_activating_c,
    lymtd_product_cost_nonactivating_c,
    lymtd_product_cost_repeat_c,
    lymtd_product_cost_guest_c,
    lymtd_total_non_reduced_vip_price,
    lymtd_non_reduced_vip_price_activating,
    lymtd_non_reduced_vip_price_nonactivating,
    lymtd_non_reduced_vip_price_repeat,
    lymtd_non_reduced_vip_price_guest,
    lymtd_total_return_unit_count,
    lymtd_return_unit_count_activating,
    lymtd_return_unit_count_nonactivating,
    lymtd_return_unit_count_repeat,
    lymtd_return_unit_count_guest,
    lymtd_total_returned_product_gross_rev_excluding_shipping,
    lymtd_returned_product_gross_rev_excluding_shipping_activating,
    lymtd_returned_product_gross_rev_excluding_shipping_nonactivating,
    lymtd_returned_product_gross_rev_excluding_shipping_repeat,
    lymtd_returned_product_gross_rev_excluding_shipping_guest,
    meta_create_datetime,
    meta_update_datetime
)
SELECT p.start_date AS date,
    p.source AS period,
    date_type,
    store_id,
    store_name,
    store_full_name,
    ips.store_brand,
    store_type,
    ips.country,
    finance_specialty_store,
    region,
    ips.product_brand,
    ips.product_sku,
    base_sku,
    is_retail,
    ps.wms_class,
    SUM(ty_total_unit_count) AS ty_total_unit_count,
    SUM(ty_unit_count_activating) AS ty_unit_count_activating,
    SUM(ty_unit_count_nonactivating) AS ty_unit_count_nonactivating,
    SUM(ty_unit_count_repeat) AS ty_unit_count_repeat,
    SUM(ty_unit_count_guest) AS ty_unit_count_guest,
    SUM(ty_total_product_subtotal) AS ty_total_product_subtotal,
    SUM(ty_product_subtotal_activating) AS ty_product_subtotal_activating,
    SUM(ty_product_subtotal_nonactivating) AS ty_product_subtotal_nonactivating,
    SUM(ty_product_subtotal_repeat) AS ty_product_subtotal_repeat,
    SUM(ty_product_subtotal_guest) AS ty_product_subtotal_guest,
    SUM(ty_total_product_discount) AS ty_total_product_discount,
    SUM(ty_product_discount_activating) AS ty_product_discount_activating,
    SUM(ty_product_discount_nonactivating) AS ty_product_discount_nonactivating,
    SUM(ty_product_discount_repeat) AS ty_product_discount_repeat,
    SUM(ty_product_discount_guest) AS ty_product_discount_guest,
    SUM(ty_total_product_gross_revenue_excl_shipping) AS ty_total_product_gross_revenue_excl_shipping,
    SUM(ty_product_gross_revenue_excl_shipping_activating) AS ty_product_gross_revenue_excl_shipping_activating,
    SUM(ty_product_gross_revenue_excl_shipping_nonactivating) AS ty_product_gross_revenue_excl_shipping_nonactivating,
    SUM(ty_product_gross_revenue_excl_shipping_repeat) AS ty_product_gross_revenue_excl_shipping_repeat,
    SUM(ty_product_gross_revenue_excl_shipping_guest) AS ty_product_gross_revenue_excl_shipping_guest,
    SUM(ty_total_product_margin_pre_return_excl_shipping) AS ty_total_product_margin_pre_return_excl_shipping,
    SUM(ty_product_margin_pre_return_excl_shipping_activating) AS ty_product_margin_pre_return_excl_shipping_activating,
    SUM(ty_product_margin_pre_return_excl_shipping_nonactivating) AS ty_product_margin_pre_return_excl_shipping_nonactivating,
    SUM(ty_product_margin_pre_return_excl_shipping_repeat) AS ty_product_margin_pre_return_excl_shipping_repeat,
    SUM(ty_product_margin_pre_return_excl_shipping_guest) AS ty_product_margin_pre_return_excl_shipping_guest,
    SUM(ty_total_product_cost) AS ty_total_product_cost,
    SUM(ty_product_cost_activating) AS ty_product_cost_activating,
    SUM(ty_product_cost_nonactivating) AS ty_product_cost_nonactivating,
    SUM(ty_product_cost_repeat) AS ty_product_cost_repeat,
    SUM(ty_product_cost_guest) AS ty_product_cost_guest,
    SUM(ty_total_product_cost_c) AS ty_total_product_cost_c,
    SUM(ty_product_cost_activating_c) AS ty_product_cost_activating_c,
    SUM(ty_product_cost_nonactivating_c) AS ty_product_cost_nonactivating_c,
    SUM(ty_product_cost_repeat_c) AS ty_product_cost_repeat_c,
    SUM(ty_product_cost_guest_c) AS ty_product_cost_guest_c,
    SUM(ty_total_non_reduced_vip_price) AS ty_total_non_reduced_vip_price,
    SUM(ty_non_reduced_vip_price_activating) AS ty_non_reduced_vip_price_activating,
    SUM(ty_non_reduced_vip_price_nonactivating) AS ty_non_reduced_vip_price_nonactivating,
    SUM(ty_non_reduced_vip_price_repeat) AS ty_non_reduced_vip_price_repeat,
    SUM(ty_non_reduced_vip_price_guest) AS ty_non_reduced_vip_price_guest,
    SUM(ty_total_return_unit_count) AS ty_total_return_unit_count,
    SUM(ty_return_unit_count_activating) AS ty_return_unit_count_activating,
    SUM(ty_return_unit_count_nonactivating) AS ty_return_unit_count_nonactivating,
    SUM(ty_return_unit_count_repeat) AS ty_return_unit_count_repeat,
    SUM(ty_return_unit_count_guest) AS ty_return_unit_count_guest,
    SUM(ty_total_returned_product_gross_rev_excluding_shipping) AS ty_total_returned_product_gross_rev_excluding_shipping,
    SUM(ty_returned_product_gross_rev_excluding_shipping_activating) AS ty_returned_product_gross_rev_excluding_shipping_activating,
    SUM(ty_returned_product_gross_rev_excluding_shipping_nonactivating) AS ty_returned_product_gross_rev_excluding_shipping_nonactivating,
    SUM(ty_returned_product_gross_rev_excluding_shipping_repeat) AS ty_returned_product_gross_rev_excluding_shipping_repeat,
    SUM(ty_returned_product_gross_rev_excluding_shipping_guest) AS ty_returned_product_gross_rev_excluding_shipping_guest,
    SUM(IFF(ips.date = p.start_date, ty_bop_onhand_qty, 0)) AS ty_bop_onhand_qty,
    SUM(IFF(ips.date = p.start_date, ty_bop_available_to_sell_qty, 0)) AS ty_bop_available_to_sell_qty,
    SUM(IFF(ips.date = p.start_date, ty_bop_open_to_buy_qty, 0)) AS ty_bop_open_to_buy_qty,
    SUM(IFF(ips.date = p.start_date, ty_bop_open_to_buy_qty_incl_in_transit, 0)) AS ty_bop_open_to_buy_qty_incl_in_transit,
    SUM(IFF(ips.date = p.start_date, ty_bop_retail_reserve, 0)) AS ty_bop_retail_reserve,
    SUM(IFF(ips.date = p.start_date, ty_bop_inventory_cost, 0)) AS ty_bop_inventory_cost,
    SUM(IFF(ips.date = p.end_date, ty_eop_onhand_qty, 0)) AS ty_eop_onhand_qty,
    SUM(IFF(ips.date = p.end_date, ty_eop_available_to_sell_qty, 0)) AS ty_eop_available_to_sell_qty,
    SUM(IFF(ips.date = p.end_date, ty_eop_open_to_buy_qty, 0)) AS ty_eop_open_to_buy_qty,
    SUM(IFF(ips.date = p.end_date, ty_eop_open_to_buy_qty_incl_in_transit, 0)) AS ty_eop_open_to_buy_qty_incl_in_transit,
    SUM(IFF(ips.date = p.end_date, ty_eop_retail_reserve, 0)) AS ty_eop_retail_reserve,
    SUM(IFF(ips.date = p.end_date, ty_eop_inventory_cost, 0)) AS ty_eop_inventory_cost,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_unit_count, 0)) AS mtd_total_unit_count,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_unit_count_activating, 0)) AS mtd_unit_count_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_unit_count_nonactivating, 0)) AS mtd_unit_count_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_unit_count_repeat, 0)) AS mtd_unit_count_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_unit_count_guest, 0)) AS mtd_unit_count_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_product_subtotal, 0)) AS mtd_total_product_subtotal,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_subtotal_activating, 0)) AS mtd_product_subtotal_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_subtotal_nonactivating, 0)) AS mtd_product_subtotal_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_subtotal_repeat, 0)) AS mtd_product_subtotal_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_subtotal_guest, 0)) AS mtd_product_subtotal_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_product_discount, 0)) AS mtd_total_product_discount,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_discount_activating, 0)) AS mtd_product_discount_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_discount_nonactivating, 0)) AS mtd_product_discount_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_discount_repeat, 0)) AS mtd_product_discount_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_discount_guest, 0)) AS mtd_product_discount_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_product_gross_revenue_excl_shipping, 0)) AS mtd_total_product_gross_revenue_excl_shipping,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_gross_revenue_excl_shipping_activating, 0)) AS mtd_product_gross_revenue_excl_shipping_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_gross_revenue_excl_shipping_nonactivating, 0)) AS mtd_product_gross_revenue_excl_shipping_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_gross_revenue_excl_shipping_repeat, 0)) AS mtd_product_gross_revenue_excl_shipping_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_gross_revenue_excl_shipping_guest, 0)) AS mtd_product_gross_revenue_excl_shipping_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_product_margin_pre_return_excl_shipping, 0)) AS mtd_total_product_margin_pre_return_excl_shipping,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_margin_pre_return_excl_shipping_activating, 0)) AS mtd_product_margin_pre_return_excl_shipping_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_margin_pre_return_excl_shipping_nonactivating, 0)) AS mtd_product_margin_pre_return_excl_shipping_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_margin_pre_return_excl_shipping_repeat, 0)) AS mtd_product_margin_pre_return_excl_shipping_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_margin_pre_return_excl_shipping_guest, 0)) AS mtd_product_margin_pre_return_excl_shipping_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_product_cost, 0)) AS mtd_total_product_cost,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_cost_activating, 0)) AS mtd_product_cost_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_cost_nonactivating, 0)) AS mtd_product_cost_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_cost_repeat, 0)) AS mtd_product_cost_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_cost_guest, 0)) AS mtd_product_cost_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_product_cost_c, 0)) AS mtd_total_product_cost_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_cost_activating_c, 0)) AS mtd_product_cost_activating_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_cost_nonactivating_c, 0)) AS mtd_product_cost_nonactivating_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_cost_repeat_c, 0)) AS mtd_product_cost_repeat_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_product_cost_guest_c, 0)) AS mtd_product_cost_guest_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_non_reduced_vip_price, 0)) AS mtd_total_non_reduced_vip_price,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_non_reduced_vip_price_activating, 0)) AS mtd_non_reduced_vip_price_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_non_reduced_vip_price_nonactivating, 0)) AS mtd_non_reduced_vip_price_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_non_reduced_vip_price_repeat, 0)) AS mtd_non_reduced_vip_price_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_non_reduced_vip_price_guest, 0)) AS mtd_non_reduced_vip_price_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_return_unit_count, 0)) AS mtd_total_return_unit_count,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_return_unit_count_activating, 0)) AS mtd_return_unit_count_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_return_unit_count_nonactivating, 0)) AS mtd_return_unit_count_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_return_unit_count_repeat, 0)) AS mtd_return_unit_count_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_return_unit_count_guest, 0)) AS mtd_return_unit_count_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_total_returned_product_gross_rev_excluding_shipping, 0)) AS mtd_total_returned_product_gross_rev_excluding_shipping,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_returned_product_gross_rev_excluding_shipping_activating, 0)) AS mtd_returned_product_gross_rev_excluding_shipping_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_returned_product_gross_rev_excluding_shipping_nonactivating, 0)) AS mtd_returned_product_gross_rev_excluding_shipping_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_returned_product_gross_rev_excluding_shipping_repeat, 0)) AS mtd_returned_product_gross_rev_excluding_shipping_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', mtd_returned_product_gross_rev_excluding_shipping_guest, 0)) AS mtd_returned_product_gross_rev_excluding_shipping_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_unit_count, ly_date_total_unit_count)) AS ly_total_unit_count,
    SUM(IFF(p.source = 'Weekly', ly_day_unit_count_activating, ly_date_unit_count_activating)) AS ly_unit_count_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_unit_count_nonactivating, ly_date_unit_count_nonactivating)) AS ly_unit_count_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_unit_count_repeat, ly_date_unit_count_repeat)) AS ly_unit_count_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_unit_count_guest, ly_date_unit_count_guest)) AS ly_unit_count_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_product_subtotal, ly_date_total_product_subtotal)) AS ly_total_product_subtotal,
    SUM(IFF(p.source = 'Weekly', ly_day_product_subtotal_activating, ly_date_product_subtotal_activating)) AS ly_product_subtotal_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_subtotal_nonactivating, ly_date_product_subtotal_nonactivating)) AS ly_product_subtotal_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_subtotal_repeat, ly_date_product_subtotal_repeat)) AS ly_product_subtotal_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_product_subtotal_guest, ly_date_product_subtotal_guest)) AS ly_product_subtotal_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_product_discount, ly_date_total_product_discount)) AS ly_total_product_discount,
    SUM(IFF(p.source = 'Weekly', ly_day_product_discount_activating, ly_date_product_discount_activating)) AS ly_product_discount_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_discount_nonactivating, ly_date_product_discount_nonactivating)) AS ly_product_discount_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_discount_repeat, ly_date_product_discount_repeat)) AS ly_product_discount_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_product_discount_guest, ly_date_product_discount_guest)) AS ly_product_discount_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_product_gross_revenue_excl_shipping, ly_date_total_product_gross_revenue_excl_shipping)) AS ly_total_product_gross_revenue_excl_shipping,
    SUM(IFF(p.source = 'Weekly', ly_day_product_gross_revenue_excl_shipping_activating, ly_date_product_gross_revenue_excl_shipping_activating)) AS ly_product_gross_revenue_excl_shipping_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_gross_revenue_excl_shipping_nonactivating, ly_date_product_gross_revenue_excl_shipping_nonactivating)) AS ly_product_gross_revenue_excl_shipping_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_gross_revenue_excl_shipping_repeat, ly_date_product_gross_revenue_excl_shipping_repeat)) AS ly_product_gross_revenue_excl_shipping_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_product_gross_revenue_excl_shipping_guest, ly_date_product_gross_revenue_excl_shipping_guest)) AS ly_product_gross_revenue_excl_shipping_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_product_margin_pre_return_excl_shipping, ly_date_total_product_margin_pre_return_excl_shipping)) AS ly_total_product_margin_pre_return_excl_shipping,
    SUM(IFF(p.source = 'Weekly', ly_day_product_margin_pre_return_excl_shipping_activating, ly_date_product_margin_pre_return_excl_shipping_activating)) AS ly_product_margin_pre_return_excl_shipping_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_margin_pre_return_excl_shipping_nonactivating, ly_date_product_margin_pre_return_excl_shipping_nonactivating)) AS ly_product_margin_pre_return_excl_shipping_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_margin_pre_return_excl_shipping_repeat, ly_date_product_margin_pre_return_excl_shipping_repeat)) AS ly_product_margin_pre_return_excl_shipping_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_product_margin_pre_return_excl_shipping_guest, ly_date_product_margin_pre_return_excl_shipping_guest)) AS ly_product_margin_pre_return_excl_shipping_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_product_cost, ly_date_total_product_cost)) AS ly_total_product_cost,
    SUM(IFF(p.source = 'Weekly', ly_day_product_cost_activating, ly_date_product_cost_activating)) AS ly_product_cost_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_cost_nonactivating, ly_date_product_cost_nonactivating)) AS ly_product_cost_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_product_cost_repeat, ly_date_product_cost_repeat)) AS ly_product_cost_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_product_cost_guest, ly_date_product_cost_guest)) AS ly_product_cost_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_product_cost_c, ly_date_total_product_cost_c)) AS ly_total_product_cost_c,
    SUM(IFF(p.source = 'Weekly', ly_day_product_cost_activating_c, ly_date_product_cost_activating_c)) AS ly_product_cost_activating_c,
    SUM(IFF(p.source = 'Weekly', ly_day_product_cost_nonactivating_c, ly_date_product_cost_nonactivating_c)) AS ly_product_cost_nonactivating_c,
    SUM(IFF(p.source = 'Weekly', ly_day_product_cost_repeat_c, ly_date_product_cost_repeat_c)) AS ly_product_cost_repeat_c,
    SUM(IFF(p.source = 'Weekly', ly_day_product_cost_guest_c, ly_date_product_cost_guest_c)) AS ly_product_cost_guest_c,
    SUM(IFF(p.source = 'Weekly', ly_day_total_non_reduced_vip_price, ly_date_total_non_reduced_vip_price)) AS ly_total_non_reduced_vip_price,
    SUM(IFF(p.source = 'Weekly', ly_day_non_reduced_vip_price_activating, ly_date_non_reduced_vip_price_activating)) AS ly_non_reduced_vip_price_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_non_reduced_vip_price_nonactivating, ly_date_non_reduced_vip_price_nonactivating)) AS ly_non_reduced_vip_price_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_non_reduced_vip_price_repeat, ly_date_non_reduced_vip_price_repeat)) AS ly_non_reduced_vip_price_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_non_reduced_vip_price_guest, ly_date_non_reduced_vip_price_guest)) AS ly_non_reduced_vip_price_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_return_unit_count, ly_date_total_return_unit_count)) AS ly_total_return_unit_count,
    SUM(IFF(p.source = 'Weekly', ly_day_return_unit_count_activating, ly_date_return_unit_count_activating)) AS ly_return_unit_count_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_return_unit_count_nonactivating, ly_date_return_unit_count_nonactivating)) AS ly_return_unit_count_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_return_unit_count_repeat, ly_date_return_unit_count_repeat)) AS ly_return_unit_count_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_return_unit_count_guest, ly_date_return_unit_count_guest)) AS ly_return_unit_count_guest,
    SUM(IFF(p.source = 'Weekly', ly_day_total_returned_product_gross_rev_excluding_shipping, ly_date_total_returned_product_gross_rev_excluding_shipping)) AS ly_total_returned_product_gross_rev_excluding_shipping,
    SUM(IFF(p.source = 'Weekly', ly_day_returned_product_gross_rev_excluding_shipping_activating, ly_date_returned_product_gross_rev_excluding_shipping_activating)) AS ly_returned_product_gross_rev_excluding_shipping_activating,
    SUM(IFF(p.source = 'Weekly', ly_day_returned_product_gross_rev_excluding_shipping_nonactivating, ly_date_returned_product_gross_rev_excluding_shipping_nonactivating)) AS ly_returned_product_gross_rev_excluding_shipping_nonactivating,
    SUM(IFF(p.source = 'Weekly', ly_day_returned_product_gross_rev_excluding_shipping_repeat, ly_date_returned_product_gross_rev_excluding_shipping_repeat)) AS ly_returned_product_gross_rev_excluding_shipping_repeat,
    SUM(IFF(p.source = 'Weekly', ly_day_returned_product_gross_rev_excluding_shipping_guest, ly_date_returned_product_gross_rev_excluding_shipping_guest)) AS ly_returned_product_gross_rev_excluding_shipping_guest,
    SUM(IFF(ips.date = p.start_date, IFF(p.source = 'Weekly', ly_day_bop_onhand_qty, ly_date_bop_onhand_qty), 0)) AS ly_bop_onhand_qty,
    SUM(IFF(ips.date = p.start_date, IFF(p.source = 'Weekly', ly_day_bop_available_to_sell_qty, ly_date_bop_available_to_sell_qty), 0)) AS ly_bop_available_to_sell_qty,
    SUM(IFF(ips.date = p.start_date, IFF(p.source = 'Weekly', ly_day_bop_open_to_buy_qty, ly_date_bop_open_to_buy_qty), 0)) AS ly_bop_open_to_buy_qty,
    SUM(IFF(ips.date = p.start_date, IFF(p.source = 'Weekly', ly_day_bop_open_to_buy_qty_incl_in_transit, ly_date_bop_open_to_buy_qty_incl_in_transit), 0)) AS ly_bop_open_to_buy_qty_incl_in_transit,
    SUM(IFF(ips.date = p.start_date, IFF(p.source = 'Weekly', ly_day_bop_retail_reserve, ly_date_bop_retail_reserve), 0)) AS ly_bop_retail_reserve,
    SUM(IFF(ips.date = p.start_date, IFF(p.source = 'Weekly', ly_day_bop_inventory_cost, ly_date_bop_inventory_cost), 0)) AS ly_bop_inventory_cost,
    SUM(IFF(ips.date = p.end_date, IFF(p.source = 'Weekly', ly_day_eop_onhand_qty, ly_date_eop_onhand_qty), 0)) AS ly_eop_onhand_qty,
    SUM(IFF(ips.date = p.end_date, IFF(p.source = 'Weekly', ly_day_eop_available_to_sell_qty, ly_date_eop_available_to_sell_qty), 0)) AS ly_eop_available_to_sell_qty,
    SUM(IFF(ips.date = p.end_date, IFF(p.source = 'Weekly', ly_day_eop_open_to_buy_qty, ly_date_eop_open_to_buy_qty), 0)) AS ly_eop_open_to_buy_qty,
    SUM(IFF(ips.date = p.end_date, IFF(p.source = 'Weekly', ly_day_eop_open_to_buy_qty_incl_in_transit, ly_date_eop_open_to_buy_qty_incl_in_transit), 0)) AS ly_eop_open_to_buy_qty_incl_in_transit,
    SUM(IFF(ips.date = p.end_date, IFF(p.source = 'Weekly', ly_day_eop_retail_reserve, ly_date_eop_retail_reserve), 0)) AS ly_eop_retail_reserve,
    SUM(IFF(ips.date = p.end_date, IFF(p.source = 'Weekly', ly_day_eop_inventory_cost, ly_date_eop_inventory_cost), 0)) AS ly_eop_inventory_cost,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_unit_count, 0)) AS lymtd_total_unit_count,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_unit_count_activating, 0)) AS lymtd_unit_count_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_unit_count_nonactivating, 0)) AS lymtd_unit_count_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_unit_count_repeat, 0)) AS lymtd_unit_count_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_unit_count_guest, 0)) AS lymtd_unit_count_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_product_subtotal, 0)) AS lymtd_total_product_subtotal,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_subtotal_activating, 0)) AS lymtd_product_subtotal_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_subtotal_nonactivating, 0)) AS lymtd_product_subtotal_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_subtotal_repeat, 0)) AS lymtd_product_subtotal_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_subtotal_guest, 0)) AS lymtd_product_subtotal_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_product_discount, 0)) AS lymtd_total_product_discount,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_discount_activating, 0)) AS lymtd_product_discount_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_discount_nonactivating, 0)) AS lymtd_product_discount_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_discount_repeat, 0)) AS lymtd_product_discount_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_discount_guest, 0)) AS lymtd_product_discount_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_product_gross_revenue_excl_shipping, 0)) AS lymtd_total_product_gross_revenue_excl_shipping,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_gross_revenue_excl_shipping_activating, 0)) AS lymtd_product_gross_revenue_excl_shipping_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_gross_revenue_excl_shipping_nonactivating, 0)) AS lymtd_product_gross_revenue_excl_shipping_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_gross_revenue_excl_shipping_repeat, 0)) AS lymtd_product_gross_revenue_excl_shipping_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_gross_revenue_excl_shipping_guest, 0)) AS lymtd_product_gross_revenue_excl_shipping_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_product_margin_pre_return_excl_shipping, 0)) AS lymtd_total_product_margin_pre_return_excl_shipping,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_margin_pre_return_excl_shipping_activating, 0)) AS lymtd_product_margin_pre_return_excl_shipping_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_margin_pre_return_excl_shipping_nonactivating, 0)) AS lymtd_product_margin_pre_return_excl_shipping_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_margin_pre_return_excl_shipping_repeat, 0)) AS lymtd_product_margin_pre_return_excl_shipping_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_margin_pre_return_excl_shipping_guest, 0)) AS lymtd_product_margin_pre_return_excl_shipping_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_product_cost, 0)) AS lymtd_total_product_cost,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_cost_activating, 0)) AS lymtd_product_cost_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_cost_nonactivating, 0)) AS lymtd_product_cost_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_cost_repeat, 0)) AS lymtd_product_cost_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_cost_guest, 0)) AS lymtd_product_cost_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_product_cost_c, 0)) AS lymtd_total_product_cost_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_cost_activating_c, 0)) AS lymtd_product_cost_activating_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_cost_nonactivating_c, 0)) AS lymtd_product_cost_nonactivating_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_cost_repeat_c, 0)) AS lymtd_product_cost_repeat_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_product_cost_guest_c, 0)) AS lymtd_product_cost_guest_c,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_non_reduced_vip_price, 0)) AS lymtd_total_non_reduced_vip_price,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_non_reduced_vip_price_activating, 0)) AS lymtd_non_reduced_vip_price_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_non_reduced_vip_price_nonactivating, 0)) AS lymtd_non_reduced_vip_price_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_non_reduced_vip_price_repeat, 0)) AS lymtd_non_reduced_vip_price_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_non_reduced_vip_price_guest, 0)) AS lymtd_non_reduced_vip_price_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_return_unit_count, 0)) AS lymtd_total_return_unit_count,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_return_unit_count_activating, 0)) AS lymtd_return_unit_count_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_return_unit_count_nonactivating, 0)) AS lymtd_return_unit_count_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_return_unit_count_repeat, 0)) AS lymtd_return_unit_count_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_return_unit_count_guest, 0)) AS lymtd_return_unit_count_guest,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_total_returned_product_gross_rev_excluding_shipping, 0)) AS lymtd_total_returned_product_gross_rev_excluding_shipping,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_returned_product_gross_rev_excluding_shipping_activating, 0)) AS lymtd_returned_product_gross_rev_excluding_shipping_activating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_returned_product_gross_rev_excluding_shipping_nonactivating, 0)) AS lymtd_returned_product_gross_rev_excluding_shipping_nonactivating,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_returned_product_gross_rev_excluding_shipping_repeat, 0)) AS lymtd_returned_product_gross_rev_excluding_shipping_repeat,
    SUM(IFF(ips.date = p.end_date AND p.source = 'Weekly', lymtd_day_returned_product_gross_rev_excluding_shipping_guest, 0)) AS lymtd_returned_product_gross_rev_excluding_shipping_guest,
    $meta_create_datetime AS meta_create_datetime,
    $meta_update_datetime AS meta_update_datetime
FROM reporting.merch_planning_item_product_sku ips
JOIN _period p ON p.full_date = ips.date
LEFT JOIN _product_sku ps ON ps.product_sku = ips.product_sku
    AND ps.product_brand = ips.product_brand
    AND ps.store_brand = ips.store_brand
    AND ps.country = ips.country
WHERE (
    p.start_date >= DATE_TRUNC('MONTH', DATEADD(MONTH, -13, CURRENT_DATE)) -- ROLLING 12 MONTHS
    OR
    (p.source = 'Yearly' AND p.start_date >= DATE_TRUNC('YEAR', DATEADD(MONTH, -1, CURRENT_DATE))) -- INCLUDES YEARLY DATA REGARDLESS
    )
    AND ips.store_brand IN ('FABLETICS', 'SAVAGE X', 'YITTY')
GROUP BY p.start_date,
    p.source,
    date_type,
    store_id,
    store_name,
    store_full_name,
    ips.store_brand,
    store_type,
    ips.country,
    finance_specialty_store,
    region,
    ips.product_brand,
    ips.product_sku,
    ps.wms_class,
    base_sku,
    is_retail;

COMMIT;

UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM reporting.merch_planning_item_final_output)),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
