CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.SXF.TOP_SELLERS_W_INVENTORY
(
        fixed_date,
        inventory_region,
        order_hq_date,
        is_activating,
        hdyh,
        membership_type_time_of_order,
        is_ecomm,
        store_region_abbr,
        store_country_abbr,
        order_store_full_name,
        store_warehouse_region,
        order_store_type,
        promo,
        promo_code,
        customer_gender,
        sizes_in_stock,
        sizes_offered,
        color_sku,
        is_marked_down,
        is_lead_only,
        base_sku,
        style_number,
        site_name,
        site_color,
        po_color,
        color_family,
        fabric,
        category,
        subcategory,
        gender,
        size_range,
        size_scale,
        core_fashion,
        persona,
        collection,
        first_showroom,
        most_recent_reorder_showroom,
        savage_showroom,
        inv_aged_days,
        inv_aged_months,
        inv_aged_status,
        planned_site_release_date,
        image_url_dp,
        first_sales_date,
        is_pre_made_set,
        is_byo_set,
        is_vip_box,
        is_bundle,
        is_individual_item,
        color_roll_up,
        fabric_grouping,
        department,
        sub_department,
        is_prepack,
        max_order_hq_datetime,
        latest_po_showroom,
        po_qty,
        current_inv_last_update_datetime,
        vip_price,
        euro_vip,
        gbp_vip,
        msrp,
        euro_msrp,
        gbp_msrp,
        new_core_fashion,
        current_vip_price_on_site_na,
        current_msrp_price_on_site_na,
        item_quantity_o_line,
        return_item_quantity_o_line,
        product_subtotal_amount_o_line,
        tax_amount_o_line,
        discount_amount_o_line,
        shipping_revenue_amount_o_line,
        recognized_revenue_amount_o_line,
        revenue_incl_shippingrev_o_line,
        revenue_excl_shippingrev_o_line,
        estimated_total_cost_o_line,
        reporting_landed_cost_o_line,
        gross_product_margin_dollar_o_line,
        gross_margin_dollar_o_line,
        qty_onhand,
        qty_replen,
        qty_ghost,
        qty_reserve,
        qty_special_pick_reserve,
        qty_manual_stock_reserve,
        qty_open_to_sell,
        qty_available_to_sell,
        eop_qty_onhand,
        eop_qty_replen,
        eop_qty_ghost,
        eop_qty_reserve,
        eop_qty_special_pick_reserve,
        eop_qty_manual_stock_reserve,
        eop_qty_open_to_sell,
        eop_qty_available_to_sell
) AS

-- calc max inventory datetime per sku
WITH currentinventorytime AS
(
    SELECT product_sku,
        MAX(datetime) AS last_update_datetime
    FROM reporting_prod.sxf.view_base_inventory_dataset i
    WHERE is_current = 'Y'
    GROUP BY product_sku
)

-- listagg all promos used per order line
,  _promo AS
(
    SELECT d.order_line_id,
                       LISTAGG(DISTINCT pr.code, ' | ') WITHIN GROUP (ORDER BY pr.code) AS promo_codes
    FROM lake_sxf_view.ultra_merchant.order_line_discount d
    LEFT JOIN lake_sxf_view.ultra_merchant.promo pr ON (pr.promo_id = d.promo_id)
    GROUP BY d.order_line_id
)


-- marked down history based on sale FPL from Console. if sku exists in view_markdown_history on date, it was on markdown
, _marked_down_history AS
(
    SELECT DISTINCT color_sku_po,
        full_date,
        store_country,
        CASE
            WHEN st.store_country = 'UK' AND full_date >= '2021-03-18'
            THEN 'EU-UK' -- started shipping UK store orders from UK warehouse on 03/18/21
            ELSE st.store_region
        END AS region
    FROM reporting_prod.sxf.view_markdown_history m
    JOIN edw_prod.data_model_sxf.dim_store st ON st.store_id = m.store_id
)

-- lead only tag from Console. if sku exists in view_lead_only_tag_history on date, it had lead only tag
, _lead_only_history AS
(
    SELECT DISTINCT lo.PRODUCT_SKU as color_sku_po,
        lo.full_date,
        lo.store_country,
        CASE
            WHEN lo.store_country = 'UK' AND lo.full_date >= '2021-03-18'
            THEN 'EU-UK' -- started shipping UK store orders from UK warehouse on 03/18/21
            ELSE lo.store_region
        END AS region
    FROM REPORTING_PROD.SXF.VIEW_LEAD_ONLY_TAG_HISTORY lo
)



, _base_bop_inventory_dataset AS
(
    SELECT date,
        region,
        color_sku_po,
        CASE
            WHEN is_retail = 'N' THEN 'Online'
            WHEN is_retail = 'Y' THEN 'Retail'
        END AS store_type,
        CASE
            WHEN is_retail = 'Y' THEN warehouse
            ELSE region
        END AS store_warehouse_region,
        SUM(i.qty_onhand)                                        AS qty_onhand,
        SUM(i.qty_replen)                                        AS qty_replen,
        SUM(i.qty_ghost)                                         AS qty_ghost,
        SUM(i.qty_reserve)                                       AS qty_reserve,
        SUM(i.qty_special_pick_reserve)                          AS qty_special_pick_reserve,
        SUM(i.qty_manual_stock_reserve)                          AS qty_manual_stock_reserve,
        SUM(i.qty_open_to_sell)                                  AS qty_open_to_sell,
        SUM(i.qty_available_to_sell)                             AS qty_available_to_sell
    FROM reporting_prod.sxf.view_base_inventory_dataset i
    GROUP BY date, region, color_sku_po, store_type, store_warehouse_region
)

-- EOP = to calc inventory at end of date, use inventory at beginning of next day
-- if inventory is pulled mid-day, EOP = 0 because it is not end of day so there is no EOP yet / no BOP next day

, _base_eop_inventory_dataset AS
(
    SELECT DATEADD(DAY, -1, date) eop_date,
        region,
        color_sku_po,
        store_type,
        store_warehouse_region,
        qty_onhand,
        qty_replen,
        qty_ghost,
        qty_reserve,
        qty_special_pick_reserve,
        qty_manual_stock_reserve,
        qty_open_to_sell,
        qty_available_to_sell
    FROM _base_bop_inventory_dataset
)

, _base_inventory_dataset AS
(
    SELECT bop.date,
        bop.region,
        bop.color_sku_po,
        bop.store_type,
        bop.store_warehouse_region,
        bop.qty_onhand,
        bop.qty_replen,
        bop.qty_ghost,
        bop.qty_reserve,
        bop.qty_special_pick_reserve,
        bop.qty_manual_stock_reserve,
        bop.qty_open_to_sell,
        bop.qty_available_to_sell,
        eop.qty_onhand               eop_qty_onhand,
        eop.qty_replen               eop_qty_replen,
        eop.qty_ghost                eop_qty_ghost,
        eop.qty_reserve              eop_qty_reserve,
        eop.qty_special_pick_reserve eop_qty_special_pick_reserve,
        eop.qty_manual_stock_reserve eop_qty_manual_stock_reserve,
        eop.qty_open_to_sell         eop_qty_open_to_sell,
        eop.qty_available_to_sell    eop_qty_available_to_sell
    FROM _base_bop_inventory_dataset bop
    LEFT JOIN _base_eop_inventory_dataset eop ON bop.color_sku_po = eop.color_sku_po
        AND bop.store_type = eop.store_type
        AND bop.store_warehouse_region = eop.store_warehouse_region
        AND bop.region = eop.region
        AND bop.date = eop.eop_date
)

, _on_order AS
(
    SELECT color_sku_po,
        region,
        wh_type,
        po_showroom AS latest_po_showroom,
        SUM(po_qty) AS po_qty
     FROM reporting_prod.sxf.view_on_order
     WHERE po_showroom_rank = 1 -- only bringing in the latest showroom for each region and wh_type
     GROUP BY 1, 2, 3, 4
)

, _order_data AS
(
    SELECT s.order_hq_date::DATE                order_hq_date,
        s.store_region_abbr,
        s.is_activating,
        s.how_did_you_hear                AS hdyh,
        s.membership_type_time_of_order,
        s.is_ecomm,
        s.store_country,
        s.order_store_full_name,
        CASE
            WHEN s.order_store_type = 'Retail' THEN UPPER(order_store_full_name)
            ELSE s.store_region_abbr
        END AS store_warehouse_region,
        s.order_store_type,
        s.promo,
        s.product_sku                     AS color_sku,
        s.base_sku,
        is_pre_made_set,
        is_byo_set,
        is_vip_box,
        is_bundle,
        is_individual_item,
        pr.promo_codes,
        ga.gender as customer_gender,
        SUM(item_quantity)                AS item_quantity,
        SUM(return_item_quantity)         AS return_item_quantity,
        SUM(product_subtotal_amount)      AS product_subtotal_amount, -- includes tariff
        SUM(tax_amount)                   AS tax_amount,
        SUM(product_discount_amount)      AS product_discount_amount,
        SUM(shipping_revenue_amount)      AS shipping_revenue_amount,
        SUM(product_gross_revenue_amount) AS product_gross_revenue_amount,
        SUM(revenue_incl_shippingrev)     AS revenue_incl_shippingrev,
        SUM(product_gross_revenue_excl_shipping_amount)     AS revenue_excl_shippingrev,
        SUM(estimated_landed_cost)        AS estimated_landed_cost,   -- Actuals after 2020 orders
        SUM(REPORTING_LANDED_COST)        AS reporting_landed_cost,
        SUM(gross_product_margin)         AS gross_product_margin,    -- in OLD gross_product_margin = (Product_Gross_Revenue_Excl_Shipping_Amount - reporting_landed_cost)
        SUM(gross_margin)                 AS gross_margin             -- in OLD gross_margin = (Product_Gross_Revenue_Amount- reporting_landed_cost)
     FROM reporting_prod.sxf.view_order_line_recognized_dataset s
     LEFT JOIN _promo pr ON s.order_line_id = pr.order_line_id
     LEFT JOIN REPORTING_PROD.SXF.VIEW_SSN_GENDER_ASSIGNMENT ga on ga.customer_id = s.customer_id  -- bring in customer gender, based on ssn data
     GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
)

, _savage_stores AS
(
    SELECT store_type,
        store_region,
        TRIM(store_country) store_country,
        TRIM(store_full_name) store_full_name,
    FROM edw_prod.data_model_sxf.dim_store ds
    WHERE ds.store_brand = 'Savage X'
        AND store_full_name NOT ILIKE '%(DM)%'
        AND store_country NOT IN ('CA', 'SE', 'NL', 'DK')
        AND store_full_name NOT ILIKE 'Savage Test Store 804'
)

, _base_inv_store AS
(
    SELECT i.date,
        i.region,
        i.color_sku_po,
        i.store_type,
        i.store_warehouse_region,
        COALESCE(ds.store_full_name, ds1.store_full_name, ds2.store_full_name) store_full_name,
        COALESCE(ds.store_country, ds1.store_country, ds2.store_country) store_country,
        qty_onhand,
        qty_replen,
        qty_ghost,
        qty_reserve,
        qty_special_pick_reserve,
        qty_manual_stock_reserve,
        qty_open_to_sell,
        qty_available_to_sell,
        eop_qty_onhand,
        eop_qty_replen,
        eop_qty_ghost,
        eop_qty_reserve,
        eop_qty_special_pick_reserve,
        eop_qty_manual_stock_reserve,
        eop_qty_open_to_sell,
        eop_qty_available_to_sell
    FROM _base_inventory_dataset i
    LEFT JOIN _savage_stores ds ON CASE
                                        WHEN i.region = 'EU' AND i.date < '2021-03-11' THEN 'EU'
                                        ELSE NULL END = ds.store_region --EU with all counries from EU including UK
    LEFT JOIN _savage_stores ds1 ON CASE
                                        WHEN i.region = 'EU' AND i.date >= '2021-03-11' THEN 'EU'
                                        ELSE NULL END = ds1.store_region AND ds1.store_country <> 'UK' --EU with all counries from EU except UK
    LEFT JOIN _savage_stores ds2 ON CASE
                                        WHEN i.store_warehouse_region = 'EU-UK' THEN 'SAVAGE X UK'
                                        WHEN i.store_warehouse_region = 'NA' THEN 'SAVAGE X US'
                                        WHEN i.store_type = 'Retail' THEN i.store_warehouse_region
                                        ELSE NULL END = UPPER(ds2.store_full_name)
    WHERE CASE
            WHEN store_warehouse_region = 'RTLSXF-VALLEY FAIR' AND (DATE < '2018-10-27' OR DATE > '2019-01-22') THEN 0
            WHEN store_warehouse_region = 'RTLSXF-SOHO' AND (DATE < '2018-09-11' OR DATE > '2019-09-12') THEN 0
            WHEN store_warehouse_region = 'RTLSXF-MALL OF AMERICA' AND (DATE < '2018-08-30' OR DATE > '2019-01-23') THEN 0
            WHEN store_warehouse_region = 'RTLSXF-LAS VEGAS' AND (DATE < '2018-10-27' OR DATE > '2019-01-22') THEN 0
            ELSE 1 END = 1
)


SELECT DISTINCT COALESCE(i.date, S.order_hq_date::DATE)         AS fixed_date,
    COALESCE(i.region, S.store_region_abbr)                     AS inventory_region,
    s.order_hq_date,
    s.is_activating,
    s.hdyh                                                      AS hdyh,
    s.membership_type_time_of_order,
    s.is_ecomm,
    COALESCE(i.region, s.store_region_abbr)                     AS store_region_abbr,
    COALESCE(i.store_country,s.store_country)                   AS store_country_abbr,
    COALESCE(i.store_full_name,s.order_store_full_name)         AS order_store_full_name,
    COALESCE(i.store_warehouse_region,s.store_warehouse_region) AS store_warehouse_region,
    COALESCE(i.store_type, s.Order_Store_Type)                  AS Order_Store_Type,
    s.promo,
    s.promo_codes                           AS promo_code,
    s.customer_gender,
    sz.sizes_in_stock,
    sz.sizes_offered,
    COALESCE(i.color_sku_po, s.color_sku)   AS color_sku,
    CASE
        WHEN mdh.color_sku_po IS NOT NULL THEN 'Markdown'
        ELSE 'Full Price'
    END is_marked_down,

    CASE
        WHEN loh.color_sku_po IS NOT NULL THEN 'Lead Only'
    END is_lead_only,

    s.base_sku,
    sm.style_number_po                      AS style_number,
    sm.site_name,
    sm.site_color,
    sm.po_color,
    sm.color_family,
    sm.fabric,
    sm.category,
    sm.subcategory,
    sm.gender,
    sm.size_range,
    sm.size_scale,
    sm.core_fashion,
    sm.persona,
    sm.collection,
    sm.first_showroom,
    sm.latest_showroom                      AS most_recent_reorder_showroom,
    sm.savage_showroom,
    sm.inv_aged_days,
    sm.inv_aged_months,
    sm.inv_aged_status,
    sm.planned_site_release_date,
    sm.image_url                            AS image_url_dp,
    sm.first_sales_date,
    is_pre_made_set,
    is_byo_set,
    is_vip_box,
    is_bundle,
    is_individual_item,
    color_roll_up,
    fabric_grouping,
    sm.department,
    sm.sub_department,
    sm.is_prepack,
    (SELECT MAX(order_hq_datetime)
     FROM reporting_prod.sxf.view_order_line_recognized_dataset) max_order_hq_datetime,
    latest_po_showroom,                 -- adding the latest_po_showroom for each color style with its qty
    zeroifnull(oo.po_qty) AS  po_qty,   -- bringing in the PO qty from view_on_order but be careful since its copied across SKUs
    dt.last_update_datetime AS current_inv_last_update_datetime,
    sm.vip_price,  -- bringing in vip and msrp price
    sm.euro_vip,
    sm.gbp_vip,
    sm.msrp,
    sm.euro_msrp,
    sm.gbp_msrp,
    sm.new_core_fashion,  -- new
    sm.current_vip_price_on_site_na,
    sm.current_retail_price_on_site_na                    AS                   current_msrp_price_on_site_na,
    ZEROIFNULL(s.item_quantity)                           AS                   item_quantity_o_line,
    ZEROIFNULL(s.return_item_quantity)                    AS                   return_item_quantity_o_line,
    ZEROIFNULL(s.product_subtotal_amount)                 AS                   product_subtotal_amount_o_line,     -- includes tariff
    ZEROIFNULL(s.tax_amount)                              AS                   tax_amount_o_line,
    ZEROIFNULL(s.product_discount_amount)                 AS                   discount_amount_o_line,
    ZEROIFNULL(s.shipping_revenue_amount)                 AS                   shipping_revenue_amount_o_line,
    ZEROIFNULL(s.product_gross_revenue_Amount)            AS                   recognized_revenue_amount_o_line,
    ZEROIFNULL(s.revenue_incl_shippingrev)                AS                   revenue_incl_shippingrev_o_line,
    ZEROIFNULL(s.revenue_excl_shippingrev)                AS                   revenue_excl_shippingrev_o_line,
    ZEROIFNULL(s.estimated_landed_cost)                   AS                   estimated_total_cost_o_line,        -- Actuals after 2020 orders
    ZEROIFNULL(s.reporting_landed_cost)                   AS                   reporting_landed_cost_o_line,
    ZEROIFNULL(s.gross_product_margin)                    AS                   gross_product_margin_dollar_o_line,      -- in OLD gross_product_margin = (Product_Gross_Revenue_Excl_Shipping_Amount - reporting_landed_cost)
    ZEROIFNULL(s.gross_margin)                            AS                   gross_margin_dollar_o_line,              -- in OLD gross_margin = (Product_Gross_Revenue_Amount- reporting_landed_cost)
    ZEROIFNULL(i.qty_onhand)                              AS                   qty_onhand,
    ZEROIFNULL(i.qty_replen)                              AS                   qty_replen,
    ZEROIFNULL(i.qty_ghost)                               AS                   qty_ghost,
    ZEROIFNULL(i.qty_reserve)                             AS                   qty_reserve,
    ZEROIFNULL(i.qty_special_pick_reserve)                AS                   qty_special_pick_reserve,
    ZEROIFNULL(i.qty_manual_stock_reserve)                AS                   qty_manual_stock_reserve,
    ZEROIFNULL(i.qty_open_to_sell)                        AS                   qty_open_to_sell,
    ZEROIFNULL(i.qty_available_to_sell)                   AS                   qty_available_to_sell,
    ZEROIFNULL(i.eop_qty_onhand)                          AS                   eop_qty_onhand,
    ZEROIFNULL(i.eop_qty_replen)                          AS                   eop_qty_replen,
    ZEROIFNULL(i.eop_qty_ghost)                           AS                   eop_qty_ghost,
    ZEROIFNULL(i.eop_qty_reserve)                         AS                   eop_qty_reserve,
    ZEROIFNULL(i.eop_qty_special_pick_reserve)            AS                   eop_qty_special_pick_reserve,
    ZEROIFNULL(i.eop_qty_manual_stock_reserve)            AS                   eop_qty_manual_stock_reserve,
    ZEROIFNULL(i.eop_qty_open_to_sell)                    AS                   eop_qty_open_to_sell,
    ZEROIFNULL(i.eop_qty_available_to_sell) AS eop_qty_available_to_sell
FROM _base_inv_store i
FULL JOIN _order_data AS s ON s.color_sku = i.color_sku_po
    AND LEFT(s.order_hq_date, 10) = i.date
    AND i.region = s.store_region_abbr
    AND s.order_store_type = i.store_type
    AND i.store_warehouse_region = CASE WHEN s.order_store_type = 'Retail' THEN UPPER(order_store_full_name) ELSE region END
    AND s.order_store_full_name = i.store_full_name
LEFT JOIN reporting_prod.sxf.style_master AS sm ON sm.color_sku_po = COALESCE(i.color_sku_po, s.color_sku)
  -- new join for po_qty
LEFT JOIN _on_order oo ON oo.color_sku_po = COALESCE(i.color_sku_po, s.color_sku)
    AND oo.region = COALESCE(i.region, s.store_region_abbr)
    AND oo.wh_type = COALESCE(i.store_type,s.order_store_type)
  -- if sku exists in markdown history on dates, then was marked down
LEFT JOIN _marked_down_history mdh ON mdh.color_sku_po = COALESCE(i.color_sku_po, s.color_sku)
    AND mdh.store_country = COALESCE(i.store_country,s.store_country)
    AND mdh.full_date = COALESCE(i.date, s.order_hq_date::date)
  -- if sku exists in lead only history on dates, then was lead only
LEFT JOIN _lead_only_history loh ON loh.color_sku_po = COALESCE(i.color_sku_po, s.color_sku)
    AND loh.store_country = COALESCE(i.store_country,s.store_country)
    AND loh.full_date = COALESCE(i.date, s.order_hq_date::date)
  -- max inventory datetime
LEFT JOIN currentinventorytime dt ON (COALESCE(i.color_sku_po, s.color_sku) = dt.product_sku)
  -- sizes in stock and sizes offered
LEFT JOIN reporting_prod.sxf.view_sizesofferedvsinstock_daily AS sz ON sz.color_sku_po = i.color_sku_po
    AND sz.date = i.date
    AND i.region = sz.region
    AND sz.store_type = i.store_type
    AND i.store_warehouse_region = sz.store_warehouse_region

WHERE fixed_date >= '2022-01-01'
AND CASE WHEN i.region = 'EU-UK' AND i.date < '2021-03-11' THEN False
          WHEN i.date < '2019-01-01' THEN False
          ELSE True
          END
;
