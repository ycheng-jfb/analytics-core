SELECT
    MONTH_DATE,
    FOLE_DISCOUNT,
    FSO_DISCOUNT,
    FOLE_REVENUE,
    FSO_REVENUE,
    FOLE_MARGIN,
    FSO_MARGIN,
    FOLE_COST,
    FSO_COST,
    FOLE_SUBTOTAL,
    FSO_SUBTOTAL,
    DISC_VAR,
    REV_VAR,
    MARGIN_VAR,
    COST_VAR,
    SUBTOTAL_VAR
FROM
(
    SELECT month_date,
    SUM(FOLE_DISCOUNT)                                             AS FOLE_DISCOUNT,
    SUM(FSO_DISCOUNT)                                              AS FSO_DISCOUNT,
    SUM(FOLE_REVENUE)                                              AS FOLE_REVENUE,
    SUM(FSO_REVENUE)                                               AS FSO_REVENUE,
    SUM(FOLE_MARGIN)                                               AS FOLE_MARGIN,
    SUM(FSO_MARGIN)                                                AS FSO_MARGIN,
    SUM(FOLE_COST)                                                 AS FOLE_COST,
    SUM(FSO_COST)                                                  AS FSO_COST,
    SUM(FOLE_SUBTOTAL)                                             AS FOLE_SUBTOTAL,
    SUM(FSO_SUBTOTAL)                                              AS FSO_SUBTOTAL,
    ROUND(((SUM(FOLE_DISCOUNT) / SUM(FSO_DISCOUNT)) - 1) * 100, 2) AS DISC_VAR,
    ROUND(((SUM(FOLE_REVENUE) / SUM(FSO_REVENUE)) - 1) * 100, 2)   AS REV_VAR,
    ROUND(((SUM(FOLE_MARGIN) / SUM(FSO_MARGIN)) - 1) * 100, 2)     AS MARGIN_VAR,
    ROUND(((SUM(FOLE_COST) / SUM(FSO_COST)) - 1) * 100, 2)         AS COST_VAR,
    ROUND(((SUM(FOLE_SUBTOTAL) / SUM(FSO_SUBTOTAL)) - 1) * 100, 2) AS SUBTOTAL_VAR
    FROM (
        SELECT DATE_TRUNC('month', order_date)                  AS month_date,
            SUM(product_discount)                               AS FOLE_DISCOUNT,
            0                                                   AS FSO_DISCOUNT,
            SUM(product_gross_revenue_excl_shipping_amount)     AS FOLE_REVENUE,
            0                                                   AS FSO_REVENUE,
            SUM(product_margin_pre_return_excl_shipping_amount) AS FOLE_MARGIN,
            0                                                   AS FSO_MARGIN,
            SUM(reporting_landed_cost_amount)                   AS FOLE_COST,
            0                                                   AS FSO_COST,
            SUM(subtotal_excl_tariff)                           AS FOLE_SUBTOTAL,
            0                                                   AS FSO_SUBTOTAL
        FROM REPORTING_BASE_PROD.FABLETICS.FACT_ORDER_LINE_EXTENDED_NEW fole
        where 1 = 1
            AND order_date::date >= (current_date - INTERVAL '12 months')
            and order_date::date < current_date::date
        group by 1
        UNION
        SELECT
            DATE_TRUNC('month', date)                      AS month_date,
            0                                              AS FOLE_DISCOUNT,
            SUM(product_order_product_discount_amount)     AS FSO_DISCOUNT,
            0                                              AS FOLE_REVENUE,
            SUM(PRODUCT_GROSS_REVENUE_EXCL_SHIPPING)       AS FSO_REVENUE,
            0                                              AS FOLE_MARGIN,
            SUM(product_margin_pre_return_excl_shipping)   AS FSO_MARGIN,
            0                                              AS FOLE_COST,
            SUM(product_order_landed_product_cost_amount)  AS FSO_COST,
            0                                              AS FOLE_SUBTOTAL,
            SUM(product_order_subtotal_excl_tariff_amount) AS FSO_SUBTOTAL
        FROM EDW_PROD.ANALYTICS_BASE.FINANCE_SALES_OPS fso
        JOIN EDW_PROD.DATA_MODEL_FL.DIM_STORE st ON st.store_id = fso.store_id
        WHERE LOWER(date_object) = 'placed'
            AND LOWER(currency_object) = 'usd'
            AND UPPER(store_brand) in ('FABLETICS', 'YITTY')
            AND date::date >= (CURRENT_DATE - INTERVAL '12 months')
            AND date::date < CURRENT_DATE::date
        GROUP BY 1
    )
    GROUP BY 1
) a

WHERE
    ABS(DISC_VAR) >= 1 OR
    ABS(REV_VAR) >= 1 OR
    ABS(MARGIN_VAR) >= 1 OR
    ABS(COST_VAR) >= 1 OR
    ABS(SUBTOTAL_VAR) >= 1;
