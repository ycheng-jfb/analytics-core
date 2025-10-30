WITH fact_inventory_in_transit AS (
    -- 原始在途库存逻辑
    SELECT
        od.fromPhysicalWarehouseCode AS from_warehouse_id,
        od.toPhysicalWarehouseCode AS to_warehouse_id,
        od.sku,
        CAST(CURRENT_DATE AS TIMESTAMP) AS rollup_date, -- 当前日期作为快照时间
        (od.expectedInboundQuantity - od.actualInboundQuantity) AS units
    FROM lake.mmt.ODS_IMS_T_ON_WAY_INFO_DETAIL_DF od
    WHERE od.del_flag = 0 -- 可选的软删除标志
),

-- 按天聚合并生成拉链表
fact_inventory_in_transit_scdb AS (
    SELECT
        rollup_date,
        sku,
        from_warehouse_id,
        SUM(units) AS total_units
    FROM fact_inventory_in_transit
    GROUP BY
        rollup_date,
        sku,
        from_warehouse_id
)

SELECT
    rollup_date,
    NULL AS item_id, -- 如果没有 item_id，可留空或通过 SKU 映射获取
    sku,
    from_warehouse_id,
    total_units,
    rollup_date::TIMESTAMP_NTZ(3) AS effective_from_date,
    LEAD(DATEADD(MILLISECOND, -1, rollup_date), 1, '9999-12-31') OVER (
        PARTITION BY sku, from_warehouse_id
        ORDER BY rollup_date
    )::TIMESTAMP_NTZ(3) AS effective_to_date,
    CASE
        WHEN LEAD(rollup_date, 1, '9999-12-31') OVER (
            PARTITION BY sku, from_warehouse_id
            ORDER BY rollup_date
        ) = '9999-12-31'
        THEN TRUE
        ELSE FALSE
    END AS is_current
FROM fact_inventory_in_transit_scdb;
