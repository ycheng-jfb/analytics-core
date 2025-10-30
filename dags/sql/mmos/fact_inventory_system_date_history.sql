WITH
-- ================== 构建 v_inv_snapshot 逻辑 ==================
v_inv_snapshot AS (
    SELECT
        t.physicalWarehouseCode,
        t.warehouseName,
        t.channelId,
        c.channelName,
        c.invFunctions,
        t.sku,
        t.availableQty,
        t.damagedQty,
        CASE WHEN find_in_set('transit', c.invFunctions) > 0 THEN t.availableQty ELSE 0 END AS replenishQty,
        CASE WHEN find_in_set('platformFulfill', c.invFunctions) > 0 OR find_in_set('selfShipped', c.invFunctions) > 0 THEN t.availableQty ELSE 0 END AS sellableQty,
        CASE WHEN find_in_set('transit', c.invFunctions) > 0 THEN t.damagedQty ELSE 0 END AS damagedReplenishQty,
        CASE WHEN find_in_set('platformFulfill', c.invFunctions) > 0 OR find_in_set('selfShipped', c.invFunctions) > 0 THEN t.damagedQty ELSE 0 END AS damagedSellableQty,
        t.lockQty,
        t.availableLockQty,
        t.damagedLockQty,
        t.style AS style_name,
        t.color AS color_name,
        t.size AS size_name
    FROM (
        -- Amazon 渠道库存
        SELECT

            ai.physicalWarehouseCode,
            w.warehouseName,
            ai.channelId,
            ai.sku,
            SUM(ai.availableQty) AS availableQty,
            SUM(ai.damagedQty) AS damagedQty,
            SUM(ai.lockQty) AS lockQty,
            0 AS availableLockQty,
            0 AS damagedLockQty,
            ai.style,
            ai.color,
            ai.size
        FROM ims.t_amazon_channel_real_inv ai
        INNER JOIN ims.t_warehouse w ON ai.physicalWarehouseCode = w.logicWarehouseCode
        GROUP BY  ai.physicalWarehouseCode, w.warehouseName, ai.channelId, ai.sku, ai.style, ai.color, ai.size

        UNION ALL

        -- 其他渠道库存
        SELECT

            ai.warehouseCode AS physicalWarehouseCode,
            w.warehouseName,
            ai.channelId,
            ai.sku,
            ai.availableQty,
            ai.damagedQty,
            ai.lockQty,
            ai.availableLockQty,
            ai.damagedLockQty,
            ai.style,
            ai.color,
            ai.size
        FROM ims.t_channel_logic_inv ai
        INNER JOIN ims.t_warehouse w ON ai.warehouseCode = w.logicWarehouseCode
        WHERE w.warehouseCategory = 1
          AND (w.ownerPlatform != 'amazon' OR w.warehouseType != 'PLATFORM')
    ) t
    LEFT JOIN (
        SELECT
            channelId,
            channelName,
            warehouseCode,
            group_concat(invFunction SEPARATOR ',') AS invFunctions
        FROM ims.t_stock_channel_warehouse
        GROUP BY channelId, channelName, warehouseCode
    ) c ON t.channelId = c.channelId AND t.physicalWarehouseCode = c.warehouseCode
),

-- ================== 构建 v_onWay_snapshot 逻辑 ==================
v_onWay_snapshot AS (
    SELECT
        t.physicalWarehouseCode,
        t.channelId,
        c.channelName,
        t.sku,
        t.estimatedArriveDate,
        t.poOnWayQty,
        t.soOnWayQty,
        t.totalOnWayQty,
        c.invFunctions
    FROM (
        -- PO 在途库存
        SELECT

            t1.physicalWarehouseCode,
            t1.channelId,
            t1.sku,
            DATE(t1.estimatedArrivalDate) AS estimatedArriveDate,
            IFNULL(SUM(t1.onWayQty), 0) AS poOnWayQty,
            IFNULL(SUM(t2.onWayQty), 0) AS soOnWayQty,
            SUM(COALESCE(t1.onWayQty, 0) + COALESCE(t2.onWayQty, 0)) AS totalOnWayQty
        FROM ims.t_purchase_on_way_detail t1
        LEFT JOIN ims.t_on_way_info_detail t2
            ON  t1.physicalWarehouseCode = t2.toPhysicalWarehouseCode
            AND t1.channelId = t2.toChannelId
            AND t1.sku = t2.sku
            AND DATE(t1.estimatedArrivalDate) = DATE(t2.estimatedInboundTime)
            AND t2.delFlag = 0
        WHERE t2.toPhysicalWarehouseCode IS NULL
        GROUP BY  t1.physicalWarehouseCode, t1.channelId, t1.sku, t1.estimatedArrivalDate

        UNION ALL

        -- SO 在途库存
        SELECT
            COALESCE(t1.physicalWarehouseCode, t2.toPhysicalWarehouseCode) AS physicalWarehouseCode,
            COALESCE(t1.channelId, t2.toChannelId) AS channelId,
            t2.sku,
            DATE(t2.estimatedInboundTime) AS estimatedArriveDate,
            IFNULL(SUM(t1.onWayQty), 0) AS poOnWayQty,
            IFNULL(SUM(t2.onWayQty), 0) AS soOnWayQty,
            SUM(COALESCE(t1.onWayQty, 0) + COALESCE(t2.onWayQty, 0)) AS totalOnWayQty
        FROM ims.t_purchase_on_way_detail t1
        RIGHT JOIN ims.t_on_way_info_detail t2
            on t1.physicalWarehouseCode = t2.toPhysicalWarehouseCode
            AND t1.channelId = t2.toChannelId
            AND t1.sku = t2.sku
            AND DATE(t1.estimatedArrivalDate) = DATE(t2.estimatedInboundTime)
        WHERE t1.physicalWarehouseCode IS NULL
          AND t2.delFlag = 0
        GROUP BY
                 COALESCE(t1.physicalWarehouseCode, t2.toPhysicalWarehouseCode),
                 COALESCE(t1.channelId, t2.toChannelId),
                 t2.sku,
                 DATE(t2.estimatedInboundTime)
    ) t
    LEFT JOIN (
        SELECT
            channelId,
            channelName,
            warehouseCode,
            group_concat(invFunction SEPARATOR ',') AS invFunctions
        FROM ims.t_stock_channel_warehouse
        GROUP BY channelId, channelName, warehouseCode
    ) c ON t.channelId = c.channelId AND t.physicalWarehouseCode = c.warehouseCode
)

SELECT


    NULL AS item_id,

    -- warehouse_id
    inv.physicalWarehouseCode AS warehouse_id,

    -- brand: 当前SQL无品牌信息，暂用NULL填充
    b.base_brand_id AS brand,

    -- region: dim_warehouse 维表去关联
    NULL AS region,

    -- is_retail: 当前SQL无零售标识，暂用NULL填充
    false AS is_retail,

    inv.sku AS sku,

    -- product_sku: skc sku维表关联
    inv.sku AS product_sku,

    -- onhand_quantity: 在库库存（不包括DSW直运数量）
    inv.availableQty + inv.damagedQty + inv.lockQty   AS onhand_quantity,

    -- replen_quantity: 在途数量
    way.totalOnWayQty  AS replen_quantity,

    -- ghost_quantity: 幽灵库存，当前未提供，填充NULL
    NULL AS ghost_quantity,

    -- reserve_quantity: 预留库存（lockQty）
    inv.lockQty AS reserve_quantity,

    -- special_pick_quantity: 特殊拣货库存，当前未提供，填充NULL
    NULL AS special_pick_quantity,

    -- manual_stock_reserve_quantity:当前未提供，填充NULL
    NULL AS manual_stock_reserve_quantity,

    -- available_to_sell_quantity: 可售库存（sellableQty）
    inv.sellableQty AS available_to_sell_quantity,

    -- ecom_available_to_sell_quantity: 电商可售库存，当前未区分，填充NULL
    NULL AS ecom_available_to_sell_quantity,

    -- receipt_inspection_quantity: 入库检验中数量，当前未提供，填充NULL
    NULL AS receipt_inspection_quantity,

    -- return_quantity: 退货数量，当前未提供，填充NULL
    NULL AS return_quantity,

    -- damaged_quantity: 损坏库存（damagedQty）
    inv.damagedQty AS damaged_quantity,

    -- damaged_returns_quantity: 退货损坏数量，当前未提供，填充NULL
    NULL AS damaged_returns_quantity,

    -- allocated_quantity: 已分配数量，当前未提供，填充NULL
    NULL AS allocated_quantity,

    -- intransit_quantity: 在途库存（totalOnWayQty）
    way.totalOnWayQty AS intransit_quantity,

    -- staging_quantity: 待上架数量，当前未提供，填充NULL
    NULL AS staging_quantity,

    -- pick_staging_quantity: 拣货待发数量，当前未提供，填充NULL
    NULL AS pick_staging_quantity,

    -- lost_quantity: 丢失数量，当前未提供，填充NULL
    NULL AS lost_quantity,

    -- open_to_buy_quantity: 可采购数量，当前未提供，填充NULL
    NULL AS open_to_buy_quantity,

    -- landed_cost_per_unit: 到岸成本，当前未提供，填充NULL
    NULL AS landed_cost_per_unit,

    -- total_onhand_landed_cost: 总库存成本 = onhand * cost，当前无法计算，填充NULL
    NULL AS total_onhand_landed_cost,

    -- dsw_dropship_quantity: dropship库存，当前未提供，填充NULL
    NULL AS dsw_dropship_quantity

FROM v_inv_snapshot inv
JOIN v_onWay_snapshot way
ON inv.channelId = way.channelId AND inv.sku = way.sku
left join wms.base_style b on inv.style_name = b.style_name

