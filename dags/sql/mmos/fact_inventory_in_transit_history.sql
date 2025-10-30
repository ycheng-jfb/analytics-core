WITH

-- 获取基础数据，并添加 rollup_date
base_data AS (
  SELECT
    t1.fromPhysicalWarehouseCode AS from_warehouse_id,
    t1.transportCode AS transport_code,
    t2.sku,
    t2.quantity AS units,
    DATE(t1.modifyTime) AS rollup_date
  FROM ims.t_transport t1
  JOIN ims.t_transport_detail t2
    ON t1.transportCode = t2.transportCode
  WHERE t1.fromPhysicalWarehouseCode IS NOT NULL
),

-- 按天、SKU、仓库聚合单位数量
daily_summary AS (
  SELECT
    rollup_date,
    from_warehouse_id,
    sku,
    SUM(units) AS total_units
  FROM base_data
  GROUP BY rollup_date, from_warehouse_id, sku
),

-- 排序并分配行号
ranked_data AS (
  SELECT
    rollup_date,
    from_warehouse_id,
    sku,
    total_units,
    ROW_NUMBER() OVER (PARTITION BY from_warehouse_id, sku ORDER BY rollup_date) AS rn
  FROM daily_summary
),

-- 下一条记录时间
lead_data AS (
  SELECT
    a.rollup_date,
    a.from_warehouse_id,
    a.sku,
    a.total_units,
    MIN(b.rollup_date) AS next_date
  FROM ranked_data a
  LEFT JOIN ranked_data b
    ON a.from_warehouse_id = b.from_warehouse_id
    AND a.sku = b.sku
    AND a.rn < b.rn
  GROUP BY a.rollup_date, a.from_warehouse_id, a.sku, a.total_units
)

-- 计算生效区间
SELECT
  rollup_date,
  sku,
  from_warehouse_id,
  total_units AS units,
  CAST(CONCAT(rollup_date, ' 00:00:00.000') AS DATETIME(3)) AS effective_from_date,
  CASE
    WHEN next_date IS NOT NULL THEN
      CAST(TIMESTAMPADD(MICROSECOND, -1000, next_date) AS DATETIME(3))
    ELSE
      CAST('9999-12-31 23:59:59.999' AS DATETIME(3))
  END AS effective_to_date,
  CASE WHEN next_date IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM lead_data
ORDER BY from_warehouse_id, sku, rollup_date;


