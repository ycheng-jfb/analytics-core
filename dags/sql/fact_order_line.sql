truncate table EDW_PROD.NEW_STG.fact_order_line;

insert into EDW_PROD.NEW_STG.fact_order_line
with user_shard_all_store_id as (
    select *
    from LAKE_MMOS."mmos_membership_marketing_eu".user_shard_all
    where "is_delete" = 0
),
l_p_1 as (
  WITH TRANSACTION as (
    select order_id,sum(amount) amount
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.TRANSACTION
    where kind in ('capture','sale')
    group by order_id
  )
  ,metafield as (
      SELECT
          owner_id,id,namespace
      FROM 
          LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.metafield t2
      WHERE 
          t2.value = 'TARIFF'
          and namespace ='MMOS-product'
  )
  ,order_line_data AS (
    -- 合并基础订单行数据、订单信息和产品信息
    SELECT
      ol.id AS order_line_id,
      ol.order_id,
      ol.variant_id AS product_id,
      ol.PRODUCT_ID as master_product_id,
      ol.quantity AS item_quantity,
      -- ol.price * ol.QUANTITY AS product_subtotal_local_amount,
      -- ol.price  AS price_offered_local_amount,
      PARSE_JSON(ol.price_set):presentment_money:amount::FLOAT * ol.QUANTITY AS product_subtotal_local_amount,
      PARSE_JSON(ol.price_set):presentment_money:amount::FLOAT   AS price_offered_local_amount,
      ol.properties,
      f.customer_id,
      -- o.PROCESSED_AT AS order_local_datetime,
      b.GROUP_CODE AS group_code,
      -- 计算订单总交易金额
      t.amount AS order_total_payment_amount,
      b.VIP_UNIT_PRICE as air_vip_price,
      b.RETAIL_UNIT_PRICE as retail_unit_price,
      f.order_membership_classification_key,
      f.order_sales_channel_key,
      f.order_status_key,
      f.payment_transaction_local_datetime,
      f.shipped_local_datetime,
      f.order_completion_local_datetime,
      o.CURRENCY AS currency_key,
      date(o.PROCESSED_AT) order_placed_local_datetime,
      iff(m.owner_id is not null,1,0) is_tariff
    FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o
    JOIN LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_line ol  ON  o.id = ol.order_id
    LEFT JOIN EDW_PROD.NEW_STG.DIM_PRODUCT b ON ol.VARIANT_ID = b.product_id
    left join TRANSACTION t on o.id = t.order_id
    left join EDW_PROD.NEW_STG.FACT_ORDER f ON o.id = f.order_id
    left join metafield m on m.owner_id = ol.product_id
    -- where o.PROCESSED_AT >= '2025-09-15'
  )

  ,bundle_data AS (
    -- 解析捆绑商品信息
    SELECT
      ol.id,
      SPLIT_PART(f.value:value::STRING, ':', 1) AS bundle_product_id
    FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_line ol,
        LATERAL FLATTEN(input => PARSE_JSON(ol.properties)) f
    WHERE f.value:name::STRING = '_sb_bundle_variant_id_qty'
  )

  -- 计算非会员积分折扣的折扣金额
  ,discount_calculation AS (
    SELECT
      a.id AS order_line_id,
      a.order_id,
      SUM(c.AMOUNT) AS product_discount_local_amount
    FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_line a
    LEFT JOIN LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.discount_allocation c
      ON a.id = c.order_line_id
    LEFT JOIN LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.discount_application d
      ON a.order_id = d.order_id
      AND d.index = c.discount_application_index
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_shoedazzle".USER_CREDIT_SHARD_ALL b
      ON d.code = b."promotion_code"
    WHERE b."promotion_code" IS NULL
    GROUP BY a.id, a.order_id
  )

  -- 非vip_credit的折扣金额取shopify的折扣金额
  -- vip_credit的金额取credit的金额进行分摊
  ,order_line_discount_and_product_amount  as (
  select 
        a.order_id
      ,c.order_line_id
      , COALESCE(d.code,d.title) 
      ,PARSE_JSON(a.price_set):presentment_money:amount::FLOAT * a.QUANTITY order_line_price
      ,e."price" price
      ,iff(e."id" is not null,0,c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT) as order_line_discount_local_amount -- 非vip_credit的折扣金额取shopify的折扣金额
      -- 分摊比例
      ,(a.price * a.quantity) / sum(a.price * a.quantity) over(partition by a.order_id, COALESCE(d.code,d.title)) as ratio
      -- 分摊后的 price
      ,( (a.price * a.quantity) / sum(a.price * a.quantity) over(partition by a.order_id,  COALESCE(d.code,d.title)) ) * e."price" as product_subtotal_local_amount -- vip_credit的金额取credit的金额进行分摊
      ,e."id" vip_credit_id
  from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_line  a 
  join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.discount_allocation c 
      on a.id=c.order_line_id
  join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.discount_application d 
      on a.order_id=d.order_id and d.index=c.discount_application_index 
        and d.target_type='line_item' and d.target_selection<>'all'
  left join LAKE_MMOS."mmos_membership_marketing_shoedazzle".USER_CREDIT_SHARD_ALL b
      on  COALESCE(d.code,d.title)=b."promotion_code"
  left join LAKE_MMOS."mmos_membership_marketing_shoedazzle".VIP_CREDIT_BALANCE_DETAIL_SHARD_ALL e 
      on cast(b."token_id" as varchar) = e."id"
  )

  -- 计算订单分摊比例
  ,allocation_ratios AS (
  SELECT
    old.order_line_id,
    old.order_id,
    COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) as product_subtotal_local_amount,
    COALESCE(dc.product_discount_local_amount,0) product_discount_local_amount ,
    COALESCE(oa.order_line_discount_local_amount,0) order_line_discount_local_amount, -- 非vip_credit的折扣金额取shopify的折扣金额
    -- 单行sku计算折后价
    -- (old.product_subtotal_local_amount - COALESCE(dc.product_discount_local_amount, 0)) AS discounted_line_amount,
    -- 计算订单总折后价
    -- SUM(old.product_subtotal_local_amount - COALESCE(dc.product_discount_local_amount, 0))
    --   OVER (PARTITION BY old.order_id) AS total_order_discounted_amount,
    -- 计算订单分摊比例
    -- CASE
    --   WHEN SUM(COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0)) OVER (PARTITION BY old.order_id) > 0 THEN
    --     (COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0))
    --      /(SUM(COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0)) OVER (PARTITION BY old.order_id))
    --   ELSE 0
    -- END AS order_allocation_ratio

    CASE
      WHEN old.is_tariff = 1 then 0
      when SUM(iff(old.is_tariff = 1 , 0,COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) - COALESCE(oa.order_line_discount_local_amount,0))
                  ) OVER (PARTITION BY old.order_id) > 0 THEN
        (COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0))
        /(SUM(iff(old.is_tariff = 1 , 0,COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) - COALESCE(oa.order_line_discount_local_amount,0))
                  ) OVER (PARTITION BY old.order_id))
      ELSE 0
    END AS order_allocation_ratio
    -- 计算折扣分摊比例
    -- CASE
    --   WHEN SUM(dc.product_discount_local_amount) OVER (PARTITION BY old.order_id) > 0 THEN
    --     dc.product_discount_local_amount /
    --     SUM(dc.product_discount_local_amount) OVER (PARTITION BY old.order_id)
    --   ELSE 0
    -- END AS discount_allocation_ratio,
    -- -- 订单总支付金额
    ,COALESCE(old.order_total_payment_amount,0) order_total_payment_amount
    ,iff(oa.vip_credit_id is not null,oa.ratio,0)                         as token_count
    ,iff(oa.vip_credit_id is not null,oa.product_subtotal_local_amount,0) as token_local_amount
    ,iff(oa.vip_credit_id is not null and oa.product_subtotal_local_amount>0,oa.product_subtotal_local_amount,0) as cash_credit_local_amount
    ,iff(oa.vip_credit_id is not null and oa.product_subtotal_local_amount=0,oa.product_subtotal_local_amount,0) as non_cash_credit_local_amount
  FROM order_line_data old
  LEFT JOIN discount_calculation dc ON old.order_line_id = dc.order_line_id
  LEFT JOIN order_line_discount_and_product_amount oa ON old.order_line_id = oa.order_line_id
  )

  SELECT
    -- 基础字段
    old.order_line_id,
    old.order_id,
    55 as store_id,
    old.customer_id,
    old.product_id,
    old.master_product_id,
    bd.bundle_product_id,
    null as bundle_component_product_id,
    old.group_code,
    null as bundle_component_history_key,
    1 as product_type_key,
    null as bundle_order_line_id,
    old.order_membership_classification_key,
    old.order_sales_channel_key,
    6 as order_line_status_key,
    old.order_status_key,
    null as order_product_source_key,
    null as currency_key,
    null as administrator_id,
    null as warehouse_id,
    null as shipping_address_id,
    null as billing_address_id,
    null as bounceback_endowment_id,
    null as cost_source_id,
    null as cost_source,
    f.order_local_datetime,
    old.payment_transaction_local_datetime,
    old.shipped_local_datetime,
    old.order_completion_local_datetime,
    us_er.EXCHANGE_RATE as order_date_usd_conversion_rate,
    eu_er.EXCHANGE_RATE as order_date_eur_conversion_rate,
    us_er2.EXCHANGE_RATE as payment_transaction_date_usd_conversion_rate,
    eu_er2.EXCHANGE_RATE as payment_transaction_date_eur_conversion_rate,
    us_er3.EXCHANGE_RATE as shipped_date_usd_conversion_rate,
    us_er2.EXCHANGE_RATE as shipped_date_eur_conversion_rate,
    COALESCE(us_er3.EXCHANGE_RATE,us_er2.EXCHANGE_RATE,us_er.EXCHANGE_RATE) as reporting_usd_conversion_rate,
    COALESCE(eu_er3.EXCHANGE_RATE,eu_er2.EXCHANGE_RATE,eu_er.EXCHANGE_RATE) as reporting_eur_conversion_rate,
    null AS effective_vat_rate,
    COALESCE(old.item_quantity,0),
    -- 订单分摊比例 * 订单总支付金额
    COALESCE(ar.order_allocation_ratio * f.payment_transaction_local_amount,0) AS payment_transaction_local_amount,

    COALESCE(ar.product_subtotal_local_amount,0) as subtotal_excl_tariff_local_amount,
    COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) as tariff_revenue_local_amount,
    COALESCE(ar.product_subtotal_local_amount,0) + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) product_subtotal_local_amount,

    -- 订单分摊比例 * 税
    COALESCE(ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT,0) as tax_local_amount,
    COALESCE(ar.product_discount_local_amount,0),
    null as shipping_cost_local_amount,
    -- 折扣分摊比例 * shipping_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount,0) AS shipping_discount_local_amount,
    -- 订单分摊比例 * shipping_revenue_before_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,0) AS shipping_revenue_before_discount_local_amount,
    -- shipping_revenue_before_discount_local_amount - shipping_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount,0) AS shipping_revenue_local_amount,
    ar.cash_credit_local_amount as cash_credit_local_amount,
    null as cash_membership_credit_local_amount,
    null as cash_refund_credit_local_amount,
    null as cash_giftcard_credit_local_amount,
      -- 订单分摊比例 * cash_giftco_credit_local_amount
    COALESCE(ar.order_allocation_ratio * f.CASH_GIFTCO_CREDIT_LOCAL_AMOUNT,0) AS cash_giftco_credit_local_amount,
    COALESCE(ar.token_count,0)        as token_count,
    COALESCE(ar.token_local_amount,0) as token_local_amount,
    null as cash_token_count,
    null as cash_token_local_amount,
    null as non_cash_token_count,
    null as non_cash_token_local_amount,
    ar.non_cash_credit_local_amount as non_cash_credit_local_amount,
    null as estimated_landed_cost_local_amount,
    null as oracle_cost_local_amount,
    null as lpn_po_cost_local_amount,
    null as po_cost_local_amount,
    null as misc_cogs_local_amount,
    null as estimated_shipping_supplies_cost_local_amount,
    null as estimated_shipping_cost_local_amount,
    null as estimated_variable_warehouse_cost_local_amount,
    null as estimated_variable_gms_cost_local_amount,
    -- 订单分摊比例 * estimated_variable_payment_processing_pct_cash_revenue
    COALESCE(ar.order_allocation_ratio * f.ESTIMATED_VARIABLE_PAYMENT_PROCESSING_PCT_CASH_REVENUE,0) AS estimated_variable_payment_processing_pct_cash_revenue,
    COALESCE(old.price_offered_local_amount,0),
    COALESCE(old.air_vip_price,0),
    COALESCE(old.retail_unit_price,0),
    null as bundle_price_offered_local_amount,
    null as product_price_history_key,
    null as bundle_product_price_history_key,
    null as item_price_key,

    -- amount_to_pay计算
    COALESCE(ar.product_subtotal_local_amount,0) -- subtotal_excl_tariff_local_amount
    + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount,0) +                         -- product_discount_local_amount
    COALESCE( ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) AS amount_to_pay,


    -- cash_gross_revenue_local_amount计算
    COALESCE(ar.order_allocation_ratio * ar.order_total_payment_amount, 0) -
    COALESCE(ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT, 0, 0) AS cash_gross_revenue_local_amount,
    -- product_gross_revenue_local_amount计算
    COALESCE(ar.product_subtotal_local_amount,0) -- subtotal_excl_tariff_local_amount
    + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) -
    0 AS product_gross_revenue_local_amount, -- non_cash_credit_local_amount为null
    -- product_gross_revenue_excl_shipping_local_amount计算
    COALESCE(ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0),0) +
    COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) -
    0 AS product_gross_revenue_excl_shipping_local_amount, -- non_cash_credit_local_amount为null
    -- product_margin_pre_return_local_amount计算
    (COALESCE(ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0),0) +
    0 + -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) -
    0) - -- non_cash_credit_local_amount为null
    0 AS product_margin_pre_return_local_amount, -- estimated_landed_cost_local_amount为null
    -- product_margin_pre_return_excl_shipping_local_amount计算
    ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) + 
    0 + -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) -
    0 - -- non_cash_credit_local_amount为null
    0 AS product_margin_pre_return_excl_shipping_local_amount, -- estimated_landed_cost_local_amount为null

    null as group_key,
    null as lpn_code,
    null as custom_printed_text,
    null as reporting_landed_cost_local_amount,
    null as is_actual_landed_cost,
    null as is_fully_landed,
    null as fully_landed_conversion_date,
    null as actual_landed_cost_local_amount,
    null as actual_po_cost_local_amount,
    null as actual_cmt_cost_local_amount,
    null as actual_tariff_duty_cost_local_amount,
    null as actual_freight_cost_local_amount,
    null as actual_other_cost_local_amount,
    null as bounceback_endowment_local_amount,
    null as vip_endowment_local_amount,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime,
    old.is_tariff,
    COALESCE(ar.order_line_discount_local_amount,0)
  FROM order_line_data old
  LEFT JOIN bundle_data bd ON old.order_line_id = bd.id
  LEFT JOIN allocation_ratios ar ON old.order_line_id = ar.order_line_id
  inner join EDW_PROD.NEW_STG.fact_order f on old.order_id = f.ORDER_ID
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er on f.order_local_datetime = us_er.DATE and old.currency_key = us_er.EXCHANGE_FROM and us_er.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er on f.order_local_datetime = eu_er.DATE and old.currency_key = eu_er.EXCHANGE_FROM and eu_er.EXCHANGE_TO = 'EUR'
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er2 on old.order_placed_local_datetime = us_er2.DATE and old.currency_key = us_er2.EXCHANGE_FROM and us_er2.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er2 on old.order_placed_local_datetime = eu_er2.DATE and old.currency_key = eu_er2.EXCHANGE_FROM and eu_er2.EXCHANGE_TO = 'EUR'
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er3 on old.shipped_local_datetime = us_er3.DATE and old.currency_key = us_er3.EXCHANGE_FROM and us_er3.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er3 on old.shipped_local_datetime = eu_er3.DATE and old.currency_key = eu_er3.EXCHANGE_FROM and eu_er3.EXCHANGE_TO = 'EUR'
  where f.order_local_datetime>='2025-09-15'
)
,l_p_2 as (
  WITH TRANSACTION as (
    select order_id,sum(amount) amount
    from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.TRANSACTION
    where kind in ('capture','sale')
    group by order_id
  )
  ,metafield as (
      SELECT
          owner_id,id,namespace
      FROM 
          LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.metafield t2
      WHERE 
          t2.value = 'TARIFF'
          and namespace ='MMOS-product'
  )
  ,order_line_data AS (
    -- 合并基础订单行数据、订单信息和产品信息
    SELECT
      ol.id AS order_line_id,
      ol.order_id,
      ol.variant_id AS product_id,
      ol.PRODUCT_ID as master_product_id,
      ol.quantity AS item_quantity,
      PARSE_JSON(ol.price_set):presentment_money:amount::FLOAT * ol.QUANTITY AS product_subtotal_local_amount,
      PARSE_JSON(ol.price_set):presentment_money:amount::FLOAT  AS price_offered_local_amount,
      ol.properties,
      f.customer_id,
      -- o.PROCESSED_AT AS order_local_datetime,
      b.GROUP_CODE AS group_code,
      -- 计算订单总交易金额
      t.amount AS order_total_payment_amount,
      b.VIP_UNIT_PRICE as air_vip_price,
      b.RETAIL_UNIT_PRICE as retail_unit_price,
      f.order_membership_classification_key,
      f.order_sales_channel_key,
      f.order_status_key,
      f.payment_transaction_local_datetime,
      f.shipped_local_datetime,
      f.order_completion_local_datetime,
      o.CURRENCY AS currency_key,
      date(o.PROCESSED_AT) order_placed_local_datetime,
      iff(m.owner_id is not null,1,0) is_tariff
    FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o
    JOIN LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_line ol  ON  o.id = ol.order_id
    LEFT JOIN EDW_PROD.NEW_STG.DIM_PRODUCT b ON ol.VARIANT_ID = b.product_id
    left join TRANSACTION t on o.id = t.order_id
    left join EDW_PROD.NEW_STG.FACT_ORDER f ON o.id = f.order_id
    left join metafield m on m.owner_id = ol.product_id
    -- where o.PROCESSED_AT >= '2025-09-15'
  )

  ,bundle_data AS (
    -- 解析捆绑商品信息
    SELECT
      ol.id,
      SPLIT_PART(f.value:value::STRING, ':', 1) AS bundle_product_id
    FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_line ol,
         LATERAL FLATTEN(input => PARSE_JSON(ol.properties)) f
    WHERE f.value:name::STRING = '_sb_bundle_variant_id_qty'
  )

  -- 计算非会员积分折扣的折扣金额
  ,discount_calculation AS (
    SELECT
      a.id AS order_line_id,
      a.order_id,
      SUM(c.AMOUNT) AS product_discount_local_amount
    FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_line a
    LEFT JOIN LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.discount_allocation c
      ON a.id = c.order_line_id
    LEFT JOIN LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.discount_application d
      ON a.order_id = d.order_id
      AND d.index = c.discount_application_index
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_us"."user_credit_shard_all" b
      ON d.code = b."promotion_code"
    WHERE b."promotion_code" IS NULL
    GROUP BY a.id, a.order_id
  )

  -- 非vip_credit的折扣金额取shopify的折扣金额
  -- vip_credit的金额取credit的金额进行分摊
  ,order_line_discount_and_product_amount  as (
  select 
        a.order_id
      ,c.order_line_id
      , COALESCE(d.code,d.title) 
      ,PARSE_JSON(a.price_set):presentment_money:amount::FLOAT *a.QUANTITY order_line_price
      ,e."price" price
      ,iff(e."id" is not null,0,c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT) as order_line_discount_local_amount -- 非vip_credit的折扣金额取shopify的折扣金额
      -- 分摊比例
      ,(a.price * a.quantity) / sum(a.price * a.quantity) over(partition by a.order_id, COALESCE(d.code,d.title)) as ratio
      -- 分摊后的 price
      ,( (a.price * a.quantity) / sum(a.price * a.quantity) over(partition by a.order_id,  COALESCE(d.code,d.title)) ) * e."price" as product_subtotal_local_amount -- vip_credit的金额取credit的金额进行分摊
      ,e."id" vip_credit_id
  from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_line  a 
  join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.discount_allocation c 
      on a.id=c.order_line_id
  join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.discount_application d 
      on a.order_id=d.order_id and d.index=c.discount_application_index 
         and d.target_type='line_item' and d.target_selection<>'all'
  left join LAKE_MMOS."mmos_membership_marketing_us"."user_credit_shard_all" b
      on  COALESCE(d.code,d.title)=b."promotion_code"
  left join LAKE_MMOS."mmos_membership_marketing_us"."vip_credit_balance_detail_shard_all" e 
      on cast(b."token_id" as varchar) = e."id"
  )

  -- 计算订单分摊比例
  ,allocation_ratios AS (
  SELECT
    old.order_line_id,
    old.order_id,
    COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) as product_subtotal_local_amount,
    COALESCE(dc.product_discount_local_amount,0) product_discount_local_amount ,
    COALESCE(oa.order_line_discount_local_amount,0) order_line_discount_local_amount, -- 非vip_credit的折扣金额取shopify的折扣金额
    -- 单行sku计算折后价
    -- (old.product_subtotal_local_amount - COALESCE(dc.product_discount_local_amount, 0)) AS discounted_line_amount,
    -- 计算订单总折后价
    -- SUM(old.product_subtotal_local_amount - COALESCE(dc.product_discount_local_amount, 0))
    --   OVER (PARTITION BY old.order_id) AS total_order_discounted_amount,
    -- 计算订单分摊比例
    -- CASE
    --   WHEN SUM(COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0)) OVER (PARTITION BY old.order_id) > 0 THEN
    --     (COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0))
    --      /(SUM(COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0)) OVER (PARTITION BY old.order_id))
    --   ELSE 0
    -- END AS order_allocation_ratio

    CASE
      WHEN old.is_tariff = 1 then 0
      when SUM(iff(old.is_tariff = 1 , 0,COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) - COALESCE(oa.order_line_discount_local_amount,0))
                  ) OVER (PARTITION BY old.order_id) > 0 THEN
        (COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0))
         /(SUM(iff(old.is_tariff = 1 , 0,COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) - COALESCE(oa.order_line_discount_local_amount,0))
                  ) OVER (PARTITION BY old.order_id))
      ELSE 0
    END AS order_allocation_ratio
    -- 计算折扣分摊比例
    -- CASE
    --   WHEN SUM(dc.product_discount_local_amount) OVER (PARTITION BY old.order_id) > 0 THEN
    --     dc.product_discount_local_amount /
    --     SUM(dc.product_discount_local_amount) OVER (PARTITION BY old.order_id)
    --   ELSE 0
    -- END AS discount_allocation_ratio,
    -- -- 订单总支付金额
    ,COALESCE(old.order_total_payment_amount,0) order_total_payment_amount
    ,iff(oa.vip_credit_id is not null,oa.ratio,0)                         as token_count
    ,iff(oa.vip_credit_id is not null,oa.product_subtotal_local_amount,0) as token_local_amount
    ,iff(oa.vip_credit_id is not null and oa.product_subtotal_local_amount>0,oa.product_subtotal_local_amount,0) as cash_credit_local_amount
    ,iff(oa.vip_credit_id is not null and oa.product_subtotal_local_amount=0,oa.product_subtotal_local_amount,0) as non_cash_credit_local_amount
  FROM order_line_data old
  LEFT JOIN discount_calculation dc ON old.order_line_id = dc.order_line_id
  LEFT JOIN order_line_discount_and_product_amount oa ON old.order_line_id = oa.order_line_id
  )

  SELECT
    -- 基础字段
    old.order_line_id,
    old.order_id,
    26 as store_id,
    old.customer_id,
    old.product_id,
    old.master_product_id,
    bd.bundle_product_id,
    null as bundle_component_product_id,
    old.group_code,
    null as bundle_component_history_key,
    1 as product_type_key,
    null as bundle_order_line_id,
    old.order_membership_classification_key,
    old.order_sales_channel_key,
    6 as order_line_status_key,
    old.order_status_key,
    null as order_product_source_key,
    null as currency_key,
    null as administrator_id,
    null as warehouse_id,
    null as shipping_address_id,
    null as billing_address_id,
    null as bounceback_endowment_id,
    null as cost_source_id,
    null as cost_source,
    f.order_local_datetime,
    old.payment_transaction_local_datetime,
    old.shipped_local_datetime,
    old.order_completion_local_datetime,
    us_er.EXCHANGE_RATE as order_date_usd_conversion_rate,
    eu_er.EXCHANGE_RATE as order_date_eur_conversion_rate,
    us_er2.EXCHANGE_RATE as payment_transaction_date_usd_conversion_rate,
    eu_er2.EXCHANGE_RATE as payment_transaction_date_eur_conversion_rate,
    us_er3.EXCHANGE_RATE as shipped_date_usd_conversion_rate,
    us_er2.EXCHANGE_RATE as shipped_date_eur_conversion_rate,
    COALESCE(us_er3.EXCHANGE_RATE,us_er2.EXCHANGE_RATE,us_er.EXCHANGE_RATE) as reporting_usd_conversion_rate,
    COALESCE(eu_er3.EXCHANGE_RATE,eu_er2.EXCHANGE_RATE,eu_er.EXCHANGE_RATE) as reporting_eur_conversion_rate,
    null AS effective_vat_rate,
    COALESCE(old.item_quantity,0),
    -- 订单分摊比例 * 订单总支付金额
    COALESCE(ar.order_allocation_ratio * f.payment_transaction_local_amount,0) AS payment_transaction_local_amount,

    COALESCE(ar.product_subtotal_local_amount,0) as subtotal_excl_tariff_local_amount,
    COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) as tariff_revenue_local_amount,
    COALESCE(ar.product_subtotal_local_amount,0) + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) product_subtotal_local_amount,

    -- 订单分摊比例 * 税
    COALESCE(ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT,0) as tax_local_amount,
    COALESCE(ar.product_discount_local_amount,0),
    null as shipping_cost_local_amount,
    -- 折扣分摊比例 * shipping_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount,0) AS shipping_discount_local_amount,
    -- 订单分摊比例 * shipping_revenue_before_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,0) AS shipping_revenue_before_discount_local_amount,
    -- shipping_revenue_before_discount_local_amount - shipping_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount,0) AS shipping_revenue_local_amount,
    ar.cash_credit_local_amount as cash_credit_local_amount,
    null as cash_membership_credit_local_amount,
    null as cash_refund_credit_local_amount,
    null as cash_giftcard_credit_local_amount,
      -- 订单分摊比例 * cash_giftco_credit_local_amount
    COALESCE(ar.order_allocation_ratio * f.CASH_GIFTCO_CREDIT_LOCAL_AMOUNT,0) AS cash_giftco_credit_local_amount,
    COALESCE(ar.token_count,0)        as token_count,
    COALESCE(ar.token_local_amount,0) as token_local_amount,
    null as cash_token_count,
    null as cash_token_local_amount,
    null as non_cash_token_count,
    null as non_cash_token_local_amount,
    ar.non_cash_credit_local_amount as non_cash_credit_local_amount,
    null as estimated_landed_cost_local_amount,
    null as oracle_cost_local_amount,
    null as lpn_po_cost_local_amount,
    null as po_cost_local_amount,
    null as misc_cogs_local_amount,
    null as estimated_shipping_supplies_cost_local_amount,
    null as estimated_shipping_cost_local_amount,
    null as estimated_variable_warehouse_cost_local_amount,
    null as estimated_variable_gms_cost_local_amount,
    -- 订单分摊比例 * estimated_variable_payment_processing_pct_cash_revenue
    COALESCE(ar.order_allocation_ratio * f.ESTIMATED_VARIABLE_PAYMENT_PROCESSING_PCT_CASH_REVENUE,0) AS estimated_variable_payment_processing_pct_cash_revenue,
    COALESCE(old.price_offered_local_amount,0),
    COALESCE(old.air_vip_price,0),
    COALESCE(old.retail_unit_price,0),
    null as bundle_price_offered_local_amount,
    null as product_price_history_key,
    null as bundle_product_price_history_key,
    null as item_price_key,

    -- amount_to_pay计算
    COALESCE(ar.product_subtotal_local_amount,0) -- subtotal_excl_tariff_local_amount
    + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount,0) +                         -- product_discount_local_amount
    COALESCE( ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) AS amount_to_pay,


    -- cash_gross_revenue_local_amount计算
    COALESCE(ar.order_allocation_ratio * ar.order_total_payment_amount, 0) -
    COALESCE(ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT, 0, 0) AS cash_gross_revenue_local_amount,
    -- product_gross_revenue_local_amount计算
    COALESCE(ar.product_subtotal_local_amount,0) -- subtotal_excl_tariff_local_amount
    + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) -
    0 AS product_gross_revenue_local_amount, -- non_cash_credit_local_amount为null
    -- product_gross_revenue_excl_shipping_local_amount计算
    COALESCE(ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0),0) +
    COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) -
    0 AS product_gross_revenue_excl_shipping_local_amount, -- non_cash_credit_local_amount为null
    -- product_margin_pre_return_local_amount计算
    (COALESCE(ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0),0) +
    0 + -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) -
    0) - -- non_cash_credit_local_amount为null
    0 AS product_margin_pre_return_local_amount, -- estimated_landed_cost_local_amount为null
    -- product_margin_pre_return_excl_shipping_local_amount计算
    ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) + 
    0 + -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) -
    0 - -- non_cash_credit_local_amount为null
    0 AS product_margin_pre_return_excl_shipping_local_amount, -- estimated_landed_cost_local_amount为null

    null as group_key,
    null as lpn_code,
    null as custom_printed_text,
    null as reporting_landed_cost_local_amount,
    null as is_actual_landed_cost,
    null as is_fully_landed,
    null as fully_landed_conversion_date,
    null as actual_landed_cost_local_amount,
    null as actual_po_cost_local_amount,
    null as actual_cmt_cost_local_amount,
    null as actual_tariff_duty_cost_local_amount,
    null as actual_freight_cost_local_amount,
    null as actual_other_cost_local_amount,
    null as bounceback_endowment_local_amount,
    null as vip_endowment_local_amount,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime,
    old.is_tariff,
    COALESCE(ar.order_line_discount_local_amount,0)
  FROM order_line_data old
  LEFT JOIN bundle_data bd ON old.order_line_id = bd.id
  LEFT JOIN allocation_ratios ar ON old.order_line_id = ar.order_line_id
  inner join EDW_PROD.NEW_STG.fact_order f on old.order_id = f.ORDER_ID
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er on f.order_local_datetime = us_er.DATE and old.currency_key = us_er.EXCHANGE_FROM and us_er.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er on f.order_local_datetime = eu_er.DATE and old.currency_key = eu_er.EXCHANGE_FROM and eu_er.EXCHANGE_TO = 'EUR'
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er2 on old.order_placed_local_datetime = us_er2.DATE and old.currency_key = us_er2.EXCHANGE_FROM and us_er2.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er2 on old.order_placed_local_datetime = eu_er2.DATE and old.currency_key = eu_er2.EXCHANGE_FROM and eu_er2.EXCHANGE_TO = 'EUR'
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er3 on old.shipped_local_datetime = us_er3.DATE and old.currency_key = us_er3.EXCHANGE_FROM and us_er3.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er3 on old.shipped_local_datetime = eu_er3.DATE and old.currency_key = eu_er3.EXCHANGE_FROM and eu_er3.EXCHANGE_TO = 'EUR'
  where f.order_local_datetime>='2025-09-23'
)
, l_p_3 as (
  WITH TRANSACTION as (
    select order_id,sum(amount) amount
    from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.TRANSACTION
    where kind in ('capture','sale')
    group by order_id
  )
  ,metafield as (
      SELECT
          owner_id,id,namespace
      FROM
          LAKE_MMOS.SHOPIFY_FABKIDS_PROD.metafield t2
      WHERE
          t2.value = 'TARIFF'
          and namespace ='MMOS-product'
  )
  ,order_line_data AS (
    -- 合并基础订单行数据、订单信息和产品信息
    SELECT
      ol.id AS order_line_id,
      ol.order_id,
      ol.variant_id AS product_id,
      ol.PRODUCT_ID as master_product_id,
      ol.quantity AS item_quantity,
      PARSE_JSON(ol.price_set):presentment_money:amount::FLOAT * ol.QUANTITY AS product_subtotal_local_amount,
      PARSE_JSON(ol.price_set):presentment_money:amount::FLOAT  AS price_offered_local_amount,
      ol.properties,
      f.customer_id,
      -- o.PROCESSED_AT AS order_local_datetime,
      b.GROUP_CODE AS group_code,
      -- 计算订单总交易金额
      t.amount AS order_total_payment_amount,
      b.VIP_UNIT_PRICE as air_vip_price,
      b.RETAIL_UNIT_PRICE as retail_unit_price,
      f.order_membership_classification_key,
      f.order_sales_channel_key,
      f.order_status_key,
      f.payment_transaction_local_datetime,
      f.shipped_local_datetime,
      f.order_completion_local_datetime,
      o.CURRENCY AS currency_key,
      date(o.PROCESSED_AT) order_placed_local_datetime,
      iff(m.owner_id is not null,1,0) is_tariff
    FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o
    JOIN LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_line ol  ON  o.id = ol.order_id
    LEFT JOIN EDW_PROD.NEW_STG.DIM_PRODUCT b ON ol.VARIANT_ID = b.product_id
    left join TRANSACTION t on o.id = t.order_id
    left join EDW_PROD.NEW_STG.FACT_ORDER f ON o.id = f.order_id
    left join metafield m on m.owner_id = ol.product_id
    -- where o.PROCESSED_AT >= '2025-09-15'
  )

  ,bundle_data AS (
    -- 解析捆绑商品信息
    SELECT
      ol.id,
      SPLIT_PART(f.value:value::STRING, ':', 1) AS bundle_product_id
    FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_line ol,
        LATERAL FLATTEN(input => PARSE_JSON(ol.properties)) f
    WHERE f.value:name::STRING = '_sb_bundle_variant_id_qty'
  )

  -- 计算非会员积分折扣的折扣金额
  ,discount_calculation AS (
    SELECT
      a.id AS order_line_id,
      a.order_id,
      SUM(c.AMOUNT) AS product_discount_local_amount
    FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_line a
    LEFT JOIN LAKE_MMOS.SHOPIFY_FABKIDS_PROD.discount_allocation c
      ON a.id = c.order_line_id
    LEFT JOIN LAKE_MMOS.SHOPIFY_FABKIDS_PROD.discount_application d
      ON a.order_id = d.order_id
      AND d.index = c.discount_application_index
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_fabkids".USER_CREDIT_SHARD_ALL b
      ON d.code = b."promotion_code"
    WHERE b."promotion_code" IS NULL
    GROUP BY a.id, a.order_id
  )

  -- 非vip_credit的折扣金额取shopify的折扣金额
  -- vip_credit的金额取credit的金额进行分摊
  ,order_line_discount_and_product_amount  as (
  select
        a.order_id
      ,c.order_line_id
      , COALESCE(d.code,d.title)
      ,PARSE_JSON(a.price_set):presentment_money:amount::FLOAT *a.QUANTITY order_line_price
      ,e."price" price
      ,iff(e."id" is not null,0,c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT) as order_line_discount_local_amount -- 非vip_credit的折扣金额取shopify的折扣金额
      -- 分摊比例
      ,(a.price * a.quantity) / sum(a.price * a.quantity) over(partition by a.order_id, COALESCE(d.code,d.title)) as ratio
      -- 分摊后的 price
      ,( (a.price * a.quantity) / sum(a.price * a.quantity) over(partition by a.order_id,  COALESCE(d.code,d.title)) ) * e."price" as product_subtotal_local_amount -- vip_credit的金额取credit的金额进行分摊
      ,e."id" vip_credit_id
  from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_line  a
  join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.discount_allocation c
      on a.id=c.order_line_id
  join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.discount_application d
      on a.order_id=d.order_id and d.index=c.discount_application_index
        and d.target_type='line_item' and d.target_selection<>'all'
  left join LAKE_MMOS."mmos_membership_marketing_fabkids".USER_CREDIT_SHARD_ALL b
      on  COALESCE(d.code,d.title)=b."promotion_code"
  left join LAKE_MMOS."mmos_membership_marketing_fabkids"."vip_credit_balance_detail_shard_all" e
      on cast(b."token_id" as varchar) = e."id"
  )

  -- 计算订单分摊比例
  ,allocation_ratios AS (
  SELECT
    old.order_line_id,
    old.order_id,
    COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) as product_subtotal_local_amount,
    COALESCE(dc.product_discount_local_amount,0) product_discount_local_amount ,
    COALESCE(oa.order_line_discount_local_amount,0) order_line_discount_local_amount, -- 非vip_credit的折扣金额取shopify的折扣金额
    -- 单行sku计算折后价
    -- (old.product_subtotal_local_amount - COALESCE(dc.product_discount_local_amount, 0)) AS discounted_line_amount,
    -- 计算订单总折后价
    -- SUM(old.product_subtotal_local_amount - COALESCE(dc.product_discount_local_amount, 0))
    --   OVER (PARTITION BY old.order_id) AS total_order_discounted_amount,
    -- 计算订单分摊比例
    -- CASE
    --   WHEN SUM(COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0)) OVER (PARTITION BY old.order_id) > 0 THEN
    --     (COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0))
    --      /(SUM(COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0)) OVER (PARTITION BY old.order_id))
    --   ELSE 0
    -- END AS order_allocation_ratio

    CASE
      WHEN old.is_tariff = 1 then 0
      when SUM(iff(old.is_tariff = 1 , 0,COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) - COALESCE(oa.order_line_discount_local_amount,0))
                  ) OVER (PARTITION BY old.order_id) > 0 THEN
        (COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0))
        /(SUM(iff(old.is_tariff = 1 , 0,COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) - COALESCE(oa.order_line_discount_local_amount,0))
                  ) OVER (PARTITION BY old.order_id))
      ELSE 0
    END AS order_allocation_ratio
    -- 计算折扣分摊比例
    -- CASE
    --   WHEN SUM(dc.product_discount_local_amount) OVER (PARTITION BY old.order_id) > 0 THEN
    --     dc.product_discount_local_amount /
    --     SUM(dc.product_discount_local_amount) OVER (PARTITION BY old.order_id)
    --   ELSE 0
    -- END AS discount_allocation_ratio,
    -- -- 订单总支付金额
    ,COALESCE(old.order_total_payment_amount,0) order_total_payment_amount
    ,iff(oa.vip_credit_id is not null,oa.ratio,0)                         as token_count
    ,iff(oa.vip_credit_id is not null,oa.product_subtotal_local_amount,0) as token_local_amount
    ,iff(oa.vip_credit_id is not null and oa.product_subtotal_local_amount>0,oa.product_subtotal_local_amount,0) as cash_credit_local_amount
    ,iff(oa.vip_credit_id is not null and oa.product_subtotal_local_amount=0,oa.product_subtotal_local_amount,0) as non_cash_credit_local_amount
  FROM order_line_data old
  LEFT JOIN discount_calculation dc ON old.order_line_id = dc.order_line_id
  LEFT JOIN order_line_discount_and_product_amount oa ON old.order_line_id = oa.order_line_id
  )

  SELECT
    -- 基础字段
    old.order_line_id,
    old.order_id,
    46 as store_id,
    old.customer_id,
    old.product_id,
    old.master_product_id,
    bd.bundle_product_id,
    null as bundle_component_product_id,
    old.group_code,
    null as bundle_component_history_key,
    1 as product_type_key,
    null as bundle_order_line_id,
    old.order_membership_classification_key,
    old.order_sales_channel_key,
    6 as order_line_status_key,
    old.order_status_key,
    null as order_product_source_key,
    null as currency_key,
    null as administrator_id,
    null as warehouse_id,
    null as shipping_address_id,
    null as billing_address_id,
    null as bounceback_endowment_id,
    null as cost_source_id,
    null as cost_source,
    f.order_local_datetime,
    old.payment_transaction_local_datetime,
    old.shipped_local_datetime,
    old.order_completion_local_datetime,
    us_er.EXCHANGE_RATE as order_date_usd_conversion_rate,
    eu_er.EXCHANGE_RATE as order_date_eur_conversion_rate,
    us_er2.EXCHANGE_RATE as payment_transaction_date_usd_conversion_rate,
    eu_er2.EXCHANGE_RATE as payment_transaction_date_eur_conversion_rate,
    us_er3.EXCHANGE_RATE as shipped_date_usd_conversion_rate,
    us_er2.EXCHANGE_RATE as shipped_date_eur_conversion_rate,
    COALESCE(us_er3.EXCHANGE_RATE,us_er2.EXCHANGE_RATE,us_er.EXCHANGE_RATE) as reporting_usd_conversion_rate,
    COALESCE(eu_er3.EXCHANGE_RATE,eu_er2.EXCHANGE_RATE,eu_er.EXCHANGE_RATE) as reporting_eur_conversion_rate,
    null AS effective_vat_rate,
    COALESCE(old.item_quantity,0),
    -- 订单分摊比例 * 订单总支付金额
    COALESCE(ar.order_allocation_ratio * f.payment_transaction_local_amount,0) AS payment_transaction_local_amount,

    COALESCE(ar.product_subtotal_local_amount,0) as subtotal_excl_tariff_local_amount,
    COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) as tariff_revenue_local_amount,
    COALESCE(ar.product_subtotal_local_amount,0) + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) product_subtotal_local_amount,

    -- 订单分摊比例 * 税
    COALESCE(ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT,0) as tax_local_amount,
    COALESCE(ar.product_discount_local_amount,0),
    null as shipping_cost_local_amount,
    -- 折扣分摊比例 * shipping_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount,0) AS shipping_discount_local_amount,
    -- 订单分摊比例 * shipping_revenue_before_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,0) AS shipping_revenue_before_discount_local_amount,
    -- shipping_revenue_before_discount_local_amount - shipping_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount,0) AS shipping_revenue_local_amount,
    ar.cash_credit_local_amount as cash_credit_local_amount,
    null as cash_membership_credit_local_amount,
    null as cash_refund_credit_local_amount,
    null as cash_giftcard_credit_local_amount,
      -- 订单分摊比例 * cash_giftco_credit_local_amount
    COALESCE(ar.order_allocation_ratio * f.CASH_GIFTCO_CREDIT_LOCAL_AMOUNT,0) AS cash_giftco_credit_local_amount,
    COALESCE(ar.token_count,0)        as token_count,
    COALESCE(ar.token_local_amount,0) as token_local_amount,
    null as cash_token_count,
    null as cash_token_local_amount,
    null as non_cash_token_count,
    null as non_cash_token_local_amount,
    ar.non_cash_credit_local_amount as non_cash_credit_local_amount,
    null as estimated_landed_cost_local_amount,
    null as oracle_cost_local_amount,
    null as lpn_po_cost_local_amount,
    null as po_cost_local_amount,
    null as misc_cogs_local_amount,
    null as estimated_shipping_supplies_cost_local_amount,
    null as estimated_shipping_cost_local_amount,
    null as estimated_variable_warehouse_cost_local_amount,
    null as estimated_variable_gms_cost_local_amount,
    -- 订单分摊比例 * estimated_variable_payment_processing_pct_cash_revenue
    COALESCE(ar.order_allocation_ratio * f.ESTIMATED_VARIABLE_PAYMENT_PROCESSING_PCT_CASH_REVENUE,0) AS estimated_variable_payment_processing_pct_cash_revenue,
    COALESCE(old.price_offered_local_amount,0),
    COALESCE(old.air_vip_price,0),
    COALESCE(old.retail_unit_price,0),
    null as bundle_price_offered_local_amount,
    null as product_price_history_key,
    null as bundle_product_price_history_key,
    null as item_price_key,

    -- amount_to_pay计算
    COALESCE(ar.product_subtotal_local_amount,0) -- subtotal_excl_tariff_local_amount
    + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount,0) +                         -- product_discount_local_amount
    COALESCE( ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) AS amount_to_pay,


    -- cash_gross_revenue_local_amount计算
    COALESCE(ar.order_allocation_ratio * ar.order_total_payment_amount, 0) -
    COALESCE(ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT, 0, 0) AS cash_gross_revenue_local_amount,
    -- product_gross_revenue_local_amount计算
    COALESCE(ar.product_subtotal_local_amount,0) -- subtotal_excl_tariff_local_amount
    + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) -
    0 AS product_gross_revenue_local_amount, -- non_cash_credit_local_amount为null
    -- product_gross_revenue_excl_shipping_local_amount计算
    COALESCE(ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0),0) +
    COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) -
    0 AS product_gross_revenue_excl_shipping_local_amount, -- non_cash_credit_local_amount为null
    -- product_margin_pre_return_local_amount计算
    (COALESCE(ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0),0) +
    0 + -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) -
    0) - -- non_cash_credit_local_amount为null
    0 AS product_margin_pre_return_local_amount, -- estimated_landed_cost_local_amount为null
    -- product_margin_pre_return_excl_shipping_local_amount计算
    ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) +
    0 + -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) -
    0 - -- non_cash_credit_local_amount为null
    0 AS product_margin_pre_return_excl_shipping_local_amount, -- estimated_landed_cost_local_amount为null

    null as group_key,
    null as lpn_code,
    null as custom_printed_text,
    null as reporting_landed_cost_local_amount,
    null as is_actual_landed_cost,
    null as is_fully_landed,
    null as fully_landed_conversion_date,
    null as actual_landed_cost_local_amount,
    null as actual_po_cost_local_amount,
    null as actual_cmt_cost_local_amount,
    null as actual_tariff_duty_cost_local_amount,
    null as actual_freight_cost_local_amount,
    null as actual_other_cost_local_amount,
    null as bounceback_endowment_local_amount,
    null as vip_endowment_local_amount,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime,
    old.is_tariff,
    COALESCE(ar.order_line_discount_local_amount,0)
  FROM order_line_data old
  LEFT JOIN bundle_data bd ON old.order_line_id = bd.id
  LEFT JOIN allocation_ratios ar ON old.order_line_id = ar.order_line_id
  inner join EDW_PROD.NEW_STG.fact_order f on old.order_id = f.ORDER_ID
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er on f.order_local_datetime = us_er.DATE and old.currency_key = us_er.EXCHANGE_FROM and us_er.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er on f.order_local_datetime = eu_er.DATE and old.currency_key = eu_er.EXCHANGE_FROM and eu_er.EXCHANGE_TO = 'EUR'
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er2 on old.order_placed_local_datetime = us_er2.DATE and old.currency_key = us_er2.EXCHANGE_FROM and us_er2.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er2 on old.order_placed_local_datetime = eu_er2.DATE and old.currency_key = eu_er2.EXCHANGE_FROM and eu_er2.EXCHANGE_TO = 'EUR'
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er3 on old.shipped_local_datetime = us_er3.DATE and old.currency_key = us_er3.EXCHANGE_FROM and us_er3.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er3 on old.shipped_local_datetime = eu_er3.DATE and old.currency_key = eu_er3.EXCHANGE_FROM and eu_er3.EXCHANGE_TO = 'EUR'
  where f.order_local_datetime>='2025-10-15'
)
, l_p_4 as (
  WITH TRANSACTION as (
    select order_id,sum(amount) amount
    from LAKE_MMOS.SHOPIFY_JFEU_PROD.TRANSACTION
    where kind in ('capture','sale')
    group by order_id
  )
  ,metafield as (
      SELECT
          owner_id,id,namespace
      FROM
          LAKE_MMOS.SHOPIFY_JFEU_PROD.metafield t2
      WHERE
          t2.value = 'TARIFF'
          and namespace ='MMOS-product'
  )
  ,order_line_data AS (
    -- 合并基础订单行数据、订单信息和产品信息
    SELECT
      ol.id AS order_line_id,
      ol.order_id,
      ol.store_variant_id AS product_id,
      ol.store_master_product_id as master_product_id,
      ol.quantity AS item_quantity,
      PARSE_JSON(ol.price_set):presentment_money:amount::FLOAT * ol.QUANTITY AS product_subtotal_local_amount,
      PARSE_JSON(ol.price_set):presentment_money:amount::FLOAT  AS price_offered_local_amount,
      ol.properties,
      f.customer_id,
      -- o.PROCESSED_AT AS order_local_datetime,
      b.GROUP_CODE AS group_code,
      -- 计算订单总交易金额
      t.amount AS order_total_payment_amount,
      b.VIP_UNIT_PRICE as air_vip_price,
      b.RETAIL_UNIT_PRICE as retail_unit_price,
      f.order_membership_classification_key,
      f.order_sales_channel_key,
      f.order_status_key,
      f.payment_transaction_local_datetime,
      f.shipped_local_datetime,
      f.order_completion_local_datetime,
      o.CURRENCY AS currency_key,
      date(o.PROCESSED_AT) order_placed_local_datetime,
      iff(m.owner_id is not null,1,0) is_tariff,
      f.TAX_LOCAL_AMOUNT
    FROM LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o
    JOIN LAKE_MMOS.SHOPIFY_JFEU_PROD.order_line_store_view ol  ON  o.id = ol.order_id
    LEFT JOIN EDW_PROD.NEW_STG.DIM_PRODUCT b ON ol.store_variant_id = b.product_id
    left join TRANSACTION t on o.id = t.order_id
    left join EDW_PROD.NEW_STG.FACT_ORDER f ON o.id = f.order_id
    left join metafield m on m.owner_id = ol.product_id
    -- where o.PROCESSED_AT >= '2025-09-15'
  )

  ,bundle_data AS (
    -- 解析捆绑商品信息
    SELECT
      ol.id,
      SPLIT_PART(f.value:value::STRING, ':', 1) AS bundle_product_id
    FROM LAKE_MMOS.SHOPIFY_JFEU_PROD.order_line ol,
        LATERAL FLATTEN(input => PARSE_JSON(ol.properties)) f
    WHERE f.value:name::STRING = '_sb_bundle_variant_id_qty'
  )

  -- 计算非会员积分折扣的折扣金额
  ,discount_calculation AS (
    SELECT
      a.id AS order_line_id,
      a.order_id,
      SUM(c.AMOUNT) AS product_discount_local_amount
    FROM LAKE_MMOS.SHOPIFY_JFEU_PROD.order_line a
    LEFT JOIN LAKE_MMOS.SHOPIFY_JFEU_PROD.discount_allocation c
      ON a.id = c.order_line_id
    LEFT JOIN LAKE_MMOS.SHOPIFY_JFEU_PROD.discount_application d
      ON a.order_id = d.order_id
      AND d.index = c.discount_application_index
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_eu".USER_CREDIT_SHARD_ALL b
      ON d.code = b."promotion_code"
    WHERE b."promotion_code" IS NULL
    GROUP BY a.id, a.order_id
  )

  -- 非vip_credit的折扣金额取shopify的折扣金额
  -- vip_credit的金额取credit的金额进行分摊
  ,order_line_discount_and_product_amount  as (
  select
        a.order_id
      ,c.order_line_id
      , COALESCE(d.code,d.title)
      ,PARSE_JSON(a.price_set):presentment_money:amount::FLOAT *a.QUANTITY order_line_price
      ,e."price" price
      ,iff(e."id" is not null,0,c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT) as order_line_discount_local_amount -- 非vip_credit的折扣金额取shopify的折扣金额
      -- 分摊比例
      ,(a.price * a.quantity) / sum(a.price * a.quantity) over(partition by a.order_id, COALESCE(d.code,d.title)) as ratio
      -- 分摊后的 price
      ,( (a.price * a.quantity) / sum(a.price * a.quantity) over(partition by a.order_id,  COALESCE(d.code,d.title)) ) * e."price" as product_subtotal_local_amount -- vip_credit的金额取credit的金额进行分摊
      ,e."id" vip_credit_id
  from LAKE_MMOS.SHOPIFY_JFEU_PROD.order_line  a
  join LAKE_MMOS.SHOPIFY_JFEU_PROD.discount_allocation c
      on a.id=c.order_line_id
  join LAKE_MMOS.SHOPIFY_JFEU_PROD.discount_application d
      on a.order_id=d.order_id and d.index=c.discount_application_index
        and d.target_type='line_item' and d.target_selection<>'all'
  left join LAKE_MMOS."mmos_membership_marketing_eu".USER_CREDIT_SHARD_ALL b
      on  COALESCE(d.code,d.title)=b."promotion_code"
  left join LAKE_MMOS."mmos_membership_marketing_eu".vip_credit_balance_detail_shard_all e
      on cast(b."token_id" as varchar) = e."id"
  )

  -- 计算订单分摊比例
  ,allocation_ratios AS (
  select 
   order_line_id
  ,order_id
  ,product_subtotal_local_amount - COALESCE(order_allocation_ratio * TAX_LOCAL_AMOUNT,0) as product_subtotal_local_amount
  ,product_discount_local_amount
  ,order_line_discount_local_amount
  ,order_allocation_ratio
  ,order_total_payment_amount
  ,token_count
  ,token_local_amount
  from (
    SELECT
      old.order_line_id,
      old.order_id,
      COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) as product_subtotal_local_amount,
      COALESCE(dc.product_discount_local_amount,0) product_discount_local_amount ,
      COALESCE(oa.order_line_discount_local_amount,0) order_line_discount_local_amount, -- 非vip_credit的折扣金额取shopify的折扣金额
      CASE
        WHEN old.is_tariff = 1 then 0
        when SUM(iff(old.is_tariff = 1 , 0,COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) - COALESCE(oa.order_line_discount_local_amount,0))
                    ) OVER (PARTITION BY old.order_id) > 0 THEN
          (COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount)  - COALESCE(oa.order_line_discount_local_amount,0))
          /(SUM(iff(old.is_tariff = 1 , 0,COALESCE(oa.product_subtotal_local_amount,old.product_subtotal_local_amount) - COALESCE(oa.order_line_discount_local_amount,0))
                    ) OVER (PARTITION BY old.order_id))
        ELSE 0
      END AS order_allocation_ratio
      ,COALESCE(old.order_total_payment_amount,0) order_total_payment_amount
      ,iff(oa.vip_credit_id is not null,oa.ratio,0)                         as token_count
      ,iff(oa.vip_credit_id is not null,oa.product_subtotal_local_amount,0) as token_local_amount
    ,iff(oa.vip_credit_id is not null and oa.product_subtotal_local_amount>0,oa.product_subtotal_local_amount,0) as cash_credit_local_amount
    ,iff(oa.vip_credit_id is not null and oa.product_subtotal_local_amount=0,oa.product_subtotal_local_amount,0) as non_cash_credit_local_amount
      ,old.TAX_LOCAL_AMOUNT
    FROM order_line_data old
    LEFT JOIN discount_calculation dc ON old.order_line_id = dc.order_line_id
    LEFT JOIN order_line_discount_and_product_amount oa ON old.order_line_id = oa.order_line_id
    )
  )

  SELECT
    -- 基础字段
    old.order_line_id,
    old.order_id,
    usasi.store_id,
    old.customer_id,
    old.product_id,
    old.master_product_id,
    bd.bundle_product_id,
    null as bundle_component_product_id,
    old.group_code,
    null as bundle_component_history_key,
    1 as product_type_key,
    null as bundle_order_line_id,
    old.order_membership_classification_key,
    old.order_sales_channel_key,
    6 as order_line_status_key,
    old.order_status_key,
    null as order_product_source_key,
    null as currency_key,
    null as administrator_id,
    null as warehouse_id,
    null as shipping_address_id,
    null as billing_address_id,
    null as bounceback_endowment_id,
    null as cost_source_id,
    null as cost_source,
    f.order_local_datetime,
    old.payment_transaction_local_datetime,
    old.shipped_local_datetime,
    old.order_completion_local_datetime,
    us_er.EXCHANGE_RATE as order_date_usd_conversion_rate,
    eu_er.EXCHANGE_RATE as order_date_eur_conversion_rate,
    us_er2.EXCHANGE_RATE as payment_transaction_date_usd_conversion_rate,
    eu_er2.EXCHANGE_RATE as payment_transaction_date_eur_conversion_rate,
    us_er3.EXCHANGE_RATE as shipped_date_usd_conversion_rate,
    us_er2.EXCHANGE_RATE as shipped_date_eur_conversion_rate,
    COALESCE(us_er3.EXCHANGE_RATE,us_er2.EXCHANGE_RATE,us_er.EXCHANGE_RATE) as reporting_usd_conversion_rate,
    COALESCE(eu_er3.EXCHANGE_RATE,eu_er2.EXCHANGE_RATE,eu_er.EXCHANGE_RATE) as reporting_eur_conversion_rate,
    null AS effective_vat_rate,
    COALESCE(old.item_quantity,0),
    -- 订单分摊比例 * 订单总支付金额
    COALESCE(ar.order_allocation_ratio * f.payment_transaction_local_amount,0) AS payment_transaction_local_amount,

    COALESCE(ar.product_subtotal_local_amount,0) as subtotal_excl_tariff_local_amount,
    COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) as tariff_revenue_local_amount,
    COALESCE(ar.product_subtotal_local_amount,0) + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) product_subtotal_local_amount,

    -- 订单分摊比例 * 税
    COALESCE(ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT,0) as tax_local_amount,
    COALESCE(ar.product_discount_local_amount,0),
    null as shipping_cost_local_amount,
    -- 折扣分摊比例 * shipping_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount,0) AS shipping_discount_local_amount,
    -- 订单分摊比例 * shipping_revenue_before_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,0) AS shipping_revenue_before_discount_local_amount,
    -- shipping_revenue_before_discount_local_amount - shipping_discount_local_amount
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT,0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount,0) AS shipping_revenue_local_amount,
    ar.cash_credit_local_amount as cash_credit_local_amount,
    null as cash_membership_credit_local_amount,
    null as cash_refund_credit_local_amount,
    null as cash_giftcard_credit_local_amount,
      -- 订单分摊比例 * cash_giftco_credit_local_amount
    COALESCE(ar.order_allocation_ratio * f.CASH_GIFTCO_CREDIT_LOCAL_AMOUNT,0) AS cash_giftco_credit_local_amount,
    COALESCE(ar.token_count,0)        as token_count,
    COALESCE(ar.token_local_amount,0) as token_local_amount,
    null as cash_token_count,
    null as cash_token_local_amount,
    null as non_cash_token_count,
    null as non_cash_token_local_amount,
    ar.non_cash_credit_local_amount as non_cash_credit_local_amount,
    null as estimated_landed_cost_local_amount,
    null as oracle_cost_local_amount,
    null as lpn_po_cost_local_amount,
    null as po_cost_local_amount,
    null as misc_cogs_local_amount,
    null as estimated_shipping_supplies_cost_local_amount,
    null as estimated_shipping_cost_local_amount,
    null as estimated_variable_warehouse_cost_local_amount,
    null as estimated_variable_gms_cost_local_amount,
    -- 订单分摊比例 * estimated_variable_payment_processing_pct_cash_revenue
    COALESCE(ar.order_allocation_ratio * f.ESTIMATED_VARIABLE_PAYMENT_PROCESSING_PCT_CASH_REVENUE,0) AS estimated_variable_payment_processing_pct_cash_revenue,
    COALESCE(old.price_offered_local_amount,0),
    COALESCE(old.air_vip_price,0),
    COALESCE(old.retail_unit_price,0),
    null as bundle_price_offered_local_amount,
    null as product_price_history_key,
    null as bundle_product_price_history_key,
    null as item_price_key,

    -- amount_to_pay计算
    COALESCE(ar.product_subtotal_local_amount,0) -- subtotal_excl_tariff_local_amount
    + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount,0) +                         -- product_discount_local_amount
    COALESCE( ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) AS amount_to_pay,


    -- cash_gross_revenue_local_amount计算
    COALESCE(ar.order_allocation_ratio * ar.order_total_payment_amount, 0) -
    COALESCE(ar.order_allocation_ratio *  f.TAX_LOCAL_AMOUNT, 0, 0) AS cash_gross_revenue_local_amount,
    -- product_gross_revenue_local_amount计算
    COALESCE(ar.product_subtotal_local_amount,0) -- subtotal_excl_tariff_local_amount
    + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) -
    0 AS product_gross_revenue_local_amount, -- non_cash_credit_local_amount为null
    -- product_gross_revenue_excl_shipping_local_amount计算
    COALESCE(ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0),0) +
    COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0)  -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) -
    0 AS product_gross_revenue_excl_shipping_local_amount, -- non_cash_credit_local_amount为null
    -- product_margin_pre_return_local_amount计算
    (COALESCE(ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0),0) +
    0 + -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) +
    COALESCE(ar.order_allocation_ratio * f.SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT, 0) -
    COALESCE(ar.order_allocation_ratio * f.shipping_discount_local_amount, 0) -
    0) - -- non_cash_credit_local_amount为null
    0 AS product_margin_pre_return_local_amount, -- estimated_landed_cost_local_amount为null
    -- product_margin_pre_return_excl_shipping_local_amount计算
    ar.product_subtotal_local_amount + COALESCE(ar.order_allocation_ratio * f.tariff_revenue_local_amount,0) +
    0 + -- tariff_revenue_local_amount
    - COALESCE(ar.product_discount_local_amount, 0) -
    0 - -- non_cash_credit_local_amount为null
    0 AS product_margin_pre_return_excl_shipping_local_amount, -- estimated_landed_cost_local_amount为null

    null as group_key,
    null as lpn_code,
    null as custom_printed_text,
    null as reporting_landed_cost_local_amount,
    null as is_actual_landed_cost,
    null as is_fully_landed,
    null as fully_landed_conversion_date,
    null as actual_landed_cost_local_amount,
    null as actual_po_cost_local_amount,
    null as actual_cmt_cost_local_amount,
    null as actual_tariff_duty_cost_local_amount,
    null as actual_freight_cost_local_amount,
    null as actual_other_cost_local_amount,
    null as bounceback_endowment_local_amount,
    null as vip_endowment_local_amount,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime,
    old.is_tariff,
    COALESCE(ar.order_line_discount_local_amount,0)
  FROM order_line_data old
  LEFT JOIN bundle_data bd ON old.order_line_id = bd.id
  LEFT JOIN allocation_ratios ar ON old.order_line_id = ar.order_line_id
  inner join EDW_PROD.NEW_STG.fact_order f on old.order_id = f.ORDER_ID
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er on f.order_local_datetime = us_er.DATE and old.currency_key = us_er.EXCHANGE_FROM and us_er.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er on f.order_local_datetime = eu_er.DATE and old.currency_key = eu_er.EXCHANGE_FROM and eu_er.EXCHANGE_TO = 'EUR'
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er2 on old.order_placed_local_datetime = us_er2.DATE and old.currency_key = us_er2.EXCHANGE_FROM and us_er2.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er2 on old.order_placed_local_datetime = eu_er2.DATE and old.currency_key = eu_er2.EXCHANGE_FROM and eu_er2.EXCHANGE_TO = 'EUR'
  left join LAKE.mmt.dwd_com_exchange_rate_df us_er3 on old.shipped_local_datetime = us_er3.DATE and old.currency_key = us_er3.EXCHANGE_FROM and us_er3.EXCHANGE_TO = 'USD'
  left join LAKE.mmt.dwd_com_exchange_rate_df eu_er3 on old.shipped_local_datetime = eu_er3.DATE and old.currency_key = eu_er3.EXCHANGE_FROM and eu_er3.EXCHANGE_TO = 'EUR'
  left join user_shard_all_store_id usasi on old.customer_id = usasi."id"
  where f.order_local_datetime>='2025-10-22'
)

select *, store_id as site_id from  l_p_1
union all 
select *, store_id as site_id from  l_p_2
union all
select *, store_id as site_id from  l_p_3
union all
select *, 2000 as site_id from  l_p_4
;























