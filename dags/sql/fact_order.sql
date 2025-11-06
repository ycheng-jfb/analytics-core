truncate table EDW_PROD.NEW_STG.FACT_ORDER;

CREATE OR REPLACE TEMPORARY TABLE EDW_PROD.NEW_STG.USER_VIP_PERIODSS AS
WITH ordered_events AS (
    SELECT
        "user_id" AS user_id,
        "event" AS event,
        DATE("created_at") AS event_time,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY "created_at") AS rn
    FROM EDW_PROD.NEW_STG.USER_LEVEL_RECORDS_SHARD_ALL
    WHERE "event" IN ('join_vip', 'cancel_vip')
),

paired_events AS (
    SELECT
        user_id,
        event,
        event_time AS start_time,
        LEAD(event) OVER (PARTITION BY user_id ORDER BY event_time) AS next_event,
        LEAD(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS next_time
    FROM ordered_events
),

vip_periods AS (
    SELECT
        user_id,
        start_time,
        CASE
            WHEN next_event = 'cancel_vip' THEN next_time
            ELSE DATE('9999-12-31')
        END AS end_time
    FROM paired_events
    WHERE event = 'join_vip'
)

SELECT
    n.user_id,
    c."user_gid" AS customer_id,
    n.start_time,
    n.end_time
FROM vip_periods n
join EDW_PROD.NEW_STG.USER_SHARD_ALL c on c."id" =n.user_id;




insert into EDW_PROD.NEW_STG.FACT_ORDER
with user_shard_all_store_id as (
    select * from LAKE_MMOS."mmos_membership_marketing_eu".user_shard_all where "is_delete" = 0
),
p1 as (
with orders as (
   with metafield as (
    SELECT
        owner_id
    FROM
        LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.metafield t2
    WHERE
        t2.value = 'TARIFF'
        and namespace ='MMOS-product'
    ),

order_line_flag AS (
    SELECT
            l.order_id,
            max(PARSE_JSON(l.price_set):presentment_money:amount::FLOAT) price
        FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.ORDER_LINE l
        left join metafield m on m.owner_id = l.product_id
        where m.OWNER_ID is not null
        GROUP BY l.order_id
),
unit_count AS (
        select order_id,
               sum(quantity) quantity
        from (
           SELECT distinct
                l.order_id,
                l.id,
                quantity
            FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.ORDER_LINE l
            left join metafield m on m.owner_id = l.product_id
            where m.OWNER_ID is null
        ) t1
        group by order_id
     ),


   -- 新增的折扣计算逻辑
   order_line_discount AS (
    with order_line as (
         SELECT distinct
                l.order_id,
                l.id
            FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.ORDER_LINE l
            left join metafield m on m.owner_id = l.product_id
            where m.OWNER_ID is null
    ),
    t as (
        select a.*, c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT as amount, d.code
        from order_line a
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_ALLOCATION c
            on a.id=c.order_line_id
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_APPLICATION d
            on a.order_id=d.order_id and d.index=c.discount_application_index
    )
    select order_id, sum(amount) as discount
    from t a
    left join LAKE_MMOS."mmos_membership_marketing_shoedazzle".USER_CREDIT_SHARD_ALL b
        on a.code=b."promotion_code"
    where b."promotion_code" is null
    group by 1
),

   tariff_discount_local_amout as (
      with order_line as (
      select
        t1.order_id,t1.ID
        from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_line t1
        left join metafield m on m.owner_id = t1.product_id
        where m.OWNER_ID is not  null ),
        t as (select a.*,c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT as amount,d.code
        from order_line  a
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.discount_allocation c
        on a.id=c.order_line_id
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.discount_application d
        on a.order_id=d.order_id and  d.index=c.discount_application_index )
        select order_id,sum(amount) as discount
        from t a
        left join LAKE_MMOS."mmos_membership_marketing_shoedazzle".USER_CREDIT_SHARD_ALL b--去除credit兑换的
        on a.code=b."promotion_code"
        where b."promotion_code" is null
        group by 1
   )   ,

       -- 分摊逻辑
product_subtotal_logic AS (
    SELECT
        order_id,
        COALESCE(SUM(product_subtotal_local_amount),0) AS product_subtotal_local_amount,
        COALESCE(SUM(case when vip_credit_id is not null then ratio else 0 end ),0) AS token_count,
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount>0
                                                                   then ratio else 0 end ),0) AS cash_credit_count,        
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount=0
                                                                   then ratio else 0 end ),0) AS non_cash_credit_count,
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount>0
                                            then product_subtotal_local_amount else 0 end ),0) AS cash_credit_local_amount,        
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount=0
                                            then product_subtotal_local_amount else 0 end ),0) AS non_cash_credit_local_amount,
        COALESCE(SUM(case when vip_credit_id is not null then product_subtotal_local_amount else 0 end ),0) AS credit_local_amount
    FROM (
            with order_line_discount_and_product_amount  as (
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

    SELECT
      old.id,
      old.order_id,
      oa.vip_credit_id,
      oa.ratio,
      COALESCE(oa.product_subtotal_local_amount,PARSE_JSON(old.price_set):presentment_money:amount::FLOAT * old.QUANTITY) as product_subtotal_local_amount
    FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_line old
    LEFT JOIN order_line_discount_and_product_amount oa ON old.id = oa.order_line_id
    ) sub
    GROUP BY order_id
)

 SELECT
    o.id AS order_id,
    max(o.CURRENCY) currency_key,
    max(uc."id") customer_id,
    CASE
        WHEN max(o.FINANCIAL_STATUS) = 'voided' THEN 15325
        WHEN max(o.FINANCIAL_STATUS) = 'pending' THEN 1
        WHEN max(o.FINANCIAL_STATUS) = 'partially_refunded' THEN 2
        WHEN max(o.FINANCIAL_STATUS) = 'refunded' THEN 13
        WHEN max(o.FINANCIAL_STATUS) = 'Authorized' THEN 6
        WHEN max(o.FINANCIAL_STATUS) = 'paid' THEN 12
        WHEN max(o.FINANCIAL_STATUS) = 'Expired' THEN 10
        ELSE -1
    END AS order_payment_status_key,
    max(o.FULFILLMENT_STATUS) FULFILLMENT_STATUS,
    max(f.SHIPMENT_STATUS) SHIPMENT_STATUS ,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT))  order_local_datetime,
    CONVERT_TIMEZONE('America/Los_Angeles',max(case when t.kind  in ('capture','sale') then t.CREATED_AT end)) payment_transaction_local_datetime,
    -- 优化unit_count计算，确保与简单查询一致
    max(ol.quantity) unit_count,
    max(case when t.kind  in ('capture','sale') then t.amount end) payment_transaction_local_amount,
    max(p2.product_subtotal_local_amount) product_subtotal_local_amount,
    max(PARSE_JSON(o.total_tax_set):presentment_money:amount::FLOAT) tax_local_amount,
    max(case when t.kind  in ('capture','sale')and t.GATEWAY = 'gift_card' then t.amount else 0 end) cash_giftco_credit_local_amount,
    count(distinct case when t.kind  in ('capture','sale') and t.GATEWAY = 'gift_card' then t.id end) cash_giftco_credit_count,
    max(PARSE_JSON(osl.price_set):presentment_money:amount::FLOAT) shipping_revenue_before_discount_local_amount,
    max(ol_disc.discount) AS product_discount_local_amount,
    max(case
        when da_shipping.VALUE_TYPE = 'percentage' then PARSE_JSON(osl.price_set):presentment_money:amount::FLOAT * da_shipping.VALUE / 100
        when da_shipping.VALUE_TYPE = 'fixed_amount' then da_shipping.VALUE
        else 0
    end) AS shipping_discount_local_amount,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) order_placed_local,
    max(p2.product_subtotal_local_amount) -
       COALESCE(
       COALESCE(max(ot.price), 0) - COALESCE(max(tariff_discount_local_amout.discount), 0),
       0
   ) as subtotal_excl_tariff_local_amount,
    max(t.PAYMENT_CREDIT_CARD_COMPANY ) PAYMENT_CREDIT_CARD_COMPANY,
    max(CONVERT_TIMEZONE('America/Los_Angeles', f.CREATED_AT)) shipped_local_datetime,
    max(o.FINANCIAL_STATUS) AS FINANCIAL_STATUS,
    count(distinct ucs."promotion_code") as credit_count,
    sum(p2.credit_local_amount) as credit_local_amount,
    sum(case when tag.value='Dropship Fulfillment' then l.quantity else 0 end) as third_party_unit_count,
    COALESCE(COALESCE(max(ot.price),0) -- tariff_revenue_before_discount_local_amount
        - COALESCE(max(tariff_discount_local_amout.discount),0) --tariff_discount_local_amout
        , 0) as tariff_revenue_local_amount,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) order_placed_local_datetime,
    max(PARSE_JSON(o.TOTAL_LINE_ITEMS_PRICE_SET):presentment_money:amount::FLOAT) AS shopify_total_line_items_price,
    max(tariff_discount_local_amout.discount) AS tariff_discount_local_amout,
    max(ot.price) tariff_revenue_before_discount_local_amount,
    max(cash_credit_count) cash_credit_count,
    max(non_cash_credit_count) non_cash_credit_count,
    max(cash_credit_local_amount) cash_credit_local_amount,
    max(non_cash_credit_local_amount) non_cash_credit_local_amount
FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o
left join LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_shard_all" uc on o.CUSTOMER_ID = uc."user_gid"
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.ORDER_LINE l on o.id = l.order_id
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.FULFILLMENT f on o.id = f.order_id
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.TRANSACTION t on o.id = t.order_id
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.ORDER_SHIPPING_LINE osl on o.id = osl.order_id
-- 折扣应用表连接
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_APPLICATION da on o.id = da.ORDER_ID
-- 排除在user_credit_shard_000000中存在的promotion_code
left join LAKE_MMOS."mmos_membership_marketing_shoedazzle"."USER_CREDIT_SHARD_ALL" ucs on da.code = ucs."promotion_code"
-- 仅包含shipping_line
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_APPLICATION da_shipping on o.id = da_shipping.order_id and da_shipping.TARGET_TYPE = 'shipping_line'
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_tag tag on o.id = tag.order_id  and tag.value != 'Test Order'
-- 移除原来的order_line_flag关联，改用子查询方式
left join order_line_discount ol_disc on o.id = ol_disc.order_id
left join order_line_flag ot on o.id = ot.order_id
left join unit_count ol on o.id = ol.order_id
left join product_subtotal_logic p2 on o.id = p2.order_id
left join tariff_discount_local_amout on o.id = tariff_discount_local_amout.order_id
group by o.id
),

-- 添加order_sales_channel逻辑
 order_sales_channel_data as (
   with t as (
        select o.id,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        else 'Online Order' end as order_sales_channel_l1,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        else 'Web Order' end as order_sales_channel_l2,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        when ot.value2='Exchange-Order' then 'Exchange'
        when ot.value3='Reship-Order' then 'Reship'
        else 'Product Order' end as order_classification_l1,
        case when ot.value1='Ordergroove Subscription Order' then 'Token Billing'
        else order_classification_l1 end as order_classification_l2,
        false as is_border_free_order,
        false as is_custom_order,
        false as is_ps_order,
        false as is_retail_ship_only_order,
        case when ot.value4='Test Order' then true
        else false end as is_test_order,
        false as is_preorder,
        false as is_product_seeding_order,
        false as is_bops_order,
        false as is_discreet_packaging,
        false as is_bill_me_now_online,
        false as is_bill_me_now_gms,
        false as is_membership_gift,
        false as is_warehouse_outlet_order,
        case when ot.value5='Dropship Fulfillment' then true else false end as is_third_party
        from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o
        left join (
            select
              ORDER_ID,
              max(case when VALUE ='Ordergroove Subscription Order' then 'Ordergroove Subscription Order' else null end) as value1,
              max(case when VALUE ='Exchange-Order' then 'Exchange-Order' else null end) as value2,
              max(case when VALUE ='Reship-Order' then 'Reship-Order' else null end) as value3,
              max(case when VALUE ='Test Order' then 'Test Order' else null end) as value4,
              max(case when VALUE ='Dropship Fulfillment' then 'Dropship Fulfillment' else null end) as value5
            from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_tag
            group by order_id
        ) ot
        on o.id=ot.order_id
    )
    select distinct  t.id, max(d.order_sales_channel_key) order_sales_channel_key
    from t
    left join EDW_PROD.STG.dim_order_sales_channel d
    on t.order_sales_channel_l1=d.order_sales_channel_l1
    AND t.order_sales_channel_l2 = d.ORDER_SALES_CHANNEL_L2
    and t.order_classification_l1=d.order_classification_l1
    AND t.order_classification_l2 = d.ORDER_CLASSIFICATION_L2
    AND t.is_border_free_order = d.is_border_free_order
    AND t.is_custom_order = d.is_custom_order
    AND t.is_ps_order = d.is_ps_order
    AND t.is_retail_ship_only_order = d.is_retail_ship_only_order
    AND t.is_test_order = d.is_test_order
    AND t.is_preorder = d.is_preorder
    AND t.is_product_seeding_order = d.is_product_seeding_order
    AND t.is_bops_order = d.is_bops_order
    AND t.is_discreet_packaging = d.is_discreet_packaging
    AND t.is_bill_me_now_online = d.is_bill_me_now_online
    AND t.is_bill_me_now_gms = d.is_bill_me_now_gms
    AND t.is_membership_gift = d.is_membership_gift
    AND t.is_warehouse_outlet_order = d.is_warehouse_outlet_order
    AND t.is_third_party = d.is_third_party
    group by t.id

),
     t1 as(
select
      o.id order_id
from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_tag ot on o.id = ot.order_id
where ot.value = 'Test Order'
group by o.id
),

-- 添加order_status_key逻辑
order_status_data as (
    select t.ID,max(t.order_status_key) order_status_key
    from (
    select
       o.id,
     CASE
         WHEN UPPER(ot.value) = 'DROPSHIP FULFILLMENT' THEN 8
         WHEN UPPER(o.FULFILLMENT_STATUS) = 'FULFILLED' THEN 1
         WHEN UPPER(o.FINANCIAL_STATUS) IN ('PAID', 'AUTHORIZED') THEN 1
         ELSE -1
     END AS order_status_key
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o
    left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_tag ot on o.id = ot.order_id
    where UPPER(ot.value) != 'TEST ORDER' or ot.value is null
         ) t
    group by t.ID
),

vip_first_order_logic as (
    select
          t.id,
       max(vip_order_membership_classification_key) vip_order_membership_classification_key
    from (
        select
            o.id,
            CASE
                -- 情况1: VIP首单且从未成为过VIP
                WHEN oto.value = 'Ordergroove Trigger Order'
                     AND NOT EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                     )
                THEN 2
                -- 情况2: VIP首单但之前成为过VIP（包括已取消的）
                WHEN oto.value = 'Ordergroove Trigger Order'
                     AND EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE  ulr.customer_id = o.customer_id
                     )
                THEN 5

                -- 情况3: 下订单时非VIP状态，且是第一次购买商品，但是非VIP首单
                WHEN oto.value != 'Ordergroove Trigger Order'
                  and NOT EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                          AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                THEN 1

                -- 情况4: 下订单时VIP状态，非首次购买商品
                WHEN  EXISTS (
                         SELECT 1
                         FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                      WHERE ulr.customer_id = o.customer_id
                           AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                THEN 4

                -- 情况5: 下订单时非VIP状态，非首次购买，但是以前是VIP，在购买商品之前取消了
                WHEN  EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS ( --在购买商品之前取消了 非VIP状态
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                           AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                    AND  EXISTS (  --但是以前是VIP
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                     )
                THEN 3

                -- 情况6: 下订单时非VIP状态，非首次购买，从未成为过VIP
                WHEN
                    EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS ( --从未成为过VIP
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                      WHERE ulr.user_id = o.customer_id
                     )

                THEN 6
                ELSE 1
            END AS vip_order_membership_classification_key
        from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD."ORDER" o
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.order_tag oto on o.id = oto.order_id
    ) t
    group by  t.id
)
select
    orders.order_id,
    COALESCE(fa.membership_event_key, fme.membership_event_key, -1) AS membership_event_key,
    COALESCE(fa.activation_key, -1) AS activation_key,
    COALESCE(ffa.activation_key, -1) AS first_activation_key,
    orders.currency_key,
    orders.customer_id,
    55 as store_id,
    55 as cart_store_id,
    null as membership_brand_id,
    null as administrator_id,
    null as master_order_id,
    null order_payment_status_key,
    21 as order_processing_status_key,
    vip.vip_order_membership_classification_key AS order_membership_classification_key,
    CASE WHEN (orders.subtotal_excl_tariff_local_amount - orders.product_discount_local_amount - orders.credit_local_amount) > 0 THEN 1
         ELSE 0
    END AS is_cash,
    null AS is_credit_billing_on_retry,
    null AS shipping_address_id,
    null AS billing_address_id,
    osd.order_status_key AS order_status_key,
    osc.order_sales_channel_key,
    case when orders.PAYMENT_CREDIT_CARD_COMPANY='Visa' then '28504'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='Mastercard' then '28775'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='American Express' then '10660'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='Discover' then '28734'
    else -1 end AS payment_key,
    null AS order_customer_selected_shipping_key,
    null AS session_id,
    null AS bops_store_id,
    orders.order_local_datetime,
    orders.payment_transaction_local_datetime,
    orders.shipped_local_datetime,
    COALESCE(orders.payment_transaction_local_datetime,orders.shipped_local_datetime) AS order_completion_local_datetime,
    null AS bops_pickup_local_datetime,
    orders.order_placed_local_datetime,
    orders.unit_count,
    null as loyalty_unit_count,
    orders.third_party_unit_count as third_party_unit_count,
    us_er.EXCHANGE_RATE as order_date_usd_conversion_rate,
    eu_er.EXCHANGE_RATE as order_date_eur_conversion_rate,
    us_er2.EXCHANGE_RATE as payment_transaction_date_usd_conversion_rate,
    eu_er2.EXCHANGE_RATE as payment_transaction_date_eur_conversion_rate,
    us_er3.EXCHANGE_RATE as shipped_date_usd_conversion_rate,
    us_er2.EXCHANGE_RATE as shipped_date_eur_conversion_rate,
    COALESCE(us_er3.EXCHANGE_RATE,us_er2.EXCHANGE_RATE,us_er.EXCHANGE_RATE) as reporting_usd_conversion_rate,
    COALESCE(eu_er3.EXCHANGE_RATE,eu_er2.EXCHANGE_RATE,eu_er.EXCHANGE_RATE) as reporting_eur_conversion_rate,
    null as effective_vat_rate,
    orders.payment_transaction_local_amount,
    orders.subtotal_excl_tariff_local_amount,
    orders.tariff_revenue_local_amount,
    orders.product_subtotal_local_amount,
    orders.tax_local_amount,
    null as delivery_fee_local_amount,
    orders.cash_credit_count as cash_credit_count,
    orders.cash_credit_local_amount as cash_credit_local_amount,
    null as cash_membership_credit_local_amount,
    null as cash_membership_credit_count,
    null as cash_refund_credit_local_amount,
    null as cash_refund_credit_count,
    orders.cash_giftco_credit_local_amount,
    orders.cash_giftco_credit_count,
    null as cash_giftcard_credit_local_amount,
    null as cash_giftcard_credit_count,
    null as token_local_amount,
    null as token_count,
    null as cash_token_local_amount,
    null as cash_token_count,
    null as non_cash_token_local_amount,
    null as non_cash_token_count,
    orders.non_cash_credit_local_amount as non_cash_credit_local_amount,
    orders.non_cash_credit_count as non_cash_credit_count,
    orders.shipping_revenue_before_discount_local_amount,
    orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount as shipping_revenue_local_amount,
    null as shipping_cost_local_amount,
    null as shipping_cost_source,
    orders.product_discount_local_amount,
    orders.shipping_discount_local_amount,
    GREATEST(
        IFNULL(orders.subtotal_excl_tariff_local_amount, 0)
        + IFNULL(orders.tariff_revenue_local_amount, 0)
        - IFNULL(orders.product_discount_local_amount, 0)
        + IFNULL(orders.tax_local_amount, 0)
        + IFNULL(orders.shipping_revenue_before_discount_local_amount, 0)
        - IFNULL(orders.shipping_discount_local_amount, 0),
        0
    ) AS amount_to_pay,
        IFNULL(orders.payment_transaction_local_amount, 0)
        - IFNULL(orders.tax_local_amount, 0) AS cash_gross_revenue_local_amount,
    null as product_gross_revenue_local_amount,
    null as product_gross_revenue_excl_shipping_local_amount,
    null as product_margin_pre_return_local_amount,
    null as product_margin_pre_return_excl_shipping_local_amount,
    IFNULL(
        IFNULL(orders.product_subtotal_local_amount, 0)
        - IFNULL(orders.shipping_discount_local_amount, 0)
        - IFNULL(orders.product_discount_local_amount, 0)
        + IFNULL(orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(COALESCE(0, 0), 0),
        IFNULL(
            ifnull(IFNULL(orders.product_subtotal_local_amount, 0)
                - IFNULL(orders.product_discount_local_amount, 0)
                + IFNULL(orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount, 0)
                - IFNULL(0, 0)
                - IFNULL(0, 0) ,
                IFNULL(orders.payment_transaction_local_amount, 0)
                - IFNULL(orders.tax_local_amount, 0)
            ), 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(COALESCE(0, 0), 0)
    ) AS product_order_cash_margin_pre_return_local_amount,
    null as estimated_landed_cost_local_amount,
    null as misc_cogs_local_amount,
    null as reporting_landed_cost_local_amount,
    null as is_actual_landed_cost,
    null as estimated_variable_gms_cost_local_amount,
    null as estimated_variable_warehouse_cost_local_amount,
    null as estimated_variable_payment_processing_pct_cash_revenue,
    null as estimated_shipping_supplies_cost_local_amount,
    null as bounceback_endowment_local_amount,
    null as vip_endowment_local_amount,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime,
    COALESCE(orders.credit_count, 0) AS credit_count,
    COALESCE(orders.credit_local_amount, 0) AS credit_local_amount,
    orders.PAYMENT_CREDIT_CARD_COMPANY,
    orders.financial_status,
    orders.fulfillment_Status,
    orders.SHIPMENT_STATUS,
    COALESCE(orders.shopify_total_line_items_price,0),
    COALESCE(orders.tariff_revenue_before_discount_local_amount,0),
    COALESCE(orders.tariff_discount_local_amout,0)
from orders
left join (
    select *
    from (
        select *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activation_local_datetime) as rn
        from EDW_PROD.DATA_MODEL_JFB.FACT_ACTIVATION
    ) t
    where rn = 1
) fa on orders.customer_id = fa.customer_id

left join (
    select *
    from (
        select *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY EVENT_START_LOCAL_DATETIME) as rn
        from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT
    ) t
    where rn = 1
) fme on orders.customer_id = fme.customer_id
left join (
  select customer_id, activation_key, activation_local_datetime,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activation_local_datetime) as rn
  from EDW_PROD.DATA_MODEL_JFB.FACT_ACTIVATION
  where activation_local_datetime <= (select max(order_local_datetime) from orders)
) ffa
  on orders.customer_id = ffa.customer_id and ffa.rn = 1
left join order_sales_channel_data osc on orders.order_id = osc.id
left join order_status_data osd on orders.order_id = osd.id
left join LAKE.mmt.dwd_com_exchange_rate_df us_er on date(orders.order_local_datetime) = us_er.DATE and orders.currency_key = us_er.EXCHANGE_FROM and us_er.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er on date(orders.order_local_datetime) = eu_er.DATE and orders.currency_key = eu_er.EXCHANGE_FROM and eu_er.EXCHANGE_TO = 'EUR'
left join LAKE.mmt.dwd_com_exchange_rate_df us_er2 on date(orders.order_placed_local_datetime) = us_er2.DATE and orders.currency_key = us_er2.EXCHANGE_FROM and us_er2.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er2 on date(orders.order_placed_local_datetime) = eu_er2.DATE and orders.currency_key = eu_er2.EXCHANGE_FROM and eu_er2.EXCHANGE_TO = 'EUR'
left join LAKE.mmt.dwd_com_exchange_rate_df us_er3 on date(orders.shipped_local_datetime) = us_er3.DATE and orders.currency_key = us_er3.EXCHANGE_FROM and us_er3.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er3 on date(orders.shipped_local_datetime) = eu_er3.DATE and orders.currency_key = eu_er3.EXCHANGE_FROM and eu_er3.EXCHANGE_TO = 'EUR'
left join vip_first_order_logic vip on vip.ID = orders.order_id
left join t1 on t1.order_id = orders.order_id
where t1.order_id is null
and orders.order_local_datetime >='2025-09-15'
)

, p2  as (
with orders as (
   with metafield as (
    SELECT
        owner_id
    FROM
        LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.metafield t2
    WHERE
        t2.value = 'TARIFF'
        and namespace ='MMOS-product'
    ),

order_line_flag AS (
    SELECT
            l.order_id,
            max(PARSE_JSON(l.price_set):presentment_money:amount::FLOAT) price
        FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.ORDER_LINE l
        left join metafield m on m.owner_id = l.product_id
        where m.OWNER_ID is not null
        GROUP BY l.order_id
),
unit_count AS (
        select order_id,
               sum(quantity) quantity
        from (
           SELECT distinct
                l.order_id,
                l.id,
                quantity
            FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.ORDER_LINE l
            left join metafield m on m.owner_id = l.product_id
            where m.OWNER_ID is null
        ) t1
        group by order_id
     ),


   -- 新增的折扣计算逻辑
   order_line_discount AS (
    with order_line as (
         SELECT distinct
                l.order_id,
                l.id
            FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.ORDER_LINE l
            left join metafield m on m.owner_id = l.product_id
            where m.OWNER_ID is null
    ),
    t as (
        select a.*, c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT as amount, d.code
        from order_line a
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_ALLOCATION c
            on a.id=c.order_line_id
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_APPLICATION d
            on a.order_id=d.order_id and d.index=c.discount_application_index
    )
    select order_id, sum(amount) as discount
    from t a
    left join LAKE_MMOS."mmos_membership_marketing_us"."user_credit_shard_all" b
        on a.code=b."promotion_code"
    where b."promotion_code" is null
    group by 1
),

   tariff_discount_local_amout as (
      with order_line as (
      select
        t1.order_id,t1.ID
        from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_line t1
        left join metafield m on m.owner_id = t1.product_id
        where m.OWNER_ID is not  null ),
        t as (select a.*,c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT as amount,d.code
        from order_line  a
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.discount_allocation c
        on a.id=c.order_line_id
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.discount_application d
        on a.order_id=d.order_id and  d.index=c.discount_application_index )
        select order_id,sum(amount) as discount
        from t a
        left join LAKE_MMOS."mmos_membership_marketing_us"."user_credit_shard_all" b--去除credit兑换的
        on a.code=b."promotion_code"
        where b."promotion_code" is null
        group by 1
   )   ,

       -- 分摊逻辑
product_subtotal_logic AS (
    SELECT
        order_id,
        COALESCE(SUM(product_subtotal_local_amount),0) AS product_subtotal_local_amount,
        COALESCE(SUM(case when vip_credit_id is not null then ratio else 0 end ),0) AS token_count,
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount>0
                                                                   then ratio else 0 end ),0) AS cash_credit_count,        
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount=0
                                                                   then ratio else 0 end ),0) AS non_cash_credit_count,
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount>0
                                            then product_subtotal_local_amount else 0 end ),0) AS cash_credit_local_amount,        
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount=0
                                            then product_subtotal_local_amount else 0 end ),0) AS non_cash_credit_local_amount,
        COALESCE(SUM(case when vip_credit_id is not null then product_subtotal_local_amount else 0 end ),0) AS credit_local_amount
    FROM (
        with order_line_discount_and_product_amount  as (
select
      a.order_id
    ,c.order_line_id
    , COALESCE(d.code,d.title)
    ,PARSE_JSON(a.price_set):presentment_money:amount::FLOAT * a.QUANTITY  order_line_price
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

SELECT
  old.id,
  old.order_id,
  oa.vip_credit_id,
      oa.ratio,
  COALESCE(oa.product_subtotal_local_amount,PARSE_JSON(old.price_set):presentment_money:amount::FLOAT * old.QUANTITY) as product_subtotal_local_amount
FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_line old
LEFT JOIN order_line_discount_and_product_amount oa ON old.id = oa.order_line_id
    ) sub
    GROUP BY order_id
)

 SELECT
    o.id AS order_id,
    max(o.CURRENCY) currency_key,
    max(uc."id") customer_id,
    CASE
        WHEN max(o.FINANCIAL_STATUS) = 'voided' THEN 15325
        WHEN max(o.FINANCIAL_STATUS) = 'pending' THEN 1
        WHEN max(o.FINANCIAL_STATUS) = 'partially_refunded' THEN 2
        WHEN max(o.FINANCIAL_STATUS) = 'refunded' THEN 13
        WHEN max(o.FINANCIAL_STATUS) = 'Authorized' THEN 6
        WHEN max(o.FINANCIAL_STATUS) = 'paid' THEN 12
        WHEN max(o.FINANCIAL_STATUS) = 'Expired' THEN 10
        ELSE -1
    END AS order_payment_status_key,
    max(o.FULFILLMENT_STATUS) FULFILLMENT_STATUS,
    max(f.SHIPMENT_STATUS) SHIPMENT_STATUS ,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT))  order_local_datetime,
    CONVERT_TIMEZONE('America/Los_Angeles',max(case when t.kind  in ('capture','sale') then t.CREATED_AT end)) payment_transaction_local_datetime,
    -- 优化unit_count计算，确保与简单查询一致
    max(ol.quantity) unit_count,
    max(case when t.kind  in ('capture','sale') then t.amount end) payment_transaction_local_amount,
    max(p2.product_subtotal_local_amount) product_subtotal_local_amount,
    max(PARSE_JSON(o.total_tax_set):presentment_money:amount::FLOAT) tax_local_amount,
    max(case when t.kind  in ('capture','sale')and t.GATEWAY = 'gift_card' then t.amount else 0 end) cash_giftco_credit_local_amount,
    count(distinct case when t.kind  in ('capture','sale') and t.GATEWAY = 'gift_card' then t.id end) cash_giftco_credit_count,
    max(PARSE_JSON(osl.price_set):presentment_money:amount::FLOAT) shipping_revenue_before_discount_local_amount,
    max(ol_disc.discount) AS product_discount_local_amount,
    max(case
        when da_shipping.VALUE_TYPE = 'percentage' then PARSE_JSON(osl.price_set):presentment_money:amount::FLOAT * da_shipping.VALUE / 100
        when da_shipping.VALUE_TYPE = 'fixed_amount' then da_shipping.VALUE
        else 0
    end) AS shipping_discount_local_amount,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) order_placed_local,
    max(p2.product_subtotal_local_amount) -
       COALESCE(
       COALESCE(max(ot.price), 0) - COALESCE(max(tariff_discount_local_amout.discount), 0),
       0
   ) as subtotal_excl_tariff_local_amount,
    max(t.PAYMENT_CREDIT_CARD_COMPANY ) PAYMENT_CREDIT_CARD_COMPANY,
    max(CONVERT_TIMEZONE('America/Los_Angeles', f.CREATED_AT)) shipped_local_datetime,
    max(o.FINANCIAL_STATUS) AS FINANCIAL_STATUS,
    count(distinct ucs."promotion_code") as credit_count,
    sum(p2.credit_local_amount) as credit_local_amount,
    sum(case when tag.value='Dropship Fulfillment' then l.quantity else 0 end) as third_party_unit_count,
    COALESCE(COALESCE(max(ot.price),0) -- tariff_revenue_before_discount_local_amount
        - COALESCE(max(tariff_discount_local_amout.discount),0) --tariff_discount_local_amout
        , 0) as tariff_revenue_local_amount,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) order_placed_local_datetime,
    max(PARSE_JSON(o.TOTAL_LINE_ITEMS_PRICE_SET):presentment_money:amount::FLOAT) AS shopify_total_line_items_price,
    max(tariff_discount_local_amout.discount) AS tariff_discount_local_amout,
    max(ot.price) tariff_revenue_before_discount_local_amount,
    max(cash_credit_count) cash_credit_count,
    max(non_cash_credit_count) non_cash_credit_count,
    max(cash_credit_local_amount) cash_credit_local_amount,
    max(non_cash_credit_local_amount) non_cash_credit_local_amount
FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o
left join LAKE_MMOS."mmos_membership_marketing_us"."user_shard_all" uc on o.CUSTOMER_ID = uc."user_gid"
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.ORDER_LINE l on o.id = l.order_id
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.FULFILLMENT f on o.id = f.order_id
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.TRANSACTION t on o.id = t.order_id
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.ORDER_SHIPPING_LINE osl on o.id = osl.order_id
-- 折扣应用表连接
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_APPLICATION da on o.id = da.ORDER_ID
-- 排除在user_credit_shard_000000中存在的promotion_code
left join LAKE_MMOS."mmos_membership_marketing_us"."user_credit_shard_all" ucs on da.code = ucs."promotion_code"
-- 仅包含shipping_line
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_APPLICATION da_shipping on o.id = da_shipping.order_id and da_shipping.TARGET_TYPE = 'shipping_line'
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_tag tag on o.id = tag.order_id  and tag.value != 'Test Order'
-- 移除原来的order_line_flag关联，改用子查询方式
left join order_line_discount ol_disc on o.id = ol_disc.order_id
left join order_line_flag ot on o.id = ot.order_id
left join unit_count ol on o.id = ol.order_id
left join product_subtotal_logic p2 on o.id = p2.order_id
left join tariff_discount_local_amout on o.id = tariff_discount_local_amout.order_id
group by o.id
),

-- 添加order_sales_channel逻辑
 order_sales_channel_data as (
   with t as (
        select o.id,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        else 'Online Order' end as order_sales_channel_l1,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        else 'Web Order' end as order_sales_channel_l2,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        when ot.value2='Exchange-Order' then 'Exchange'
        when ot.value3='Reship-Order' then 'Reship'
        else 'Product Order' end as order_classification_l1,
        case when ot.value1='Ordergroove Subscription Order' then 'Token Billing'
        else order_classification_l1 end as order_classification_l2,
        false as is_border_free_order,
        false as is_custom_order,
        false as is_ps_order,
        false as is_retail_ship_only_order,
        case when ot.value4='Test Order' then true
        else false end as is_test_order,
        false as is_preorder,
        false as is_product_seeding_order,
        false as is_bops_order,
        false as is_discreet_packaging,
        false as is_bill_me_now_online,
        false as is_bill_me_now_gms,
        false as is_membership_gift,
        false as is_warehouse_outlet_order,
        case when ot.value5='Dropship Fulfillment' then true else false end as is_third_party
        from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o
        left join (
            select
              ORDER_ID,
              max(case when VALUE ='Ordergroove Subscription Order' then 'Ordergroove Subscription Order' else null end) as value1,
              max(case when VALUE ='Exchange-Order' then 'Exchange-Order' else null end) as value2,
              max(case when VALUE ='Reship-Order' then 'Reship-Order' else null end) as value3,
              max(case when VALUE ='Test Order' then 'Test Order' else null end) as value4,
              max(case when VALUE ='Dropship Fulfillment' then 'Dropship Fulfillment' else null end) as value5
            from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_tag
            group by order_id
        ) ot
        on o.id=ot.order_id
    )
    select distinct  t.id, max(d.order_sales_channel_key) order_sales_channel_key
    from t
    left join EDW_PROD.STG.dim_order_sales_channel d
    on t.order_sales_channel_l1=d.order_sales_channel_l1
    AND t.order_sales_channel_l2 = d.ORDER_SALES_CHANNEL_L2
    and t.order_classification_l1=d.order_classification_l1
    AND t.order_classification_l2 = d.ORDER_CLASSIFICATION_L2
    AND t.is_border_free_order = d.is_border_free_order
    AND t.is_custom_order = d.is_custom_order
    AND t.is_ps_order = d.is_ps_order
    AND t.is_retail_ship_only_order = d.is_retail_ship_only_order
    AND t.is_test_order = d.is_test_order
    AND t.is_preorder = d.is_preorder
    AND t.is_product_seeding_order = d.is_product_seeding_order
    AND t.is_bops_order = d.is_bops_order
    AND t.is_discreet_packaging = d.is_discreet_packaging
    AND t.is_bill_me_now_online = d.is_bill_me_now_online
    AND t.is_bill_me_now_gms = d.is_bill_me_now_gms
    AND t.is_membership_gift = d.is_membership_gift
    AND t.is_warehouse_outlet_order = d.is_warehouse_outlet_order
    AND t.is_third_party = d.is_third_party
    group by t.id

),
     t1 as(
select
      o.id order_id
from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_tag ot on o.id = ot.order_id
where ot.value = 'Test Order'
group by o.id
),

-- 添加order_status_key逻辑
order_status_data as (
    select t.ID,max(t.order_status_key) order_status_key
    from (
    select
       o.id,
     CASE
         WHEN UPPER(ot.value) = 'DROPSHIP FULFILLMENT' THEN 8
         WHEN UPPER(o.FULFILLMENT_STATUS) = 'FULFILLED' THEN 1
         WHEN UPPER(o.FINANCIAL_STATUS) IN ('PAID', 'AUTHORIZED') THEN 1
         ELSE -1
     END AS order_status_key
    from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o
    left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_tag ot on o.id = ot.order_id
    where UPPER(ot.value) != 'TEST ORDER' or ot.value is null
         ) t
    group by t.ID
),


vip_first_order_logic as (
    select
          t.id,
       max(vip_order_membership_classification_key) vip_order_membership_classification_key
    from (
        select
            o.id,
            CASE
                -- 情况1: VIP首单且从未成为过VIP
                WHEN oto.value = 'Ordergroove Trigger Order'
                     AND NOT EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                     )
                THEN 2
                -- 情况2: VIP首单但之前成为过VIP（包括已取消的）
                WHEN oto.value = 'Ordergroove Trigger Order'
                     AND EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE  ulr.customer_id = o.customer_id
                     )
                THEN 5

                -- 情况3: 下订单时非VIP状态，且是第一次购买商品，但是非VIP首单
                WHEN oto.value != 'Ordergroove Trigger Order'
                  and NOT EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                          AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                THEN 1

                -- 情况4: 下订单时VIP状态，非首次购买商品
                WHEN  EXISTS (
                         SELECT 1
                         FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                      WHERE ulr.customer_id = o.customer_id
                           AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                THEN 4

                -- 情况5: 下订单时非VIP状态，非首次购买，但是以前是VIP，在购买商品之前取消了
                WHEN  EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS ( --在购买商品之前取消了 非VIP状态
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                           AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                    AND  EXISTS (  --但是以前是VIP
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                     )
                THEN 3

                -- 情况6: 下订单时非VIP状态，非首次购买，从未成为过VIP
                WHEN
                    EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS ( --从未成为过VIP
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                      WHERE ulr.user_id = o.customer_id
                     )

                THEN 6
                ELSE 1
            END AS vip_order_membership_classification_key
        from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD."ORDER" o
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.order_tag oto on o.id = oto.order_id
    ) t
    group by  t.id
    )

select
    orders.order_id,
    COALESCE(fa.membership_event_key, fme.membership_event_key, -1) AS membership_event_key,
    COALESCE(fa.activation_key, -1) AS activation_key,
    COALESCE(ffa.activation_key, -1) AS first_activation_key,
    orders.currency_key,
    orders.customer_id,
    26 as store_id,
    26 as cart_store_id,
    null as membership_brand_id,
    null as administrator_id,
    null as master_order_id,
    null order_payment_status_key,
    21 as order_processing_status_key,
    vip.vip_order_membership_classification_key AS order_membership_classification_key,
    CASE WHEN (orders.subtotal_excl_tariff_local_amount - orders.product_discount_local_amount - orders.credit_local_amount) > 0 THEN 1
         ELSE 0
    END AS is_cash,
    null AS is_credit_billing_on_retry,
    null AS shipping_address_id,
    null AS billing_address_id,
    osd.order_status_key AS order_status_key,
    osc.order_sales_channel_key,
    case when orders.PAYMENT_CREDIT_CARD_COMPANY='Visa' then '28504'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='Mastercard' then '28775'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='American Express' then '10660'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='Discover' then '28734'
    else -1 end AS payment_key,
    null AS order_customer_selected_shipping_key,
    null AS session_id,
    null AS bops_store_id,
    orders.order_local_datetime,
    orders.payment_transaction_local_datetime,
    orders.shipped_local_datetime,
    COALESCE(orders.payment_transaction_local_datetime,orders.shipped_local_datetime) AS order_completion_local_datetime,
    null AS bops_pickup_local_datetime,
    orders.order_placed_local_datetime,
    orders.unit_count,
    null as loyalty_unit_count,
    orders.third_party_unit_count as third_party_unit_count,
    us_er.EXCHANGE_RATE as order_date_usd_conversion_rate,
    eu_er.EXCHANGE_RATE as order_date_eur_conversion_rate,
    us_er2.EXCHANGE_RATE as payment_transaction_date_usd_conversion_rate,
    eu_er2.EXCHANGE_RATE as payment_transaction_date_eur_conversion_rate,
    us_er3.EXCHANGE_RATE as shipped_date_usd_conversion_rate,
    us_er2.EXCHANGE_RATE as shipped_date_eur_conversion_rate,
    COALESCE(us_er3.EXCHANGE_RATE,us_er2.EXCHANGE_RATE,us_er.EXCHANGE_RATE) as reporting_usd_conversion_rate,
    COALESCE(eu_er3.EXCHANGE_RATE,eu_er2.EXCHANGE_RATE,eu_er.EXCHANGE_RATE) as reporting_eur_conversion_rate,
    null as effective_vat_rate,
    orders.payment_transaction_local_amount,
    orders.subtotal_excl_tariff_local_amount,
    orders.tariff_revenue_local_amount,
    orders.product_subtotal_local_amount,
    orders.tax_local_amount,
    null as delivery_fee_local_amount,
    orders.cash_credit_count as cash_credit_count,
    orders.cash_credit_local_amount as cash_credit_local_amount,
    null as cash_membership_credit_local_amount,
    null as cash_membership_credit_count,
    null as cash_refund_credit_local_amount,
    null as cash_refund_credit_count,
    orders.cash_giftco_credit_local_amount,
    orders.cash_giftco_credit_count,
    null as cash_giftcard_credit_local_amount,
    null as cash_giftcard_credit_count,
    null as token_local_amount,
    null as token_count,
    null as cash_token_local_amount,
    null as cash_token_count,
    null as non_cash_token_local_amount,
    null as non_cash_token_count,
    orders.non_cash_credit_local_amount as non_cash_credit_local_amount,
    orders.non_cash_credit_count as non_cash_credit_count,
    orders.shipping_revenue_before_discount_local_amount,
    orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount as shipping_revenue_local_amount,
    null as shipping_cost_local_amount,
    null as shipping_cost_source,
    orders.product_discount_local_amount,
    orders.shipping_discount_local_amount,
    GREATEST(
        IFNULL(orders.subtotal_excl_tariff_local_amount, 0)
        + IFNULL(orders.tariff_revenue_local_amount, 0)
        - IFNULL(orders.product_discount_local_amount, 0)
        + IFNULL(orders.tax_local_amount, 0)
        + IFNULL(orders.shipping_revenue_before_discount_local_amount, 0)
        - IFNULL(orders.shipping_discount_local_amount, 0),
        0
    ) AS amount_to_pay,
        IFNULL(orders.payment_transaction_local_amount, 0)
        - IFNULL(orders.tax_local_amount, 0) AS cash_gross_revenue_local_amount,
    null as product_gross_revenue_local_amount,
    null as product_gross_revenue_excl_shipping_local_amount,
    null as product_margin_pre_return_local_amount,
    null as product_margin_pre_return_excl_shipping_local_amount,
    IFNULL(
        IFNULL(orders.product_subtotal_local_amount, 0)
        - IFNULL(orders.shipping_discount_local_amount, 0)
        - IFNULL(orders.product_discount_local_amount, 0)
        + IFNULL(orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(COALESCE(0, 0), 0),
        IFNULL(
            ifnull(IFNULL(orders.product_subtotal_local_amount, 0)
                - IFNULL(orders.product_discount_local_amount, 0)
                + IFNULL(orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount, 0)
                - IFNULL(0, 0)
                - IFNULL(0, 0) ,
                IFNULL(orders.payment_transaction_local_amount, 0)
                - IFNULL(orders.tax_local_amount, 0)
            ), 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(COALESCE(0, 0), 0)
    ) AS product_order_cash_margin_pre_return_local_amount,
    null as estimated_landed_cost_local_amount,
    null as misc_cogs_local_amount,
    null as reporting_landed_cost_local_amount,
    null as is_actual_landed_cost,
    null as estimated_variable_gms_cost_local_amount,
    null as estimated_variable_warehouse_cost_local_amount,
    null as estimated_variable_payment_processing_pct_cash_revenue,
    null as estimated_shipping_supplies_cost_local_amount,
    null as bounceback_endowment_local_amount,
    null as vip_endowment_local_amount,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime,
    COALESCE(orders.credit_count, 0) AS credit_count,
    COALESCE(orders.credit_local_amount, 0) AS credit_local_amount,
    orders.PAYMENT_CREDIT_CARD_COMPANY,
    orders.financial_status,
    orders.fulfillment_Status,
    orders.SHIPMENT_STATUS,
    COALESCE(orders.shopify_total_line_items_price,0),
    COALESCE(orders.tariff_revenue_before_discount_local_amount,0),
    COALESCE(orders.tariff_discount_local_amout,0)
from orders
left join (
    select *
    from (
        select *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activation_local_datetime) as rn
        from EDW_PROD.DATA_MODEL_JFB.FACT_ACTIVATION
    ) t
    where rn = 1
) fa on orders.customer_id = fa.customer_id

left join (
    select *
    from (
        select *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY EVENT_START_LOCAL_DATETIME) as rn
        from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT
    ) t
    where rn = 1
) fme on orders.customer_id = fme.customer_id
left join (
  select customer_id, activation_key, activation_local_datetime,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activation_local_datetime) as rn
  from EDW_PROD.DATA_MODEL_JFB.FACT_ACTIVATION
  where activation_local_datetime <= (select max(order_local_datetime) from orders)
) ffa
  on orders.customer_id = ffa.customer_id and ffa.rn = 1
left join order_sales_channel_data osc on orders.order_id = osc.id
left join order_status_data osd on orders.order_id = osd.id
left join LAKE.mmt.dwd_com_exchange_rate_df us_er on date(orders.order_local_datetime) = us_er.DATE and orders.currency_key = us_er.EXCHANGE_FROM and us_er.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er on date(orders.order_local_datetime) = eu_er.DATE and orders.currency_key = eu_er.EXCHANGE_FROM and eu_er.EXCHANGE_TO = 'EUR'
left join LAKE.mmt.dwd_com_exchange_rate_df us_er2 on date(orders.order_placed_local_datetime) = us_er2.DATE and orders.currency_key = us_er2.EXCHANGE_FROM and us_er2.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er2 on date(orders.order_placed_local_datetime) = eu_er2.DATE and orders.currency_key = eu_er2.EXCHANGE_FROM and eu_er2.EXCHANGE_TO = 'EUR'
left join LAKE.mmt.dwd_com_exchange_rate_df us_er3 on date(orders.shipped_local_datetime) = us_er3.DATE and orders.currency_key = us_er3.EXCHANGE_FROM and us_er3.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er3 on date(orders.shipped_local_datetime) = eu_er3.DATE and orders.currency_key = eu_er3.EXCHANGE_FROM and eu_er3.EXCHANGE_TO = 'EUR'
left join vip_first_order_logic vip on vip.ID = orders.order_id
left join t1 on t1.order_id = orders.order_id
where t1.order_id is null
and orders.order_local_datetime >='2025-09-23'
),

p3 as (

with orders as (
   with metafield as (
    SELECT
        owner_id
    FROM
        LAKE_MMOS.SHOPIFY_FABKIDS_PROD.metafield t2
    WHERE
        t2.value = 'TARIFF'
        and namespace ='MMOS-product'
    ),

order_line_flag AS (
    SELECT
            l.order_id,
            max(PARSE_JSON(l.price_set):presentment_money:amount::FLOAT) price
        FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD.ORDER_LINE l
        left join metafield m on m.owner_id = l.product_id
        where m.OWNER_ID is not null
        GROUP BY l.order_id
),
unit_count AS (
        select order_id,
               sum(quantity) quantity
        from (
           SELECT distinct
                l.order_id,
                l.id,
                quantity
            FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD.ORDER_LINE l
            left join metafield m on m.owner_id = l.product_id
            where m.OWNER_ID is null
        ) t1
        group by order_id
     ),


   -- 新增的折扣计算逻辑
   order_line_discount AS (
    with order_line as (
         SELECT distinct
                l.order_id,
                l.id
            FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD.ORDER_LINE l
            left join metafield m on m.owner_id = l.product_id
            where m.OWNER_ID is null
    ),
    t as (
        select a.*, c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT as amount, d.code
        from order_line a
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_ALLOCATION c
            on a.id=c.order_line_id
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_APPLICATION d
            on a.order_id=d.order_id and d.index=c.discount_application_index
    )
    select order_id, sum(amount) as discount
    from t a
    left join LAKE_MMOS."mmos_membership_marketing_fabkids".USER_CREDIT_SHARD_ALL b
        on a.code=b."promotion_code"
    where b."promotion_code" is null
    group by 1
),

   tariff_discount_local_amout as (
      with order_line as (
      select
        t1.order_id,t1.ID
        from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_line t1
        left join metafield m on m.owner_id = t1.product_id
        where m.OWNER_ID is not  null ),
        t as (select a.*,c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT as amount,d.code
        from order_line  a
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.discount_allocation c
        on a.id=c.order_line_id
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.discount_application d
        on a.order_id=d.order_id and  d.index=c.discount_application_index )
        select order_id,sum(amount) as discount
        from t a
        left join LAKE_MMOS."mmos_membership_marketing_fabkids".USER_CREDIT_SHARD_ALL b--去除credit兑换的
        on a.code=b."promotion_code"
        where b."promotion_code" is null
        group by 1
   )   ,

       -- 分摊逻辑
product_subtotal_logic AS (
    SELECT
        order_id,
        COALESCE(SUM(product_subtotal_local_amount),0) AS product_subtotal_local_amount,
        COALESCE(SUM(case when vip_credit_id is not null then ratio else 0 end ),0) AS token_count,
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount>0
                                                                   then ratio else 0 end ),0) AS cash_credit_count,        
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount=0
                                                                   then ratio else 0 end ),0) AS non_cash_credit_count,
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount>0
                                            then product_subtotal_local_amount else 0 end ),0) AS cash_credit_local_amount,        
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount=0
                                            then product_subtotal_local_amount else 0 end ),0) AS non_cash_credit_local_amount,
        COALESCE(SUM(case when vip_credit_id is not null then product_subtotal_local_amount else 0 end ),0) AS credit_local_amount
    FROM (
        with order_line_discount_and_product_amount  as (
select
      a.order_id
    ,c.order_line_id
    , COALESCE(d.code,d.title)
    ,PARSE_JSON(a.price_set):presentment_money:amount::FLOAT * a.QUANTITY  order_line_price
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

SELECT
  old.id,
  old.order_id,
  oa.vip_credit_id,
      oa.ratio,
  COALESCE(oa.product_subtotal_local_amount,PARSE_JSON(old.price_set):presentment_money:amount::FLOAT * old.QUANTITY) as product_subtotal_local_amount
FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_line old
LEFT JOIN order_line_discount_and_product_amount oa ON old.id = oa.order_line_id
    ) sub
    GROUP BY order_id
)

 SELECT
    o.id AS order_id,
    max(o.CURRENCY) currency_key,
    max(uc."id") customer_id,
    CASE
        WHEN max(o.FINANCIAL_STATUS) = 'voided' THEN 15325
        WHEN max(o.FINANCIAL_STATUS) = 'pending' THEN 1
        WHEN max(o.FINANCIAL_STATUS) = 'partially_refunded' THEN 2
        WHEN max(o.FINANCIAL_STATUS) = 'refunded' THEN 13
        WHEN max(o.FINANCIAL_STATUS) = 'Authorized' THEN 6
        WHEN max(o.FINANCIAL_STATUS) = 'paid' THEN 12
        WHEN max(o.FINANCIAL_STATUS) = 'Expired' THEN 10
        ELSE -1
    END AS order_payment_status_key,
    max(o.FULFILLMENT_STATUS) FULFILLMENT_STATUS,
    max(f.SHIPMENT_STATUS) SHIPMENT_STATUS ,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT))  order_local_datetime,
    CONVERT_TIMEZONE('America/Los_Angeles',max(case when t.kind  in ('capture','sale') then t.CREATED_AT end)) payment_transaction_local_datetime,
    -- 优化unit_count计算，确保与简单查询一致
    max(ol.quantity) unit_count,
    max(case when t.kind  in ('capture','sale') then t.amount end) payment_transaction_local_amount,
    max(p2.product_subtotal_local_amount) product_subtotal_local_amount,
    max(PARSE_JSON(o.total_tax_set):presentment_money:amount::FLOAT) tax_local_amount,
    max(case when t.kind  in ('capture','sale')and t.GATEWAY = 'gift_card' then t.amount else 0 end) cash_giftco_credit_local_amount,
    count(distinct case when t.kind  in ('capture','sale') and t.GATEWAY = 'gift_card' then t.id end) cash_giftco_credit_count,
    max(PARSE_JSON(osl.price_set):presentment_money:amount::FLOAT) shipping_revenue_before_discount_local_amount,
    max(ol_disc.discount) AS product_discount_local_amount,
    max(case
        when da_shipping.VALUE_TYPE = 'percentage' then PARSE_JSON(osl.price_set):presentment_money:amount::FLOAT * da_shipping.VALUE / 100
        when da_shipping.VALUE_TYPE = 'fixed_amount' then da_shipping.VALUE
        else 0
    end) AS shipping_discount_local_amount,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) order_placed_local,
    max(p2.product_subtotal_local_amount) -
       COALESCE(
       COALESCE(max(ot.price), 0) - COALESCE(max(tariff_discount_local_amout.discount), 0),
       0
   ) as subtotal_excl_tariff_local_amount,
    max(t.PAYMENT_CREDIT_CARD_COMPANY ) PAYMENT_CREDIT_CARD_COMPANY,
    max(CONVERT_TIMEZONE('America/Los_Angeles', f.CREATED_AT)) shipped_local_datetime,
    max(o.FINANCIAL_STATUS) AS FINANCIAL_STATUS,
    count(distinct ucs."promotion_code") as credit_count,
    sum(p2.credit_local_amount) as credit_local_amount,
    sum(case when tag.value='Dropship Fulfillment' then l.quantity else 0 end) as third_party_unit_count,
    COALESCE(COALESCE(max(ot.price),0) -- tariff_revenue_before_discount_local_amount
        - COALESCE(max(tariff_discount_local_amout.discount),0) --tariff_discount_local_amout
        , 0) as tariff_revenue_local_amount,
    max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) order_placed_local_datetime,
    max(PARSE_JSON(o.TOTAL_LINE_ITEMS_PRICE_SET):presentment_money:amount::FLOAT) AS shopify_total_line_items_price,
    max(tariff_discount_local_amout.discount) AS tariff_discount_local_amout,
    max(ot.price) tariff_revenue_before_discount_local_amount,
    max(cash_credit_count) cash_credit_count,
    max(non_cash_credit_count) non_cash_credit_count,
    max(cash_credit_local_amount) cash_credit_local_amount,
    max(non_cash_credit_local_amount) non_cash_credit_local_amount
FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o
left join LAKE_MMOS."mmos_membership_marketing_fabkids".USER_SHARD_ALL uc on o.CUSTOMER_ID = uc."user_gid"
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.ORDER_LINE l on o.id = l.order_id
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.FULFILLMENT f on o.id = f.order_id
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.TRANSACTION t on o.id = t.order_id
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.ORDER_SHIPPING_LINE osl on o.id = osl.order_id
-- 折扣应用表连接
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_APPLICATION da on o.id = da.ORDER_ID
-- 排除在user_credit_shard_000000中存在的promotion_code
left join LAKE_MMOS."mmos_membership_marketing_fabkids".USER_CREDIT_SHARD_ALL ucs on da.code = ucs."promotion_code"
-- 仅包含shipping_line
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_APPLICATION da_shipping on o.id = da_shipping.order_id and da_shipping.TARGET_TYPE = 'shipping_line'
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_tag tag on o.id = tag.order_id  and tag.value != 'Test Order'
-- 移除原来的order_line_flag关联，改用子查询方式
left join order_line_discount ol_disc on o.id = ol_disc.order_id
left join order_line_flag ot on o.id = ot.order_id
left join unit_count ol on o.id = ol.order_id
left join product_subtotal_logic p2 on o.id = p2.order_id
left join tariff_discount_local_amout on o.id = tariff_discount_local_amout.order_id
group by o.id
),

-- 添加order_sales_channel逻辑
 order_sales_channel_data as (
   with t as (
        select o.id,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        else 'Online Order' end as order_sales_channel_l1,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        else 'Web Order' end as order_sales_channel_l2,
        case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
        when ot.value2='Exchange-Order' then 'Exchange'
        when ot.value3='Reship-Order' then 'Reship'
        else 'Product Order' end as order_classification_l1,
        case when ot.value1='Ordergroove Subscription Order' then 'Token Billing'
        else order_classification_l1 end as order_classification_l2,
        false as is_border_free_order,
        false as is_custom_order,
        false as is_ps_order,
        false as is_retail_ship_only_order,
        case when ot.value4='Test Order' then true
        else false end as is_test_order,
        false as is_preorder,
        false as is_product_seeding_order,
        false as is_bops_order,
        false as is_discreet_packaging,
        false as is_bill_me_now_online,
        false as is_bill_me_now_gms,
        false as is_membership_gift,
        false as is_warehouse_outlet_order,
        case when ot.value5='Dropship Fulfillment' then true else false end as is_third_party
        from LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o
        left join (
            select
              ORDER_ID,
              max(case when VALUE ='Ordergroove Subscription Order' then 'Ordergroove Subscription Order' else null end) as value1,
              max(case when VALUE ='Exchange-Order' then 'Exchange-Order' else null end) as value2,
              max(case when VALUE ='Reship-Order' then 'Reship-Order' else null end) as value3,
              max(case when VALUE ='Test Order' then 'Test Order' else null end) as value4,
              max(case when VALUE ='Dropship Fulfillment' then 'Dropship Fulfillment' else null end) as value5
            from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_tag
            group by order_id
        ) ot
        on o.id=ot.order_id
    )
    select distinct  t.id, max(d.order_sales_channel_key) order_sales_channel_key
    from t
    left join EDW_PROD.STG.dim_order_sales_channel d
    on t.order_sales_channel_l1=d.order_sales_channel_l1
    AND t.order_sales_channel_l2 = d.ORDER_SALES_CHANNEL_L2
    and t.order_classification_l1=d.order_classification_l1
    AND t.order_classification_l2 = d.ORDER_CLASSIFICATION_L2
    AND t.is_border_free_order = d.is_border_free_order
    AND t.is_custom_order = d.is_custom_order
    AND t.is_ps_order = d.is_ps_order
    AND t.is_retail_ship_only_order = d.is_retail_ship_only_order
    AND t.is_test_order = d.is_test_order
    AND t.is_preorder = d.is_preorder
    AND t.is_product_seeding_order = d.is_product_seeding_order
    AND t.is_bops_order = d.is_bops_order
    AND t.is_discreet_packaging = d.is_discreet_packaging
    AND t.is_bill_me_now_online = d.is_bill_me_now_online
    AND t.is_bill_me_now_gms = d.is_bill_me_now_gms
    AND t.is_membership_gift = d.is_membership_gift
    AND t.is_warehouse_outlet_order = d.is_warehouse_outlet_order
    AND t.is_third_party = d.is_third_party
    group by t.id

),
     t1 as(
select
      o.id order_id
from LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_tag ot on o.id = ot.order_id
where ot.value = 'Test Order'
group by o.id
),

-- 添加order_status_key逻辑
order_status_data as (
    select t.ID,max(t.order_status_key) order_status_key
    from (
    select
       o.id,
     CASE
         WHEN UPPER(ot.value) = 'DROPSHIP FULFILLMENT' THEN 8
         WHEN UPPER(o.FULFILLMENT_STATUS) = 'FULFILLED' THEN 1
         WHEN UPPER(o.FINANCIAL_STATUS) IN ('PAID', 'AUTHORIZED') THEN 1
         ELSE -1
     END AS order_status_key
    from LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o
    left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_tag ot on o.id = ot.order_id
    where UPPER(ot.value) != 'TEST ORDER' or ot.value is null
         ) t
    group by t.ID
),

vip_first_order_logic as (
    select
          t.id,
       max(vip_order_membership_classification_key) vip_order_membership_classification_key
    from (
        select
            o.id,
            CASE
                -- 情况1: VIP首单且从未成为过VIP
                WHEN oto.value = 'Ordergroove Trigger Order'
                     AND NOT EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                     )
                THEN 2
                -- 情况2: VIP首单但之前成为过VIP（包括已取消的）
                WHEN oto.value = 'Ordergroove Trigger Order'
                     AND EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE  ulr.customer_id = o.customer_id
                     )
                THEN 5

                -- 情况3: 下订单时非VIP状态，且是第一次购买商品，但是非VIP首单
                WHEN oto.value != 'Ordergroove Trigger Order'
                  and NOT EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                          AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                THEN 1

                -- 情况4: 下订单时VIP状态，非首次购买商品
                WHEN  EXISTS (
                         SELECT 1
                         FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND EXISTS (
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                      WHERE ulr.customer_id = o.customer_id
                           AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                THEN 4

                -- 情况5: 下订单时非VIP状态，非首次购买，但是以前是VIP，在购买商品之前取消了
                WHEN  EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS ( --在购买商品之前取消了 非VIP状态
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                           AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                     )
                    AND  EXISTS (  --但是以前是VIP
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                         WHERE ulr.customer_id = o.customer_id
                     )
                THEN 3

                -- 情况6: 下订单时非VIP状态，非首次购买，从未成为过VIP
                WHEN
                    EXISTS (
                         SELECT 1
                         FROM  LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o2
                         WHERE o2.customer_id = o.customer_id
                     )
                     AND NOT EXISTS ( --从未成为过VIP
                         SELECT 1
                         FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                      WHERE ulr.user_id = o.customer_id
                     )

                THEN 6
                ELSE 1
            END AS vip_order_membership_classification_key
        from LAKE_MMOS.SHOPIFY_FABKIDS_PROD."ORDER" o
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.order_tag oto on o.id = oto.order_id
    ) t
    group by  t.id
)
select
    orders.order_id,
    COALESCE(fa.membership_event_key, fme.membership_event_key, -1) AS membership_event_key,
    COALESCE(fa.activation_key, -1) AS activation_key,
    COALESCE(ffa.activation_key, -1) AS first_activation_key,
    orders.currency_key,
    orders.customer_id,
    46 as store_id,
    46 as cart_store_id,
    null as membership_brand_id,
    null as administrator_id,
    null as master_order_id,
    null order_payment_status_key,
    21 as order_processing_status_key,
    vip.vip_order_membership_classification_key AS order_membership_classification_key,
    CASE WHEN (orders.subtotal_excl_tariff_local_amount - orders.product_discount_local_amount - orders.credit_local_amount) > 0 THEN 1
         ELSE 0
    END AS is_cash,
    null AS is_credit_billing_on_retry,
    null AS shipping_address_id,
    null AS billing_address_id,
    osd.order_status_key AS order_status_key,
    osc.order_sales_channel_key,
    case when orders.PAYMENT_CREDIT_CARD_COMPANY='Visa' then '28504'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='Mastercard' then '28775'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='American Express' then '10660'
    when orders.PAYMENT_CREDIT_CARD_COMPANY='Discover' then '28734'
    else -1 end AS payment_key,
    null AS order_customer_selected_shipping_key,
    null AS session_id,
    null AS bops_store_id,
    orders.order_local_datetime,
    orders.payment_transaction_local_datetime,
    orders.shipped_local_datetime,
    COALESCE(orders.payment_transaction_local_datetime,orders.shipped_local_datetime) AS order_completion_local_datetime,
    null AS bops_pickup_local_datetime,
    orders.order_placed_local_datetime,
    orders.unit_count,
    null as loyalty_unit_count,
    orders.third_party_unit_count as third_party_unit_count,
    us_er.EXCHANGE_RATE as order_date_usd_conversion_rate,
    eu_er.EXCHANGE_RATE as order_date_eur_conversion_rate,
    us_er2.EXCHANGE_RATE as payment_transaction_date_usd_conversion_rate,
    eu_er2.EXCHANGE_RATE as payment_transaction_date_eur_conversion_rate,
    us_er3.EXCHANGE_RATE as shipped_date_usd_conversion_rate,
    us_er2.EXCHANGE_RATE as shipped_date_eur_conversion_rate,
    COALESCE(us_er3.EXCHANGE_RATE,us_er2.EXCHANGE_RATE,us_er.EXCHANGE_RATE) as reporting_usd_conversion_rate,
    COALESCE(eu_er3.EXCHANGE_RATE,eu_er2.EXCHANGE_RATE,eu_er.EXCHANGE_RATE) as reporting_eur_conversion_rate,
    null as effective_vat_rate,
    orders.payment_transaction_local_amount,
    orders.subtotal_excl_tariff_local_amount,
    orders.tariff_revenue_local_amount,
    orders.product_subtotal_local_amount,
    orders.tax_local_amount,
    null as delivery_fee_local_amount,
    orders.cash_credit_count as cash_credit_count,
    orders.cash_credit_local_amount as cash_credit_local_amount,
    null as cash_membership_credit_local_amount,
    null as cash_membership_credit_count,
    null as cash_refund_credit_local_amount,
    null as cash_refund_credit_count,
    orders.cash_giftco_credit_local_amount,
    orders.cash_giftco_credit_count,
    null as cash_giftcard_credit_local_amount,
    null as cash_giftcard_credit_count,
    null as token_local_amount,
    null as token_count,
    null as cash_token_local_amount,
    null as cash_token_count,
    null as non_cash_token_local_amount,
    null as non_cash_token_count,
    orders.non_cash_credit_local_amount as non_cash_credit_local_amount,
    orders.non_cash_credit_count as non_cash_credit_count,
    orders.shipping_revenue_before_discount_local_amount,
    orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount as shipping_revenue_local_amount,
    null as shipping_cost_local_amount,
    null as shipping_cost_source,
    orders.product_discount_local_amount,
    orders.shipping_discount_local_amount,
    GREATEST(
        IFNULL(orders.subtotal_excl_tariff_local_amount, 0)
        + IFNULL(orders.tariff_revenue_local_amount, 0)
        - IFNULL(orders.product_discount_local_amount, 0)
        + IFNULL(orders.tax_local_amount, 0)
        + IFNULL(orders.shipping_revenue_before_discount_local_amount, 0)
        - IFNULL(orders.shipping_discount_local_amount, 0),
        0
    ) AS amount_to_pay,
        IFNULL(orders.payment_transaction_local_amount, 0)
        - IFNULL(orders.tax_local_amount, 0) AS cash_gross_revenue_local_amount,
    null as product_gross_revenue_local_amount,
    null as product_gross_revenue_excl_shipping_local_amount,
    null as product_margin_pre_return_local_amount,
    null as product_margin_pre_return_excl_shipping_local_amount,
    IFNULL(
        IFNULL(orders.product_subtotal_local_amount, 0)
        - IFNULL(orders.shipping_discount_local_amount, 0)
        - IFNULL(orders.product_discount_local_amount, 0)
        + IFNULL(orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(COALESCE(0, 0), 0),
        IFNULL(
            ifnull(IFNULL(orders.product_subtotal_local_amount, 0)
                - IFNULL(orders.product_discount_local_amount, 0)
                + IFNULL(orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount, 0)
                - IFNULL(0, 0)
                - IFNULL(0, 0) ,
                IFNULL(orders.payment_transaction_local_amount, 0)
                - IFNULL(orders.tax_local_amount, 0)
            ), 0)
        - IFNULL(0, 0)
        - IFNULL(0, 0)
        - IFNULL(COALESCE(0, 0), 0)
    ) AS product_order_cash_margin_pre_return_local_amount,
    null as estimated_landed_cost_local_amount,
    null as misc_cogs_local_amount,
    null as reporting_landed_cost_local_amount,
    null as is_actual_landed_cost,
    null as estimated_variable_gms_cost_local_amount,
    null as estimated_variable_warehouse_cost_local_amount,
    null as estimated_variable_payment_processing_pct_cash_revenue,
    null as estimated_shipping_supplies_cost_local_amount,
    null as bounceback_endowment_local_amount,
    null as vip_endowment_local_amount,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime,
    COALESCE(orders.credit_count, 0) AS credit_count,
    COALESCE(orders.credit_local_amount, 0) AS credit_local_amount,
    orders.PAYMENT_CREDIT_CARD_COMPANY,
    orders.financial_status,
    orders.fulfillment_Status,
    orders.SHIPMENT_STATUS,
    COALESCE(orders.shopify_total_line_items_price,0),
    COALESCE(orders.tariff_revenue_before_discount_local_amount,0),
    COALESCE(orders.tariff_discount_local_amout,0)
from orders
left join (
    select *
    from (
        select *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activation_local_datetime) as rn
        from EDW_PROD.DATA_MODEL_JFB.FACT_ACTIVATION
    ) t
    where rn = 1
) fa on orders.customer_id = fa.customer_id

left join (
    select *
    from (
        select *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY EVENT_START_LOCAL_DATETIME) as rn
        from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT
    ) t
    where rn = 1
) fme on orders.customer_id = fme.customer_id
left join (
  select customer_id, activation_key, activation_local_datetime,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activation_local_datetime) as rn
  from EDW_PROD.DATA_MODEL_JFB.FACT_ACTIVATION
  where activation_local_datetime <= (select max(order_local_datetime) from orders)
) ffa
  on orders.customer_id = ffa.customer_id and ffa.rn = 1
left join order_sales_channel_data osc on orders.order_id = osc.id
left join order_status_data osd on orders.order_id = osd.id
left join LAKE.mmt.dwd_com_exchange_rate_df us_er on date(orders.order_local_datetime) = us_er.DATE and orders.currency_key = us_er.EXCHANGE_FROM and us_er.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er on date(orders.order_local_datetime) = eu_er.DATE and orders.currency_key = eu_er.EXCHANGE_FROM and eu_er.EXCHANGE_TO = 'EUR'
left join LAKE.mmt.dwd_com_exchange_rate_df us_er2 on date(orders.order_placed_local_datetime) = us_er2.DATE and orders.currency_key = us_er2.EXCHANGE_FROM and us_er2.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er2 on date(orders.order_placed_local_datetime) = eu_er2.DATE and orders.currency_key = eu_er2.EXCHANGE_FROM and eu_er2.EXCHANGE_TO = 'EUR'
left join LAKE.mmt.dwd_com_exchange_rate_df us_er3 on date(orders.shipped_local_datetime) = us_er3.DATE and orders.currency_key = us_er3.EXCHANGE_FROM and us_er3.EXCHANGE_TO = 'USD'
left join LAKE.mmt.dwd_com_exchange_rate_df eu_er3 on date(orders.shipped_local_datetime) = eu_er3.DATE and orders.currency_key = eu_er3.EXCHANGE_FROM and eu_er3.EXCHANGE_TO = 'EUR'
left join vip_first_order_logic vip on vip.ID = orders.order_id
left join t1 on t1.order_id = orders.order_id
where t1.order_id is null
and orders.order_local_datetime >='2025-10-15'
),
p4 as (
    with orders as (
        with metafield as (
            SELECT
                owner_id
            FROM
                LAKE_MMOS.SHOPIFY_JFEU_PROD.metafield t2
            WHERE
                t2.value = 'TARIFF'
                and namespace ='MMOS-product'
            ),

        order_line_flag AS (
            SELECT
                    l.order_id,
                    max(PARSE_JSON(l.price_set):presentment_money:amount::FLOAT) price
                FROM LAKE_MMOS.SHOPIFY_JFEU_PROD.ORDER_LINE l
                left join metafield m on m.owner_id = l.product_id
                where m.OWNER_ID is not null
                GROUP BY l.order_id
        ),
        unit_count AS (
                select order_id,
                    sum(quantity) quantity
                from (
                SELECT distinct
                        l.order_id,
                        l.id,
                        quantity
                    FROM LAKE_MMOS.SHOPIFY_JFEU_PROD.ORDER_LINE l
                    left join metafield m on m.owner_id = l.product_id
                    where m.OWNER_ID is null
                ) t1
                group by order_id
            ),


        -- 新增的折扣计算逻辑
        order_line_discount AS (
            with order_line as (
                SELECT distinct
                        l.order_id,
                        l.id
                    FROM LAKE_MMOS.SHOPIFY_JFEU_PROD.ORDER_LINE l
                    left join metafield m on m.owner_id = l.product_id
                    where m.OWNER_ID is null
            ),
            t as (
                select a.*, c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT as amount, d.code
                from order_line a
                left join LAKE_MMOS.SHOPIFY_JFEU_PROD.DISCOUNT_ALLOCATION c
                    on a.id=c.order_line_id
                left join LAKE_MMOS.SHOPIFY_JFEU_PROD.DISCOUNT_APPLICATION d
                    on a.order_id=d.order_id and d.index=c.discount_application_index
            )
            select order_id, sum(amount) as discount
            from t a
            left join LAKE_MMOS."mmos_membership_marketing_eu".USER_CREDIT_SHARD_ALL b
                on a.code=b."promotion_code"
            where b."promotion_code" is null
            group by 1
        ),

        tariff_discount_local_amout as (
            with order_line as (
            select
                t1.order_id,t1.ID
                from LAKE_MMOS.SHOPIFY_JFEU_PROD.order_line t1
                left join metafield m on m.owner_id = t1.product_id
                where m.OWNER_ID is not  null ),
                t as (select a.*,c.AMOUNT_SET_PRESENTMENT_MONEY_AMOUNT as amount,d.code
                from order_line  a
                left join LAKE_MMOS.SHOPIFY_JFEU_PROD.discount_allocation c
                on a.id=c.order_line_id
                left join LAKE_MMOS.SHOPIFY_JFEU_PROD.discount_application d
                on a.order_id=d.order_id and  d.index=c.discount_application_index )
                select order_id,sum(amount) as discount
                from t a
                left join LAKE_MMOS."mmos_membership_marketing_eu".USER_CREDIT_SHARD_ALL b--去除credit兑换的
                on a.code=b."promotion_code"
                where b."promotion_code" is null
                group by 1
        )   ,

            -- 分摊逻辑
        product_subtotal_logic AS (
            SELECT
                order_id,
                COALESCE(SUM(product_subtotal_local_amount),0) AS product_subtotal_local_amount,
        COALESCE(SUM(case when vip_credit_id is not null then ratio else 0 end ),0) AS token_count,
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount>0
                                                                   then ratio else 0 end ),0) AS cash_credit_count,        
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount=0
                                                                   then ratio else 0 end ),0) AS non_cash_credit_count,
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount>0
                                            then product_subtotal_local_amount else 0 end ),0) AS cash_credit_local_amount,        
        COALESCE(SUM(case when vip_credit_id is not null and product_subtotal_local_amount=0
                                            then product_subtotal_local_amount else 0 end ),0) AS non_cash_credit_local_amount,
                COALESCE(SUM(case when vip_credit_id is not null then product_subtotal_local_amount else 0 end ),0) AS credit_local_amount
            FROM (
                with order_line_discount_and_product_amount  as (
        select
            a.order_id
            ,c.order_line_id
            , COALESCE(d.code,d.title)
            ,PARSE_JSON(a.price_set):presentment_money:amount::FLOAT * a.QUANTITY  order_line_price
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

        SELECT
        old.id,
        old.order_id,
        oa.vip_credit_id,
      oa.ratio,
  COALESCE(oa.product_subtotal_local_amount,PARSE_JSON(old.price_set):presentment_money:amount::FLOAT * old.QUANTITY) as product_subtotal_local_amount
        FROM LAKE_MMOS.SHOPIFY_JFEU_PROD.order_line old
        LEFT JOIN order_line_discount_and_product_amount oa ON old.id = oa.order_line_id
            ) sub
            GROUP BY order_id
        )

        SELECT
            o.id AS order_id,
            max(o.CURRENCY) currency_key,
            max(uc."id") customer_id,
            CASE
                WHEN max(o.FINANCIAL_STATUS) = 'voided' THEN 15325
                WHEN max(o.FINANCIAL_STATUS) = 'pending' THEN 1
                WHEN max(o.FINANCIAL_STATUS) = 'partially_refunded' THEN 2
                WHEN max(o.FINANCIAL_STATUS) = 'refunded' THEN 13
                WHEN max(o.FINANCIAL_STATUS) = 'Authorized' THEN 6
                WHEN max(o.FINANCIAL_STATUS) = 'paid' THEN 12
                WHEN max(o.FINANCIAL_STATUS) = 'Expired' THEN 10
                ELSE -1
            END AS order_payment_status_key,
            max(o.FULFILLMENT_STATUS) FULFILLMENT_STATUS,
            max(f.SHIPMENT_STATUS) SHIPMENT_STATUS ,
            max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT))  order_local_datetime,
            CONVERT_TIMEZONE('America/Los_Angeles',max(case when t.kind  in ('capture','sale') then t.CREATED_AT end)) payment_transaction_local_datetime,
            -- 优化unit_count计算，确保与简单查询一致
            max(ol.quantity) unit_count,
            max(case when t.kind  in ('capture','sale') then t.amount end) payment_transaction_local_amount,
            max(p2.product_subtotal_local_amount)-max(o.TOTAL_TAX) product_subtotal_local_amount,
            max(PARSE_JSON(o.total_tax_set):presentment_money:amount::FLOAT) tax_local_amount,
            max(case when t.kind  in ('capture','sale')and t.GATEWAY = 'gift_card' then t.amount else 0 end) cash_giftco_credit_local_amount,
            count(distinct case when t.kind  in ('capture','sale') and t.GATEWAY = 'gift_card' then t.id end) cash_giftco_credit_count,
            max(PARSE_JSON(osl.price_set):presentment_money:amount::FLOAT) shipping_revenue_before_discount_local_amount,
            max(ol_disc.discount) AS product_discount_local_amount,
            max(case
                when da_shipping.VALUE_TYPE = 'percentage' then PARSE_JSON(osl.price_set):presentment_money:amount::FLOAT * da_shipping.VALUE / 100
                when da_shipping.VALUE_TYPE = 'fixed_amount' then da_shipping.VALUE
                else 0
            end) AS shipping_discount_local_amount,
            max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) order_placed_local,
            max(p2.product_subtotal_local_amount) - max(o.TOTAL_TAX) -
                COALESCE(COALESCE(max(ot.price), 0) - COALESCE(max(tariff_discount_local_amout.discount), 0),0) as subtotal_excl_tariff_local_amount,
            max(t.PAYMENT_CREDIT_CARD_COMPANY ) PAYMENT_CREDIT_CARD_COMPANY,
            max(CONVERT_TIMEZONE('America/Los_Angeles', f.CREATED_AT)) shipped_local_datetime,
            max(o.FINANCIAL_STATUS) AS FINANCIAL_STATUS,
            count(distinct ucs."promotion_code") as credit_count,
            sum(p2.credit_local_amount) as credit_local_amount,
            sum(case when tag.value='Dropship Fulfillment' then l.quantity else 0 end) as third_party_unit_count,
            COALESCE(COALESCE(max(ot.price),0) -- tariff_revenue_before_discount_local_amount
                - COALESCE(max(tariff_discount_local_amout.discount),0) --tariff_discount_local_amout
                , 0) as tariff_revenue_local_amount,
            max(CONVERT_TIMEZONE('America/Los_Angeles', o.PROCESSED_AT)) order_placed_local_datetime,
            max(PARSE_JSON(o.TOTAL_LINE_ITEMS_PRICE_SET):presentment_money:amount::FLOAT) AS shopify_total_line_items_price,
            max(tariff_discount_local_amout.discount) AS tariff_discount_local_amout,
            max(ot.price) tariff_revenue_before_discount_local_amount,
            max(cash_credit_count) cash_credit_count,
            max(non_cash_credit_count) non_cash_credit_count,
            max(cash_credit_local_amount) cash_credit_local_amount,
            max(non_cash_credit_local_amount) non_cash_credit_local_amount
        FROM LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o
        left join LAKE_MMOS."mmos_membership_marketing_eu".USER_SHARD_ALL uc on o.CUSTOMER_ID = uc."user_gid"
        left join LAKE_MMOS.SHOPIFY_JFEU_PROD.ORDER_LINE l on o.id = l.order_id
        left join LAKE_MMOS.SHOPIFY_JFEU_PROD.FULFILLMENT f on o.id = f.order_id
        left join LAKE_MMOS.SHOPIFY_JFEU_PROD.TRANSACTION t on o.id = t.order_id
        left join LAKE_MMOS.SHOPIFY_JFEU_PROD.ORDER_SHIPPING_LINE osl on o.id = osl.order_id
        -- 折扣应用表连接
        left join LAKE_MMOS.SHOPIFY_JFEU_PROD.DISCOUNT_APPLICATION da on o.id = da.ORDER_ID
        -- 排除在user_credit_shard_000000中存在的promotion_code
        left join LAKE_MMOS."mmos_membership_marketing_eu".USER_CREDIT_SHARD_ALL ucs on da.code = ucs."promotion_code"
        -- 仅包含shipping_line
        left join LAKE_MMOS.SHOPIFY_JFEU_PROD.DISCOUNT_APPLICATION da_shipping on o.id = da_shipping.order_id and da_shipping.TARGET_TYPE = 'shipping_line'
        left join LAKE_MMOS.SHOPIFY_JFEU_PROD.order_tag tag on o.id = tag.order_id  and tag.value != 'Test Order'
        -- 移除原来的order_line_flag关联，改用子查询方式
        left join order_line_discount ol_disc on o.id = ol_disc.order_id
        left join order_line_flag ot on o.id = ot.order_id
        left join unit_count ol on o.id = ol.order_id
        left join product_subtotal_logic p2 on o.id = p2.order_id
        left join tariff_discount_local_amout on o.id = tariff_discount_local_amout.order_id
        group by o.id
    ),

    -- 添加order_sales_channel逻辑
    order_sales_channel_data as (
        with t as (
            select o.id,
            case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
            else 'Online Order' end as order_sales_channel_l1,
            case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
            else 'Web Order' end as order_sales_channel_l2,
            case when ot.value1='Ordergroove Subscription Order' then 'Billing Order'
            when ot.value2='Exchange-Order' then 'Exchange'
            when ot.value3='Reship-Order' then 'Reship'
            else 'Product Order' end as order_classification_l1,
            case when ot.value1='Ordergroove Subscription Order' then 'Token Billing'
            else order_classification_l1 end as order_classification_l2,
            false as is_border_free_order,
            false as is_custom_order,
            false as is_ps_order,
            false as is_retail_ship_only_order,
            case when ot.value4='Test Order' then true
            else false end as is_test_order,
            false as is_preorder,
            false as is_product_seeding_order,
            false as is_bops_order,
            false as is_discreet_packaging,
            false as is_bill_me_now_online,
            false as is_bill_me_now_gms,
            false as is_membership_gift,
            false as is_warehouse_outlet_order,
            case when ot.value5='Dropship Fulfillment' then true else false end as is_third_party
            from LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o
            left join (
                select
                ORDER_ID,
                max(case when VALUE ='Ordergroove Subscription Order' then 'Ordergroove Subscription Order' else null end) as value1,
                max(case when VALUE ='Exchange-Order' then 'Exchange-Order' else null end) as value2,
                max(case when VALUE ='Reship-Order' then 'Reship-Order' else null end) as value3,
                max(case when VALUE ='Test Order' then 'Test Order' else null end) as value4,
                max(case when VALUE ='Dropship Fulfillment' then 'Dropship Fulfillment' else null end) as value5
                from LAKE_MMOS.SHOPIFY_JFEU_PROD.order_tag
                group by order_id
            ) ot
            on o.id=ot.order_id
        )
        select distinct  t.id, max(d.order_sales_channel_key) order_sales_channel_key
        from t
        left join EDW_PROD.STG.dim_order_sales_channel d on t.order_sales_channel_l1=d.order_sales_channel_l1
            AND t.order_sales_channel_l2 = d.ORDER_SALES_CHANNEL_L2
            and t.order_classification_l1=d.order_classification_l1
            AND t.order_classification_l2 = d.ORDER_CLASSIFICATION_L2
            AND t.is_border_free_order = d.is_border_free_order
            AND t.is_custom_order = d.is_custom_order
            AND t.is_ps_order = d.is_ps_order
            AND t.is_retail_ship_only_order = d.is_retail_ship_only_order
            AND t.is_test_order = d.is_test_order
            AND t.is_preorder = d.is_preorder
            AND t.is_product_seeding_order = d.is_product_seeding_order
            AND t.is_bops_order = d.is_bops_order
            AND t.is_discreet_packaging = d.is_discreet_packaging
            AND t.is_bill_me_now_online = d.is_bill_me_now_online
            AND t.is_bill_me_now_gms = d.is_bill_me_now_gms
            AND t.is_membership_gift = d.is_membership_gift
            AND t.is_warehouse_outlet_order = d.is_warehouse_outlet_order
            AND t.is_third_party = d.is_third_party
        group by t.id
    ),
    t1 as(
        select
            o.id order_id
        from LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o
        left join LAKE_MMOS.SHOPIFY_JFEU_PROD.order_tag ot on o.id = ot.order_id
        where ot.value = 'Test Order'
        group by o.id
    ),

    -- 添加order_status_key逻辑
    order_status_data as (
        select t.ID,max(t.order_status_key) order_status_key
        from (
            select
            o.id,
            CASE
                WHEN UPPER(ot.value) = 'DROPSHIP FULFILLMENT' THEN 8
                WHEN UPPER(o.FULFILLMENT_STATUS) = 'FULFILLED' THEN 1
                WHEN UPPER(o.FINANCIAL_STATUS) IN ('PAID', 'AUTHORIZED') THEN 1
                ELSE -1
            END AS order_status_key
            from LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o
            left join LAKE_MMOS.SHOPIFY_JFEU_PROD.order_tag ot on o.id = ot.order_id
            where UPPER(ot.value) != 'TEST ORDER' or ot.value is null
        ) t
        group by t.ID
    ),

    vip_first_order_logic as (
        select
            t.id,
        max(vip_order_membership_classification_key) vip_order_membership_classification_key
        from (
            select
                o.id,
                CASE
                    -- 情况1: VIP首单且从未成为过VIP
                    WHEN oto.value = 'Ordergroove Trigger Order'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                            WHERE ulr.customer_id = o.customer_id
                        )
                    THEN 2
                    -- 情况2: VIP首单但之前成为过VIP（包括已取消的）
                    WHEN oto.value = 'Ordergroove Trigger Order'
                        AND EXISTS (
                            SELECT 1
                            FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                            WHERE  ulr.customer_id = o.customer_id
                        )
                    THEN 5

                    -- 情况3: 下订单时非VIP状态，且是第一次购买商品，但是非VIP首单
                    WHEN oto.value != 'Ordergroove Trigger Order'
                    and NOT EXISTS (
                            SELECT 1
                            FROM  LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o2
                            WHERE o2.customer_id = o.customer_id
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                            WHERE ulr.customer_id = o.customer_id
                            AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                        )
                    THEN 1

                    -- 情况4: 下订单时VIP状态，非首次购买商品
                    WHEN  EXISTS (
                            SELECT 1
                            FROM LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o2
                            WHERE o2.customer_id = o.customer_id
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                        WHERE ulr.customer_id = o.customer_id
                            AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                        )
                    THEN 4

                    -- 情况5: 下订单时非VIP状态，非首次购买，但是以前是VIP，在购买商品之前取消了
                    WHEN  EXISTS (
                            SELECT 1
                            FROM  LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o2
                            WHERE o2.customer_id = o.customer_id
                        )
                        AND NOT EXISTS ( --在购买商品之前取消了 非VIP状态
                            SELECT 1
                            FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                            WHERE ulr.customer_id = o.customer_id
                            AND date (o.PROCESSED_AT) between ulr.start_time  and ulr.end_time
                        )
                        AND  EXISTS (  --但是以前是VIP
                            SELECT 1
                            FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                            WHERE ulr.customer_id = o.customer_id
                        )
                    THEN 3

                    -- 情况6: 下订单时非VIP状态，非首次购买，从未成为过VIP
                    WHEN
                        EXISTS (
                            SELECT 1
                            FROM  LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o2
                            WHERE o2.customer_id = o.customer_id
                        )
                        AND NOT EXISTS ( --从未成为过VIP
                            SELECT 1
                            FROM EDW_PROD.NEW_STG.USER_VIP_PERIODSS ulr
                        WHERE ulr.user_id = o.customer_id
                        )

                    THEN 6
                    ELSE 1
                END AS vip_order_membership_classification_key
            from LAKE_MMOS.SHOPIFY_JFEU_PROD."ORDER" o
            left join LAKE_MMOS.SHOPIFY_JFEU_PROD.order_tag oto on o.id = oto.order_id
        ) t
        group by  t.id
    )
    select
        orders.order_id,
        COALESCE(fa.membership_event_key, fme.membership_event_key, -1) AS membership_event_key,
        COALESCE(fa.activation_key, -1) AS activation_key,
        COALESCE(ffa.activation_key, -1) AS first_activation_key,
        orders.currency_key,
        orders.customer_id,
        usasi.store_id as store_id,
        usasi.store_id as cart_store_id,
        null as membership_brand_id,
        null as administrator_id,
        null as master_order_id,
        null order_payment_status_key,
        21 as order_processing_status_key,
        vip.vip_order_membership_classification_key AS order_membership_classification_key,
        CASE WHEN (orders.subtotal_excl_tariff_local_amount - orders.product_discount_local_amount - orders.credit_local_amount) > 0 THEN 1
            ELSE 0
        END AS is_cash,
        null AS is_credit_billing_on_retry,
        null AS shipping_address_id,
        null AS billing_address_id,
        osd.order_status_key AS order_status_key,
        osc.order_sales_channel_key,
        case when orders.PAYMENT_CREDIT_CARD_COMPANY='Visa' then '28504'
        when orders.PAYMENT_CREDIT_CARD_COMPANY='Mastercard' then '28775'
        when orders.PAYMENT_CREDIT_CARD_COMPANY='American Express' then '10660'
        when orders.PAYMENT_CREDIT_CARD_COMPANY='Discover' then '28734'
        else -1 end AS payment_key,
        null AS order_customer_selected_shipping_key,
        null AS session_id,
        null AS bops_store_id,
        orders.order_local_datetime,
        orders.payment_transaction_local_datetime,
        orders.shipped_local_datetime,
        COALESCE(orders.payment_transaction_local_datetime,orders.shipped_local_datetime) AS order_completion_local_datetime,
        null AS bops_pickup_local_datetime,
        orders.order_placed_local_datetime,
        orders.unit_count,
        null as loyalty_unit_count,
        orders.third_party_unit_count as third_party_unit_count,
        us_er.EXCHANGE_RATE as order_date_usd_conversion_rate,
        eu_er.EXCHANGE_RATE as order_date_eur_conversion_rate,
        us_er2.EXCHANGE_RATE as payment_transaction_date_usd_conversion_rate,
        eu_er2.EXCHANGE_RATE as payment_transaction_date_eur_conversion_rate,
        us_er3.EXCHANGE_RATE as shipped_date_usd_conversion_rate,
        us_er2.EXCHANGE_RATE as shipped_date_eur_conversion_rate,
        COALESCE(us_er3.EXCHANGE_RATE,us_er2.EXCHANGE_RATE,us_er.EXCHANGE_RATE) as reporting_usd_conversion_rate,
        COALESCE(eu_er3.EXCHANGE_RATE,eu_er2.EXCHANGE_RATE,eu_er.EXCHANGE_RATE) as reporting_eur_conversion_rate,
        null as effective_vat_rate,
        orders.payment_transaction_local_amount,
        orders.subtotal_excl_tariff_local_amount,
        orders.tariff_revenue_local_amount,
        orders.product_subtotal_local_amount,
        orders.tax_local_amount,
        null as delivery_fee_local_amount,
        orders.cash_credit_count as cash_credit_count,
        orders.cash_credit_local_amount as cash_credit_local_amount,
        null as cash_membership_credit_local_amount,
        null as cash_membership_credit_count,
        null as cash_refund_credit_local_amount,
        null as cash_refund_credit_count,
        orders.cash_giftco_credit_local_amount,
        orders.cash_giftco_credit_count,
        null as cash_giftcard_credit_local_amount,
        null as cash_giftcard_credit_count,
        null as token_local_amount,
        null as token_count,
        null as cash_token_local_amount,
        null as cash_token_count,
        null as non_cash_token_local_amount,
        null as non_cash_token_count,
        orders.non_cash_credit_local_amount as non_cash_credit_local_amount,
        orders.non_cash_credit_count as non_cash_credit_count,
        orders.shipping_revenue_before_discount_local_amount,
        orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount as shipping_revenue_local_amount,
        null as shipping_cost_local_amount,
        null as shipping_cost_source,
        orders.product_discount_local_amount,
        orders.shipping_discount_local_amount,
        GREATEST(
            IFNULL(orders.subtotal_excl_tariff_local_amount, 0)
            + IFNULL(orders.tariff_revenue_local_amount, 0)
            - IFNULL(orders.product_discount_local_amount, 0)
            + IFNULL(orders.tax_local_amount, 0)
            + IFNULL(orders.shipping_revenue_before_discount_local_amount, 0)
            - IFNULL(orders.shipping_discount_local_amount, 0),
            0
        ) AS amount_to_pay,
            IFNULL(orders.payment_transaction_local_amount, 0)
            - IFNULL(orders.tax_local_amount, 0) AS cash_gross_revenue_local_amount,
        null as product_gross_revenue_local_amount,
        null as product_gross_revenue_excl_shipping_local_amount,
        null as product_margin_pre_return_local_amount,
        null as product_margin_pre_return_excl_shipping_local_amount,
        IFNULL(
            IFNULL(orders.product_subtotal_local_amount, 0)
            - IFNULL(orders.shipping_discount_local_amount, 0)
            - IFNULL(orders.product_discount_local_amount, 0)
            + IFNULL(orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount, 0)
            - IFNULL(0, 0)
            - IFNULL(0, 0)
            - IFNULL(0, 0)
            - IFNULL(0, 0)
            - IFNULL(COALESCE(0, 0), 0),
            IFNULL(
                ifnull(IFNULL(orders.product_subtotal_local_amount, 0)
                    - IFNULL(orders.product_discount_local_amount, 0)
                    + IFNULL(orders.shipping_revenue_before_discount_local_amount - orders.shipping_discount_local_amount, 0)
                    - IFNULL(0, 0)
                    - IFNULL(0, 0) ,
                    IFNULL(orders.payment_transaction_local_amount, 0)
                    - IFNULL(orders.tax_local_amount, 0)
                ), 0)
            - IFNULL(0, 0)
            - IFNULL(0, 0)
            - IFNULL(COALESCE(0, 0), 0)
        ) AS product_order_cash_margin_pre_return_local_amount,
        null as estimated_landed_cost_local_amount,
        null as misc_cogs_local_amount,
        null as reporting_landed_cost_local_amount,
        null as is_actual_landed_cost,
        null as estimated_variable_gms_cost_local_amount,
        null as estimated_variable_warehouse_cost_local_amount,
        null as estimated_variable_payment_processing_pct_cash_revenue,
        null as estimated_shipping_supplies_cost_local_amount,
        null as bounceback_endowment_local_amount,
        null as vip_endowment_local_amount,
        current_date as meta_create_datetime,
        current_date as meta_update_datetime,
        COALESCE(orders.credit_count, 0) AS credit_count,
        COALESCE(orders.credit_local_amount, 0) AS credit_local_amount,
        orders.PAYMENT_CREDIT_CARD_COMPANY,
        orders.financial_status,
        orders.fulfillment_Status,
        orders.SHIPMENT_STATUS,
        COALESCE(orders.shopify_total_line_items_price,0),
        COALESCE(orders.tariff_revenue_before_discount_local_amount,0),
        COALESCE(orders.tariff_discount_local_amout,0)
    from orders
    left join (
        select *
        from (
            select *,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activation_local_datetime) as rn
            from EDW_PROD.DATA_MODEL_JFB.FACT_ACTIVATION
        ) t
        where rn = 1
    ) fa on orders.customer_id = fa.customer_id

    left join (
        select *
        from (
            select *,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY EVENT_START_LOCAL_DATETIME) as rn
            from EDW_PROD.DATA_MODEL_JFB.FACT_MEMBERSHIP_EVENT
        ) t
        where rn = 1
    ) fme on orders.customer_id = fme.customer_id
    left join (
    select customer_id, activation_key, activation_local_datetime,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activation_local_datetime) as rn
    from EDW_PROD.DATA_MODEL_JFB.FACT_ACTIVATION
    where activation_local_datetime <= (select max(order_local_datetime) from orders)
    ) ffa
    on orders.customer_id = ffa.customer_id and ffa.rn = 1
    left join order_sales_channel_data osc on orders.order_id = osc.id
    left join order_status_data osd on orders.order_id = osd.id
    left join LAKE.mmt.dwd_com_exchange_rate_df us_er on date(orders.order_local_datetime) = us_er.DATE and orders.currency_key = us_er.EXCHANGE_FROM and us_er.EXCHANGE_TO = 'USD'
    left join LAKE.mmt.dwd_com_exchange_rate_df eu_er on date(orders.order_local_datetime) = eu_er.DATE and orders.currency_key = eu_er.EXCHANGE_FROM and eu_er.EXCHANGE_TO = 'EUR'
    left join LAKE.mmt.dwd_com_exchange_rate_df us_er2 on date(orders.order_placed_local_datetime) = us_er2.DATE and orders.currency_key = us_er2.EXCHANGE_FROM and us_er2.EXCHANGE_TO = 'USD'
    left join LAKE.mmt.dwd_com_exchange_rate_df eu_er2 on date(orders.order_placed_local_datetime) = eu_er2.DATE and orders.currency_key = eu_er2.EXCHANGE_FROM and eu_er2.EXCHANGE_TO = 'EUR'
    left join LAKE.mmt.dwd_com_exchange_rate_df us_er3 on date(orders.shipped_local_datetime) = us_er3.DATE and orders.currency_key = us_er3.EXCHANGE_FROM and us_er3.EXCHANGE_TO = 'USD'
    left join LAKE.mmt.dwd_com_exchange_rate_df eu_er3 on date(orders.shipped_local_datetime) = eu_er3.DATE and orders.currency_key = eu_er3.EXCHANGE_FROM and eu_er3.EXCHANGE_TO = 'EUR'
    left join vip_first_order_logic vip on vip.ID = orders.order_id
    left join t1 on t1.order_id = orders.order_id
    left join user_shard_all_store_id usasi on orders.customer_id = usasi."id"
    where t1.order_id is null
    and orders.order_local_datetime >= '2025-10-22'
)

select *, store_id as site_id from p1
union all
select *, store_id as site_id from p2
union all
select *, store_id as site_id from p3
union all
select *, 2000 as site_id from p4;


