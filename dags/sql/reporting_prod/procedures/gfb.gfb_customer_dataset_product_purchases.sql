set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _product_purchase_base  AS
SELECT
    cd.customer_id
    ,cd.activation_local_datetime
    ,cd.cancel_local_datetime
    ,cd.month_date
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE cd
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());

CREATE OR REPLACE TEMPORARY TABLE _exchanges AS
SELECT ppb.customer_id
     , ppb.month_date
     , SUM(total_qty_sold) AS total_exchange_units
     , SUM(CASE
               WHEN olp.order_type = 'vip repeat'
                   THEN olp.total_qty_sold
               ELSE 0 END) AS repeat_exchange_units
     , SUM(CASE
               WHEN olp.order_type = 'vip activating'
                   THEN olp.total_qty_sold
               ELSE 0 END) AS activating_exchange_units
FROM _product_purchase_base ppb
         JOIN reporting_prod.gfb.gfb_order_line_data_set_place_date olp
              ON olp.customer_id = ppb.customer_id
                  AND DATE_TRUNC(MONTH, olp.order_date) = ppb.month_date
                  AND olp.order_type IN ('vip activating', 'vip repeat')
                  AND olp.order_classification = 'exchange'
WHERE ppb.month_date >= $low_watermark_ltz
    and ppb.month_date < date_trunc('MONTH',current_date())
GROUP BY ppb.customer_id
       , ppb.month_date;


delete from REPORTING_PROD.GFB.gfb_customer_dataset_product_purchases a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_product_purchases
select
    ppb.customer_id
    ,ppb.month_date

    ,count(distinct olp.ORDER_ID) as order_count
    ,sum(olp.TOTAL_QTY_SOLD) as unit_sales
    ,sum(olp.ORDER_LINE_SUBTOTAL) as subtotal
    ,sum(olp.TOTAL_DISCOUNT) as discount
    ,sum(olp.TOTAL_PRODUCT_REVENUE) as product_revenue_excl_shipping
    ,IFF(sum(olp.cash_collected_amount) > 0, COUNT(distinct order_id), 0) AS cash_collected_count
    ,SUM(olp.cash_collected_amount)                                       AS cash_collected_amount
    ,sum(olp.TOTAL_COGS) as product_cost
    ,SUM(olp.total_return_unit)                                           AS items_returned
    ,sum(case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.TOTAL_PRODUCT_REVENUE
                else 0 end)                        AS repeat_product_revenue_excl_shipping
    ,count(distinct case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.ORDER_ID
                else 0 end)                        AS repeat_order_count
    ,sum(case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.TOTAL_QTY_SOLD
                else 0 end)                        AS repeat_qty_sold
     , SUM(CASE
               WHEN olp.order_type = 'vip repeat'
                   THEN olp.total_return_unit
               ELSE 0 END)                                                  AS repeat_return_units
     , SUM(CASE
               WHEN olp.order_type = 'vip activating'
                   THEN olp.total_product_revenue
               ELSE 0 END)                                                  AS activating_product_revenue_excl_shipping
     , SUM(CASE
               WHEN olp.order_type = 'vip activating'
                   THEN olp.total_qty_sold
               ELSE 0 END)                                                  AS activating_qty_sold
     , SUM(CASE
               WHEN olp.order_type = 'vip activating'
                   THEN olp.total_discount
               ELSE 0 END)                                                  AS activating_discount
     , SUM(CASE
               WHEN olp.order_type != 'vip activating'
                   THEN olp.total_discount
               ELSE 0 END)                                                  AS discount_excl_activation    ,product_revenue_excl_shipping * 1.0/order_count as aov_excl_shipping
    ,unit_sales * 1.0/order_count as upt
    ,(case
        when count(distinct case
                    when olp.ORDER_TYPE = 'vip repeat'
                    then olp.ORDER_ID
                    else 0 end) > 0
        then sum(case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.TOTAL_PRODUCT_REVENUE
                else 0 end)
            /count(distinct case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.ORDER_ID
                else 0 end)
        else 0 end ) as non_activating_aov_excl_shipping
    ,(case
        when count(distinct case
                    when olp.ORDER_TYPE = 'vip repeat'
                    then olp.ORDER_ID
                    else 0 end) > 0
        then sum(case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.TOTAL_QTY_SOLD
                else 0 end)
            /count(distinct case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.ORDER_ID
                else 0 end)
        else 0 end) as non_activating_upt
    ,(case
        when sum(case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.TOTAL_QTY_SOLD
                else 0 end) > 0
        then sum(case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.TOTAL_PRODUCT_REVENUE
                else 0 end)
            /sum(case
                when olp.ORDER_TYPE = 'vip repeat'
                then olp.TOTAL_QTY_SOLD
                else 0 end)
        else 0 end ) as aur_excl_shipping
    ,discount * 1.0/subtotal as discount_rate

    ,iff(sum(case
        when mdp.SUBCATEGORY = 'DRESSES'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_dress
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'TOPS'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_tops
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'PUMPS'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_PUMPS
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'ANKLE BOOTS'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_ANKLE_BOOTS
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'FLAT BOOTS'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_FLAT_BOOTS
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'HEELED SANDALS'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_HEELED_SANDALS
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'SANDALS-DRESSY'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_SANDALS_DRESSY
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'JACKETS'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_JACKETS
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'BOOTIES'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_BOOTIES
    ,iff(sum(case
        when mdp.SUBCATEGORY = 'HEELED BOOTS'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_HEELED_BOOTS
    ,iff(sum(case
        when mdp.DEPARTMENT = 'FOOTWEAR'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_FOOTWEAR
    ,iff(sum(case
        when mdp.DEPARTMENT = 'APPAREL'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_APPAREL
    ,iff(sum(case
        when mdp.DEPARTMENT = 'HANDBAGS'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_HANDBAGS
    ,iff(sum(case
        when mdp.DEPARTMENT = 'ACCESSORIES'
        then olp.TOTAL_QTY_SOLD else 0 end) > 0, 1, 0) as is_shopped_ACCESSORIES
     , SUM(e.total_exchange_units)                                          AS total_exchange_units
     , SUM(e.activating_exchange_units)                                     AS activating_exchange_units
     , SUM(e.repeat_exchange_units)                                         AS repeat_exchange_units
from _product_purchase_base ppb
join REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE olp
    on olp.CUSTOMER_ID = ppb.customer_id
    and date_trunc(month, olp.ORDER_DATE) = ppb.month_date
    and olp.ORDER_TYPE in ('vip activating', 'vip repeat')
    and olp.ORDER_CLASSIFICATION = 'product order'
    and olp.ORDER_LINE_SUBTOTAL > 0
join REPORTING_PROD.GFB.MERCH_DIM_PRODUCT mdp
    on mdp.BUSINESS_UNIT = olp.BUSINESS_UNIT
    and mdp.REGION = olp.REGION
    and mdp.COUNTRY = olp.COUNTRY
    and mdp.PRODUCT_SKU = olp.PRODUCT_SKU
LEFT JOIN _exchanges e
    ON ppb.customer_id = e.customer_id
    AND ppb.month_date = e.month_date
group by
    ppb.customer_id
    ,ppb.month_date;
