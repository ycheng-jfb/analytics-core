TRUNCATE TABLE validation.finance_sales_ops_cltv_comparison;

INSERT INTO validation.finance_sales_ops_cltv_comparison
(customer_id,
 activation_key,
 vip_store_id,
 cash_gross_revenue,
 month_date)
SELECT customer_id,
       activation_key,
       vip_store_id,
       SUM(cash_gross_revenue) AS cash_gross_revenue,
       DATE_TRUNC(MONTH, date) AS month_date
FROM analytics_base.finance_sales_ops
WHERE date_object = 'placed'
  AND currency_object = 'usd'
  AND month_date != DATE_TRUNC(MONTH, CURRENT_DATE)
  AND month_date >= DATEADD(YEAR, -2, DATE_TRUNC(MONTH, CURRENT_DATE))
  AND customer_id not in (select o_customer_id from reference.cross_brand_orders)
  AND customer_id > -1
GROUP BY customer_id, activation_key, vip_store_id, DATE_TRUNC(MONTH, date)
EXCEPT
SELECT customer_id,
       activation_key,
       vip_store_id,
       SUM(cash_gross_revenue),
       month_date
FROM analytics_base.customer_lifetime_value_monthly
WHERE month_date != DATE_TRUNC(MONTH, CURRENT_DATE)
  AND month_date >= DATEADD(YEAR, -2, DATE_TRUNC(MONTH, CURRENT_DATE))
GROUP BY customer_id, activation_key, vip_store_id, month_date;

SELECT customer_id, activation_key, vip_store_id, cash_gross_revenue, month_date
FROM validation.finance_sales_ops_cltv_comparison;
