SET execution_start_datetime = current_timestamp::TIMESTAMP_LTZ;
SET max_cost_date = (SELECT MAX(cost_date) FROM reference.na_outbound_shipping_cost);
SET min_order_date = (SELECT min(fo.ORDER_LOCAL_DATETIME::DATE) FROM stg.fact_order fo WHERE order_id > 0); -- This is used in initial load only.
SET max_cost_date = nvl($max_cost_date, $min_order_date);

SET days_to_load = (SELECT datediff(day, $max_cost_date, current_date()) + 1);

CREATE OR REPLACE TEMP TABLE _cost_date AS
SELECT
    cost_date,
    date_trunc('month', dateadd(month, -2, cost_date)) AS effective_start_date,
    date_trunc('month', cost_date) AS effective_end_date
FROM
(
  SELECT
      dateadd(DAY, -1 * seq4(), current_date()) AS cost_date
  FROM table(generator(rowcount=>$days_to_load))
) A
ORDER BY cost_date;

CREATE OR REPLACE TEMP TABLE _na_outbound_shipping_cost AS
SELECT
    cd.cost_date,
    ds.store_id,
    sc.currency_code,
    o.unit_count as units,
    avg(zeroifnull(o.shipping_cost_local_amount)) AS outbound_shipping_cost
FROM stg.fact_order o -- todo: can we remove any reference to facts or dims in this reference table?
JOIN stg.dim_store ds
    ON ds.store_id = o.store_id
JOIN reference.store_currency sc
    ON sc.store_id = o.store_id
    AND o.order_local_datetime BETWEEN sc.effective_start_date AND sc.effective_end_date
JOIN stg.dim_order_sales_channel oc
    ON oc.order_sales_channel_key = o.order_sales_channel_key
JOIN stg.dim_order_status os
    ON os.order_status_key = o.order_status_key
JOIN _cost_date cd
    ON o.shipped_local_datetime::DATE >= cd.effective_start_date
    AND o.shipped_local_datetime::DATE < cd.effective_end_date
WHERE
        upper(oc.order_classification_l2) = 'PRODUCT ORDER'
    AND upper(os.order_status) = 'SUCCESS'
    AND upper(ds.store_region) = 'NA'
    AND upper(store_type) != 'RETAIL'
GROUP BY
    cd.cost_date,
    ds.store_id,
    o.unit_count,
    sc.currency_code
Order by
     cost_date,
     store_id,
     unit_count,
     currency_code;

INSERT INTO reference.na_outbound_shipping_cost
(
    cost_date,
    store_id,
    store_name,
    store_country,
    store_region,
    store_type,
    currency_code,
    units,
    outbound_shipping_cost,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    nbsc.cost_date,
    nbsc.store_id,
    ds.store_full_name,
    ds.store_country,
    ds.store_region,
    ds.store_type,
    nbsc.currency_code,
    nbsc.units,
    nbsc.outbound_shipping_cost,
    $execution_start_datetime,
    $execution_start_datetime
FROM _na_outbound_shipping_cost nbsc
JOIN stg.dim_store ds
    ON nbsc.store_id = ds.store_id
LEFT JOIN reference.na_outbound_shipping_cost nosc
    ON nbsc.cost_date = nosc.cost_date
    AND nbsc.store_id = nosc.store_id
    AND nbsc.units = nosc.units
WHERE nosc.store_id IS NULL;
