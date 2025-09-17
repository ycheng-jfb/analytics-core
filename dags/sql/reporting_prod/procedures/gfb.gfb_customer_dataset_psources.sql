set low_watermark_ltz = (
    select
        max(a.MONTH_DATE)
    from REPORTING_PROD.GFB.gfb_customer_dataset_base a
    );


CREATE OR REPLACE TEMP TABLE _psource_data_base AS
SELECT
       cd.customer_id,
       cd.month_date,
       cd.activation_local_datetime,
       cd.cancel_local_datetime
FROM REPORTING_PROD.GFB.GFB_CUSTOMER_DATASET_BASE cd
where
    cd.MONTH_DATE >= $low_watermark_ltz
    and cd.MONTH_DATE < date_trunc('MONTH', current_date());


CREATE OR REPLACE TEMPORARY TABLE _churn_customer_psource_pre_stg AS
select
    pdb.customer_id,
    pdb.month_date,
    pdb.activation_local_datetime,
    pdb.cancel_local_datetime,
    COUNT(DISTINCT olp.order_id)          AS order_count,
    COUNT(split_part(opc.VALUE, ':', -1))                     AS psource_count,
    COUNT(DISTINCT split_part(opc.VALUE, ':', -1))            AS psource_distinct_count,
    psource_count / order_count          AS avg_psources_per_order,
    psource_distinct_count / order_count AS avg_distinct_psources_per_order
from REPORTING_PROD.GFB.GFB_ORDER_LINE_DATA_SET_PLACE_DATE olp
join _psource_data_base pdb
    on pdb.customer_id = olp.CUSTOMER_ID
    and pdb.month_date = date_trunc(month, olp.ORDER_DATE)
JOIN lake_jfb_view.ultra_merchant.order_product_source opc
    ON opc.order_id = olp.order_id
    AND opc.product_id = opc.PRODUCT_ID
where
    olp.ORDER_CLASSIFICATION = 'product order'
group by
    pdb.customer_id,
    pdb.month_date,
    pdb.activation_local_datetime,
    pdb.cancel_local_datetime;


delete from REPORTING_PROD.GFB.gfb_customer_dataset_psources a
where
    a.MONTH_DATE >= $low_watermark_ltz;


insert into REPORTING_PROD.GFB.gfb_customer_dataset_psources
SELECT
    customer_id,
    month_date,
    activation_local_datetime,
    cancel_local_datetime,
    order_count,
    psource_count,
    psource_distinct_count,
    avg_psources_per_order,
    avg_distinct_psources_per_order
FROM _churn_customer_psource_pre_stg;
