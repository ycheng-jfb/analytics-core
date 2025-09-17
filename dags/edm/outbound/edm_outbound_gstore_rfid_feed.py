from datetime import timedelta
import pendulum
from airflow.models import DAG
from include.config import conn_ids, owners
from include.airflow.dag_helpers import chain_tasks
from include.config.email_lists import data_integration_support
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm
from airflow.operators.python import BranchPythonOperator
from include.airflow.operators.mssql_to_gcs import MssqlToGCSOperator
from include.airflow.operators.snowflake_export import SnowflakeToGCSOperator
import csv

product_feed_csv_export_sql = """SELECT
    OBJECT_CONSTRUCT(
        'item', um_i.item_number,
        'style', dp.product_sku,
        'barcodes', br_cds.labels,
        'shortDescription', COALESCE(REPLACE(descriptions.product_name, '"', '”'), ''),
        'longDescription', COALESCE(REPLACE(descriptions.short_description, '"', '”'), ''),

        'length', 0::DECIMAL(19,2),
        'width', 0::DECIMAL(19,2),
        'height', 0::DECIMAL(19,2),
        'dimensionsUom', '',
        'netWeight', 0::DECIMAL(19,2),
        'grossWeight', 0::DECIMAL(19,2),
        'weightUom', '',

        'productImages', ARRAY_CONSTRUCT_COMPACT(pispf.image_link,
            pispf.additional_image_link_1,
            pispf.additional_image_link_2,
            pispf.additional_image_link_3,
            pispf.additional_image_link_4,
            pispf.additional_image_link_5),
        'defaultProductImage', ubt.image_url,

        'divisionNo', merch_h.division_no,
        'divisionName', ubt.product_segment,
        'departmentNo', merch_h.department_no,
        'departmentName', ubt.category,
        'categoryId', merch_h.category_id,
        'categoryDisplayName', COALESCE(ubt.class, ''),
        'size', dus.size,
        'sizeId', dus.size_id,
        'sizeCodeOrSortingOrder', sso.size_order_id :: VARCHAR,
        'primaryColor', uw_i.color,
        'colorId', uw_i.color_code,
        'colorImageUrl', CASE WHEN dp.product_sku !='Unknown' THEN 'https://fabletics-us-cdn.justfab.com/media/images/products/'|| dp.product_sku || '/' || dp.product_sku || '-swatch_1_36x36.jpg' END,
        'onlineIndicator', CASE WHEN pdp_url IS NOT NULL THEN TRUE ELSE FALSE END,
        'onlineUrl', COALESCE(pis.pdp_url, ''),

        'originalStorePrice', dp.retail_unit_price :: DECIMAL(19,4),
        'discountedStorePrice', dp.retail_unit_price :: DECIMAL(19,4),
        'currency', ds.store_currency,
        'recommendedAlternateSkus', alt_skus.recommended_alternate_skus
    ) :: string AS json_row
FROM lake_fl_view.ultra_merchant.item um_i
JOIN edw_prod.data_model_fl.dim_item_price dip on dip.item_id=um_i.item_id
JOIN lake_fl_view.ultra_merchant.product p on dip.product_id = p.product_id
JOIN lake_view.ultra_warehouse.item uw_i ON TRIM(um_i.item_number) = TRIM(uw_i.item_number)
LEFT JOIN (SELECT DISTINCT p.item_id,
        COALESCE(dp.product_name, '') AS product_name,
        COALESCE(dp.short_description, '') AS short_description
    FROM edw_prod.data_model_fl.dim_product dp
    JOIN lake_fl_view.ultra_merchant.product p ON dp.product_id = p.product_id
    JOIN edw_prod.data_model_fl.dim_store ds ON dp.store_id = ds.store_id
    WHERE store_country = 'US' AND item_id IS NOT NULL) as descriptions ON descriptions.item_id = p.item_id
LEFT JOIN lake_fl_view.ultra_rollup.products_in_stock pis ON p.product_id = pis.product_id
JOIN lake_fl_view.ultra_rollup.products_in_stock_product_feed pispf ON pispf.product_id = p.product_id
JOIN (SELECT item_id, ARRAY_AGG(DISTINCT label) AS labels
    FROM lake_view.ultra_warehouse.item_reference
    WHERE type_code_id = 1173
    GROUP BY item_id) br_cds ON br_cds.item_id = uw_i.item_id
LEFT JOIN edw_prod.data_model_fl.dim_product dp ON dp.product_id = p.product_id
LEFT JOIN edw_prod.data_model_fl.dim_store ds ON dp.store_id = ds.store_id
LEFT JOIN edw_prod.data_model_fl.dim_sku dsku ON dsku.sku = dp.sku
LEFT JOIN (
    SELECT um_i.item_number,
            dp.product_sku,
            ARRAY_AGG(DISTINCT NULLIF(tsc_dp.sku, 'Unknown')) AS recommended_alternate_skus
    FROM lake_fl_view.ultra_merchant.product p
    JOIN lake_fl_view.ultra_merchant.item um_i ON p.item_id = um_i.item_id
    LEFT JOIN edw_prod.data_model_fl.dim_product dp ON dp.product_id = p.product_id
    LEFT JOIN reporting_prod.data_science.tableau_single_ctl tsc ON tsc.mpid = p.master_product_id
        AND tsc.test_branch IS NULL AND tsc.rank <= 12
    LEFT JOIN edw_prod.data_model_fl.dim_product tsc_dp ON tsc_dp.product_id = tsc.rec_mpid
    GROUP BY ALL
) alt_skus ON alt_skus.item_number = um_i.item_number AND alt_skus.product_sku = dp.product_sku
LEFT JOIN REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_NO_DATE_DH ubt ON dsku.product_sku = ubt.sku
JOIN (
    SELECT
        LPAD(DENSE_RANK() OVER (ORDER BY product_segment), 3, '0') AS division_no,
        product_segment,
        LPAD(DENSE_RANK() OVER (ORDER BY category), 3, '0') AS department_no,
        category,
        LPAD(DENSE_RANK() OVER (ORDER BY class), 3, '0') AS category_id,
        class
    FROM REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_NO_DATE_DH
    WHERE product_segment IS NOT NULL
    GROUP BY product_segment, category, class ) merch_h
    ON ubt.product_segment = merch_h.product_segment
    AND ubt.category = merch_h.category
    AND ubt.class = merch_h.class
JOIN (SELECT MIN(size_id) AS size_id, UPPER(size) AS size
    FROM edw_prod.stg.dim_upm_size
    GROUP BY UPPER(size)
    ) dus ON UPPER(uw_i.size) = UPPER(dus.size)
LEFT JOIN work.dbo.size_sorting_order sso ON UPPER(sso.size) = UPPER(dus.size)
GROUP BY ALL"""

pos_sales_feed_ndjson_export_sql = """
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#store_country_mapping') IS NOT NULL
    DROP TABLE #store_country_mapping;

CREATE TABLE #store_country_mapping (
    store_id INT,
    country_abbr VARCHAR(6)
    );

--Populate store-country mapping
INSERT INTO #store_country_mapping
SELECT
    s.store_id,
    CASE
        WHEN RIGHT(sg.label, 3) LIKE ' %%' THEN RIGHT(sg.label, 2)
        WHEN rl.label IN ('The Domain') THEN 'US-CST'
        WHEN rl.label IN ('St Johns Town Center', 'Soho New York', 'Roosevelt Field', 'King of Prussia', 'Natick Mall') THEN 'US-EST'
        WHEN sg.label IN ('Just Fabulous', 'JustFabulous', 'JustFab', 'FabKids', 'Fabletics', 'ShoeDazzle') THEN 'US'
        ELSE 'US'
        END AS country_abbr
FROM ultramerchant.dbo.store AS s
    JOIN ultramerchant.dbo.store_group AS sg
        ON sg.store_group_id = s.store_group_id
    LEFT JOIN analytic.dbo.evolve_ultrawarehouse_retail_location AS rl
        ON rl.store_id = s.store_id;

IF OBJECT_ID('tempdb..#transactions_base') IS NOT NULL
    DROP TABLE #transactions_base;

CREATE TABLE #transactions_base
(
    store_retail_location_code VARCHAR(10),
    transaction_id INT,
    datetime datetime,
    transaction_type VARCHAR(10)
);

INSERT INTO #transactions_base (store_retail_location_code, transaction_id, datetime, transaction_type)
SELECT
  DISTINCT CONCAT(LEFT(scm.country_abbr, 2), s.store_retail_location_code) AS store_retail_location_code,
  o.order_id AS transaction_id,
  o.datetime_added AS datetime,
  'SELL' AS transaction_type
FROM
  ultramerchant.dbo.[ORDER] AS o WITH (NOLOCK)
  JOIN analytic.dbo.store s WITH (NOLOCK) ON o.store_id = s.store_id
      AND store_type = 'Retail'
      AND store_brand = 'Fabletics'
  JOIN #store_country_mapping scm ON s.store_id = scm.store_id
  LEFT JOIN ultramerchant.dbo.order_classification AS oc WITH(NOLOCK)
        ON o.order_id = oc.order_id AND oc.order_type_id IN (40, 48, 49, 50) -- BOPIS, Amazon Today orders, Gift
WHERE
  o.processing_statuscode IN (2050, 2080, 2100, 2110) -- Pending - Placed, Ready For Pickup, Success - Shipped, Complete
  AND oc.order_id IS NULL -- exclude BOPIS, Amazon Today, Gift orders
  AND o.datetime_modified >= CONVERT(DATE, GETDATE()-2)
UNION ALL
SELECT
  DISTINCT CONCAT(LEFT(scm.country_abbr, 2), s.store_retail_location_code) AS store_retail_location_code,
  o.order_id AS transaction_id,
  r.datetime_transaction AS DATETIME,
  'RETURN' AS transaction_type
FROM
  ultramerchant.dbo.[ORDER] AS o WITH (NOLOCK)
  JOIN ultramerchant.dbo.refund AS r WITH (NOLOCK) ON o.order_id = r.order_id
  JOIN analytic.dbo.store s WITH (NOLOCK) ON o.store_id = s.store_id
      AND store_type = 'Retail'
      AND store_brand = 'Fabletics'
  JOIN #store_country_mapping scm ON s.store_id = scm.store_id
  LEFT JOIN ultramerchant.dbo.order_classification AS oc WITH(NOLOCK)
    ON o.order_id = oc.order_id AND oc.order_type_id IN (40, 48, 49, 50) -- BOPIS, Amazon Today orders, Gift
WHERE
  r.datetime_modified >= CONVERT(DATE, GETDATE()-2)
  AND oc.order_id IS NULL; -- exclude BOPIS, Amazon Today, Gift orders

SELECT
  (
    SELECT
      DISTINCT store_retail_location_code AS [externalStoreId],
      CAST(transaction_id AS VARCHAR) AS [externalPosTransactionId],
      transaction_type AS [type],
      datetime at time zone 'Pacific Standard Time' at time zone 'UTC' AS [transactionAt],
      (
        SELECT
          DISTINCT t.product_id AS [productId],
          t.quantity AS [productEachesQuantity],
          json_query('[' + stuff(( SELECT ',' + '"' + ol3.lpn_code + '"'
          FROM ultramerchant.dbo.order_line ol3 WITH (NOLOCK)
          WHERE t.order_line_id = ol3.order_line_id
          FOR XML PATH('')),1,1,'') + ']' ) [serials]
    FROM
      (
        SELECT
          ol.order_line_id,
          i.item_number AS product_id,
          ol.quantity
        FROM
          ultramerchant.dbo.order_line ol WITH (NOLOCK)
        JOIN ultramerchant.dbo.[ORDER] o WITH (NOLOCK) ON ol.order_id = o.order_id
        JOIN ultramerchant.dbo.product p WITH (NOLOCK) ON ol.product_id = p.product_id
        JOIN ultramerchant.dbo.item i WITH (NOLOCK) ON p.item_id = i.item_id
        WHERE
          ol.order_id = a.transaction_id
          AND a.transaction_type = 'SELL'
          AND o.datetime_added = a.datetime
        UNION
        SELECT
          rl.order_line_id,
          i.item_number AS product_id,
          rl.quantity
        FROM
          ultramerchant.dbo.return_line rl WITH (NOLOCK)
          JOIN ultramerchant.dbo.[RETURN] r WITH (NOLOCK) ON rl.return_id = r.return_id
          JOIN ultramerchant.dbo.[refund] rfd WITH (NOLOCK) ON rfd.refund_id = r.refund_id
          JOIN ultramerchant.dbo.product p WITH (NOLOCK) ON rl.product_id = p.product_id
          JOIN ultramerchant.dbo.item i WITH (NOLOCK) ON p.item_id = i.item_id
        WHERE
          r.order_id = a.transaction_id
          AND a.transaction_type = 'RETURN'
          AND rfd.datetime_transaction = a.datetime
      ) t FOR JSON PATH
    ) AS [posTransactionProducts]
    FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
  ) as json
FROM #transactions_base AS a;
"""

bopis_feed_ndjson_export_sql = """
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#store_country_mapping') IS NOT NULL
    DROP TABLE #store_country_mapping;

CREATE TABLE #store_country_mapping (
    store_id INT,
    country_abbr VARCHAR(6)
    );

--Populate store-country mapping
INSERT INTO #store_country_mapping
SELECT
    s.store_id,
    CASE
        WHEN RIGHT(sg.label, 3) LIKE ' %%' THEN RIGHT(sg.label, 2)
        WHEN rl.label IN ('The Domain') THEN 'US-CST'
        WHEN rl.label IN ('St Johns Town Center', 'Soho New York', 'Roosevelt Field', 'King of Prussia', 'Natick Mall') THEN 'US-EST'
        WHEN sg.label IN ('Just Fabulous', 'JustFabulous', 'JustFab', 'FabKids', 'Fabletics', 'ShoeDazzle') THEN 'US'
        ELSE 'US'
        END AS country_abbr
FROM ultramerchant.dbo.store AS s
    JOIN ultramerchant.dbo.store_group AS sg
        ON sg.store_group_id = s.store_group_id
    LEFT JOIN analytic.dbo.evolve_ultrawarehouse_retail_location AS rl
        ON rl.store_id = s.store_id;

SELECT
  (
    SELECT
      store_retail_location_code AS [externalStoreId],
      productId AS [productId],
      epc AS [serial],
      CASE WHEN status_label IN (
        'Placed', 'Ready For Pickup', 'Shipped',
        'Split (For BOPS)', 'Cancelled'
      ) THEN 'BUY_ONLINE_PICK_UP_IN_STORE' END AS [stateTransitionReason],
      CASE WHEN status_label IN (
        'Placed', 'Ready For Pickup', 'Split (For BOPS)'
      ) THEN 'LOCKED' WHEN status_label IN('Shipped') THEN 'PARTED' WHEN status_label IN('Cancelled') THEN 'FREE' END AS [currentState],
      datetime_utc AS [updatedAt]
      FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
  ) AS json
FROM
  (
    SELECT
      DISTINCT CONCAT(LEFT(scm.country_abbr, 2), s.store_retail_location_code) AS store_retail_location_code,
      i.item_number AS productId,
      COALESCE(ol.lpn_code, NULL) AS epc,
      o.datetime_added at time zone 'Pacific Standard Time' at time zone 'UTC' AS datetime_utc,
      sc.label AS status_label
    FROM
      ultramerchant.dbo.[ORDER] o WITH (nolock)
      JOIN ultramerchant.dbo.order_line ol WITH (nolock) ON ol.order_id = o.order_id
      JOIN ultramerchant.dbo.statuscode AS sc WITH (nolock) ON sc.statuscode = o.processing_statuscode
      JOIN ultramerchant.dbo.order_classification AS oc WITH (nolock) ON o.order_id = oc.order_id
      AND oc.order_type_id IN (40, 48, 49) -- Amazon Today orders, BOPIS orders
      JOIN ultramerchant.dbo.store st ON st.store_id = o.store_id
      LEFT JOIN ultramerchant.dbo.ORDER_DETAIL od WITH(NOLOCK)
			ON od.ORDER_ID = o.ORDER_ID
			AND od.NAME = 'retail_store_id'
      JOIN analytic.dbo.store s ON s.store_id = COALESCE(TRY_CONVERT(INT,od.VALUE), o.STORE_ID)
      JOIN #store_country_mapping scm ON s.store_id = scm.store_id
      JOIN ultramerchant.dbo.product p WITH (NOLOCK) ON ol.product_id = p.product_id
      JOIN ultramerchant.dbo.item i WITH (NOLOCK) ON p.item_id = i.item_id
      LEFT JOIN ultramerchant.dbo.order_classification AS oc_m WITH (nolock) ON o.order_id = oc_m.order_id
        AND oc.order_type_id = 37 -- Split Order Master
    WHERE
      o.datetime_added >= CONVERT(date, getdate()-2)
      AND sc.statuscode IN (2080, 2050, 2100, 2140, 2200)
      AND oc_m.order_id IS NULL -- Filter out master_order_ids
    ) as a"""


def check_feed_runtime_fn(**kwargs):
    execution_time = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour == 5 and execution_time.minute == 0:
        return [tasks[f"product_feed_PROD"].task_id]
    else:
        return []


default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 8, 19, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "sla": timedelta(minutes=15),
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_outbound_gstore_rfid_feed",
    default_args=default_args,
    schedule="0/15 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    sla_miss_callback=slack_sla_miss_edm,
)

datetime_param = (
    "{{macros.tfgdt.to_pst(macros.datetime.now()).strftime('%Y%m%d-%H%M%S')}}"
)
date_param = "{{macros.tfgdt.to_pst(macros.datetime.now()).strftime('%Y%m%d')}}"


environments = ["PROD", "QA"]

tasks = {}

with dag:
    for environment in environments:
        if environment.upper() == "QA":
            bucket_name = "qa-fabletics-standard"
            bopis_feed_path = "external/inbound/fulfillments"
            pos_feed_path = "external/inbound/posFeed"
            product_feed_path = "external/inbound/productFeed"
            gcp_conn_id = conn_ids.Google.cloud_rfid
        elif environment.upper() == "PROD":
            bucket_name = "production-fabletics-standard"
            bopis_feed_path = "external/inbound/fulfillments"
            pos_feed_path = "external/inbound/posFeed"
            product_feed_path = "external/inbound/productFeed"
            gcp_conn_id = "google_cloud_rfid_prod"
        else:
            bucket_name = "development-fabletics-standard"
            bopis_feed_path = "object/external/inbound/fulfillments"
            pos_feed_path = "object/external/inbound/posFeed"
            product_feed_path = "object/external/inbound/productFeed"
            gcp_conn_id = conn_ids.Google.cloud_rfid

        tasks[f"bopis_feed_{environment}"] = MssqlToGCSOperator(
            task_id=f"bopis_feed_{environment}",
            sql_or_path=bopis_feed_ndjson_export_sql,
            bucket_name=bucket_name,
            filename=f"{datetime_param}.ndjson",
            mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
            gcp_conn_id=gcp_conn_id,
            remote_dir=bopis_feed_path,
            dag=dag,
        )

        tasks[f"pos_sales_feed_{environment}"] = MssqlToGCSOperator(
            task_id=f"pos_sales_feed_{environment}",
            sql_or_path=pos_sales_feed_ndjson_export_sql,
            bucket_name=bucket_name,
            filename=f"{datetime_param}.ndjson",
            mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
            gcp_conn_id=gcp_conn_id,
            remote_dir=pos_feed_path,
            dag=dag,
        )

        tasks[f"product_feed_{environment}"] = SnowflakeToGCSOperator(
            task_id=f"product_feed_{environment}",
            sql_or_path=product_feed_csv_export_sql,
            field_delimiter="|",
            header=False,
            quoting=csv.QUOTE_MINIMAL,
            bucket_name=bucket_name,
            filename=f"{date_param}.ndjson",
            quotechar="|",
            gcp_conn_id=gcp_conn_id,
            remote_dir=product_feed_path,
            dag=dag,
        )

    check_feed_runtime = BranchPythonOperator(
        python_callable=check_feed_runtime_fn, task_id=f"check_feed_runtime"
    )

for feed in ["bopis_feed", "pos_sales_feed", "product_feed"]:
    chain_tasks(tasks[f"{feed}_PROD"], tasks[f"{feed}_QA"])

check_feed_runtime >> tasks[f"product_feed_PROD"]
