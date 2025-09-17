from datetime import timedelta

import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.product_image_url import SavageXImageUrlOperator
from include.config import owners
from include.config.email_lists import data_integration_support

product_sku_sql = """
    SELECT DISTINCT
         IFNULL(dsku.product_sku, 'Unknown') AS product_sku
        ,IFNULL(p2.label,'Unknown') AS product_name
        ,p3.label
        ,IFF(nvl(p1.master_product_id, -1) = -1,
            p1.meta_original_product_id,
            edw_prod.stg.udf_unconcat_brand(p1.master_product_id)) AS master_product_id
    FROM lake_consolidated.ultra_merchant.product p1
    JOIN lake_consolidated.ultra_merchant.product p2 ON COALESCE(p1.master_product_id, p1.product_id) = p2.product_id
    JOIN lake_consolidated.ultra_merchant_history.product p3 on COALESCE(p1.master_product_id, p1.product_id) = p3.product_id
    JOIN lake_consolidated.ultra_merchant.item i ON COALESCE(p2.item_id, p1.item_id) = i.item_id
    JOIN lake_consolidated.ultra_merchant.store ds ON p2.default_store_id = ds.store_id
    JOIN lake_consolidated.ultra_merchant.store_group sg ON ds.store_group_id = sg.store_group_id
    JOIN edw_prod.stg.dim_sku dsku ON TRIM(i.item_number) = dsku.sku
    WHERE product_sku != 'Unknown'
    AND sg.label ILIKE 'Sav%%'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY
                                LOWER(TRIM(IFNULL(dsku.product_sku,'Unknown'))),
                                LOWER(TRIM(IFNULL(p2.label,'Unknown'))),
                                IFF(NVL(p1.master_product_id, -1) = -1, p1.product_id, p1.master_product_id)
                              ORDER BY
                                IFF(NVL(p1.master_product_id, -1) = -1, p1.product_id, p1.master_product_id)
                              ) = 1;
"""

set_sku_sql = """
    SELECT DISTINCT
       IFNULL(p2.label,'Unknown') AS product_name
      ,IFNULL(p2.alias, 'Unknown') AS product_sku
      ,p3.label
    FROM lake_consolidated.ultra_merchant.product p1
    JOIN lake_consolidated.ultra_merchant.product p2 ON COALESCE(p1.master_product_id, p1.product_id) = p2.product_id
    JOIN lake_consolidated.ultra_merchant_history.product p3 on COALESCE(p1.master_product_id, p1.product_id) = p3.product_id
    JOIN edw_prod.stg.dim_store ds ON p2.default_store_id = ds.store_id
    JOIN edw_prod.stg.dim_product_type dpt ON p2.product_type_id = dpt.product_type_id
    WHERE product_sku != 'Unknown'
      AND lower(dpt.product_type_name) = 'bundle'
      AND ds.store_brand = 'Savage X'
      AND ds.store_country = 'US'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY
                                LOWER(TRIM(IFNULL(p2.label,'Unknown'))),
                                LOWER(TRIM(IFNULL(p2.alias,'Unknown')))
                              ORDER BY IFNULL(p2.label,'Unknown')
                              ) = 1;
"""

default_args = {
    "start_date": pendulum.datetime(2021, 11, 25, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_sxf_image_urls",
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)

with dag:
    product_urls = SavageXImageUrlOperator(
        task_id="edw_prod.reference.product_image_url_sxf_load",
        output_table="edw_prod.reference.product_image_url_sxf",
        sku_query_or_path=product_sku_sql,
    )

    set_urls = SavageXImageUrlOperator(
        task_id="edw_prod.reference.set_image_url_sxf_load",
        output_table="edw_prod.reference.set_image_url_sxf",
        sku_type="Set",
        sku_query_or_path=set_sku_sql,
    )
