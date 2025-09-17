import pendulum
from airflow.models import DAG

from include.config import owners, conn_ids
from include.config.email_lists import data_integration_support

from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator

fl_stylitics_product_feed_sql = """
USE DATABASE WORK;

CREATE OR REPLACE TEMPORARY TABLE _max_showroom_ubt_with_images AS (
    SELECT DISTINCT ubt.*,
                    CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/'
                            || ubt.sku || '/' || ubt.sku || '-1_271x407.jpg') AS image_url
    FROM lake.excel.fl_items_ubt ubt
    JOIN (SELECT sku,
                 MAX(current_showroom) AS max_current_showroom
                 FROM lake.excel.fl_items_ubt
                 WHERE current_showroom <= DATEADD(DAY, 45, CURRENT_DATE())
                 GROUP BY sku) AS ms
                 ON ubt.sku = ms.sku
                    AND ubt.current_showroom = ms.max_current_showroom
);

CREATE OR REPLACE TEMPORARY TABLE _fabletics_product_feed AS
SELECT feed.style AS base_sku,
       feed.product_sku AS product_sku,
       feed.item_number AS sku,
       feed.group_code AS group_code,
       feed.style AS style,
       feed.label,
       feed.product_color,
       feed.google_product_colors,
       feed.short_description,
       feed.medium_description,
       feed.long_description,
       feed.product_detail,
       feed.image_link,
       feed.additional_image_link_1,
       feed.additional_image_link_2,
       feed.additional_image_link_3,
       feed.additional_image_link_5,
       feed.laydown_image_link,
        CASE WHEN permalink IS NOT NULL AND product_link ILIKE '%https://yitty.fabletics.com/%'
             THEN 'https://yitty.fabletics.com/products/' || permalink
             WHEN permalink IS NOT NULL AND product_link ILIKE '%https://www.fabletics.com/%'
             THEN 'https://www.fabletics.com/products/' || permalink
        ELSE product_link END AS product_link,
    feed.featured_product_location_id_list,
    feed.average_review,
    feed.review_count,
    feed.product_tag_id_list,
    feed.product_tag_label_list,
    feed.product_category_label_list,
    feed.retail_unit_price AS msrp,
    feed.default_unit_price AS vip_price,
    feed.available_quantity AS inventory,
    feed.sales_count_1_day,
    feed.sales_count_5_days,
    feed.size AS size,
    ubt.lined_unlined AS ubt_lined_unlined,
    ubt.inseams_construction AS ubt_inseam,
    feed.size_type,
    feed.inventory_classification,
    DATE_FROM_PARTS(RIGHT("RELEASE_DATE_DD/MM/YYYY",4),
                     LEFT(RIGHT("RELEASE_DATE_DD/MM/YYYY",7),2),
                     LEFT("RELEASE_DATE_DD/MM/YYYY",2)) AS release_date,
    feed.season,
    feed.activity,
    feed.material,
    feed.default_product_category_hierarchy,
    feed.product_type_hierarchy,
    ubt.current_showroom AS ubt_showroom,
    ubt.current_name AS ubt_product_name,
    ubt.color AS ubt_color,
    ubt.eco_system AS ubt_ecosystem,
    CASE WHEN ubt.sub_brand = 'Scrubs' AND ubt.gender ILIKE 'MEN%' THEN 'Scrubs Mens'
         WHEN ubt.sub_brand = 'Scrubs'  THEN 'Scrubs Womens'
         WHEN ubt.sub_brand = 'Fabletics' AND ubt.gender ILIKE 'MEN%' THEN 'Fabletics Mens'
         WHEN ubt.sub_brand = 'Fabletics'  THEN 'Fabletics Womens'
         WHEN ubt.sub_brand = 'Yitty' THEN 'Yitty' END AS ubt_product_segment,
    ubt.category AS ubt_category,
    ubt.class AS ubt_class,
    ubt.subclass AS ubt_subclass,
    ubt.item_status AS ubt_lifecycle,
    ubt.initial_launch AS ubt_special_collection,
    CASE WHEN featured_product_location_id_list ILIKE '%23718%'
        OR featured_product_location_id_list ILIKE '%32680%'
        OR featured_product_location_id_list ILIKE '%32650%'
        THEN TRUE ELSE FALSE END AS clearance_flag,
    CASE WHEN inventory_classification ILIKE '%last chance%' THEN TRUE
         ELSE FALSE END AS last_chance_flag,
    CASE WHEN inventory_classification ILIKE '%lead only%' THEN TRUE
         ELSE FALSE END AS lead_only_flag,
    CASE WHEN inventory_classification ILIKE '%vip only%' THEN TRUE
         ELSE FALSE END AS vip_only_flag
FROM lake_fl.ultra_rollup.products_in_stock_product_feed feed
LEFT JOIN _max_showroom_ubt_with_images ubt ON ubt.sku = feed.product_sku
WHERE brand_name IN ('Fabletics', 'Yitty', 'FL2')
      AND store_group_id = 16
      AND is_outfit = FALSE
      AND is_bundle_product = FALSE
      AND product_type_id NOT IN (11, 4, 16)
      AND image_count > 0
      AND is_active = TRUE
      AND release_date IS NOT NULL
      AND release_date <= DATEADD(DAY, 35, CURRENT_DATE());

UPDATE _fabletics_product_feed
    SET ubt_product_segment = 'Accessories'
WHERE ubt_product_segment = 'Fabletics Mens'
      AND ubt_category = 'ACCESSORIES';

CREATE OR REPLACE TEMPORARY TABLE _no_stock AS
SELECT *
FROM (SELECT product_sku, SUM(inventory) AS inventory
      FROM _fabletics_product_feed
      GROUP BY 1)
WHERE inventory <= 20;

DELETE FROM _fabletics_product_feed
WHERE product_sku IN (SELECT product_sku FROM _no_stock);

SELECT *
FROM _fabletics_product_feed;
"""

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 11, 1, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": data_integration_support,
}

dag = DAG(
    dag_id='edm_outbound_fl_stylitics_product_feed',
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

with dag:
    post_to_sftp = SnowflakeToSFTPOperator(
        task_id='fl_stylitics_product_feed',
        sql_or_path=fl_stylitics_product_feed_sql,
        sftp_conn_id=conn_ids.SFTP.ftp_fl_stylitics_product,
        filename=f"{{{{ ds_nodash }}}}_Fabletics_Stylitics_Product_Feed.csv",
        sftp_dir="",
        field_delimiter=',',
        header=True,
    )
