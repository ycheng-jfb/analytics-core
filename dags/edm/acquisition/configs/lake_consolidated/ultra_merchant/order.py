from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order",
    cluster_by="trunc(order_id, -5)",
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        LEFT JOIN {database}.{source_schema}."ORDER" AS L
            ON DS.STORE_ID = L.STORE_ID""",
    column_list=[
        Column("order_id", "INT", uniqueness=True, key=True),
        Column("store_id", "INT"),
        Column("customer_id", "INT", key=True),
        Column("order_source_id", "INT"),
        Column("order_tracking_id", "INT"),
        Column("shipping_address_id", "INT", key=True),
        Column("billing_address_id", "INT", key=True),
        Column("payment_method", "VARCHAR(25)"),
        Column("auth_payment_transaction_id", "INT", key=True),
        Column("capture_payment_transaction_id", "INT", key=True),
        Column("master_order_id", "INT", key=True),
        Column("session_id", "INT", key=True),
        Column("payment_option_id", "INT"),
        Column("reserved_id", "INT"),
        Column("code", "VARCHAR(50)"),
        Column("subtotal", "NUMBER(19, 4)"),
        Column("shipping", "NUMBER(19, 4)"),
        Column("tax", "NUMBER(19, 4)"),
        Column("discount", "NUMBER(19, 4)"),
        Column("credit", "NUMBER(19, 4)"),
        Column("estimated_weight", "DOUBLE"),
        Column("ip", "VARCHAR(15)"),
        Column("date_added", "TIMESTAMP_NTZ(0)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("date_placed", "TIMESTAMP_NTZ(0)"),
        Column("datetime_processing_modified", "TIMESTAMP_NTZ(3)"),
        Column("datetime_payment_modified", "TIMESTAMP_NTZ(3)"),
        Column("date_shipped", "TIMESTAMP_NTZ(0)"),
        Column("processing_statuscode", "INT"),
        Column("payment_statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("store_domain_id", "INT", key=True),
        Column("membership_level_id", "INT"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
        Column("currency_code", "VARCHAR(3)"),
        Column("store_group_id", "INT"),
        Column("membership_brand_id", "INT"),
        Column("datetime_shipped", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_shipped", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
    post_sql="""CREATE TABLE IF NOT EXISTS work.dbo.order_dups_dbsplit AS
SELECT * FROM lake_consolidated.ultra_merchant."ORDER" WHERE 1=2;

INSERT INTO work.dbo.order_dups_dbsplit
SELECT * FROM lake_consolidated.ultra_merchant."ORDER"
        WHERE order_id IN (SELECT order_id
            FROM lake_consolidated.ultra_merchant."ORDER"
            GROUP BY order_id
            HAVING COUNT(1) > 1);

DELETE FROM lake_consolidated.ultra_merchant."ORDER"
        WHERE order_id IN (SELECT order_id
            FROM lake_consolidated.ultra_merchant."ORDER"
            GROUP BY order_id
            HAVING COUNT(1) > 1)
        AND (data_source_id = 10 AND meta_company_id != 10)
          OR (data_source_id = 40 AND meta_company_id NOT IN (20, 30, 40));""",
)
