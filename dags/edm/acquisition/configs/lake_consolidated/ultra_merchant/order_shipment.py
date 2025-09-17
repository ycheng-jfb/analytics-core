from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_shipment",
    company_join_sql="""
       SELECT DISTINCT
           L.ORDER_SHIPMENT_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}."ORDER" AS C
           ON DS.STORE_ID = C.STORE_ID
       INNER JOIN {database}.{source_schema}.order_shipment AS L
           ON L.ORDER_ID = C.ORDER_ID """,
    column_list=[
        Column("order_shipment_id", "INT", uniqueness=True, key=True),
        Column("order_id", "INT", key=True),
        Column("warehouse_id", "INT"),
        Column("shipping_address_id", "INT", key=True),
        Column("date_shipped", "TIMESTAMP_NTZ(0)"),
        Column("carrier_id", "INT"),
        Column("carrier_service_id", "INT"),
        Column("zone", "INT"),
        Column("rate", "NUMBER(19, 4)"),
        Column("rate_weight", "DOUBLE"),
        Column("tracking_number", "VARCHAR(50)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("delivered", "INT"),
        Column("date_delivered", "TIMESTAMP_NTZ(0)"),
        Column("date_estimate_delivery", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
