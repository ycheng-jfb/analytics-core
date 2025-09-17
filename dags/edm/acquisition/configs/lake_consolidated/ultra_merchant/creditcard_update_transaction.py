from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="creditcard_update_transaction",
    company_join_sql="""
     SELECT DISTINCT
         L.creditcard_update_transaction_id,
         DS.company_id
     FROM {database}.reference.dim_store AS DS
     INNER JOIN {database}.{source_schema}.customer AS C
        ON DS.store_id = C.store_id
     INNER JOIN {database}.{source_schema}.creditcard AS CC
        ON CC.customer_id = C.customer_id
     INNER JOIN {database}.{source_schema}.creditcard_update_transaction AS L
     ON L.creditcard_id = CC.creditcard_id""",
    column_list=[
        Column("creditcard_update_transaction_id", "INT", uniqueness=True, key=True),
        Column("creditcard_update_batch_id", "INT", key=True),
        Column("creditcard_id", "INT", key=True),
        Column("updated_creditcard_id", "INT", key=True),
        Column("response_transaction_id", "VARCHAR(50)"),
        Column("response_result_code", "VARCHAR(25)"),
        Column("response_result_text", "VARCHAR(100)"),
        Column("comment", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_modified",
)
