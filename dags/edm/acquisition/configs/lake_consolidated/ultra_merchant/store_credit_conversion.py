from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="store_credit_conversion",
    company_join_sql="""
            SELECT DISTINCT
                L.store_credit_conversion_id,
                ds.company_id
            FROM {database}.reference.dim_store AS ds
            INNER JOIN {database}.{schema}.customer AS c
            ON ds.store_group_id = c.store_group_id
            INNER JOIN {database}.{schema}.store_credit AS sc
            ON sc.customer_id=c.customer_id
            INNER JOIN {database}.{source_schema}.store_credit_conversion AS L
            ON L.original_store_credit_id=sc.store_credit_id """,
    column_list=[
        Column("store_credit_conversion_id", "INT", uniqueness=True, key=True),
        Column("original_store_credit_id", "INT", key=True),
        Column("converted_store_credit_id", "INT", key=True),
        Column("store_credit_conversion_type_id", "INT"),
        Column("administrator_id", "INT"),
        Column("comment", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
