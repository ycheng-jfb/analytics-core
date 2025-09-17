from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR,
    table="membership_billing_processor",
    company_join_sql="""
     SELECT DISTINCT
         l.membership_billing_processor_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.membership AS M
     ON DS.STORE_ID = M.STORE_ID
     INNER JOIN {database}.{source_schema}.membership_billing_processor AS l
     ON l.membership_id = M.membership_id""",
    column_list=[
        Column("membership_billing_processor_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("billing_processor_id", "INT"),
        Column("order_id", "INT", key=True),
        Column("retry_only", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
