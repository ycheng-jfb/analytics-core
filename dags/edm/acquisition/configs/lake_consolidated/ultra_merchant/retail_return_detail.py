from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="retail_return_detail",
    company_join_sql="""
        SELECT DISTINCT
            L.RETAIL_RETURN_DETAIL_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE DS
        INNER JOIN {database}.{schema}."ORDER" O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{schema}.RETAIL_RETURN R
            ON R.ORDER_ID = O.ORDER_ID
        INNER JOIN {database}.{source_schema}.retail_return_detail L
            ON L.retail_return_id = R.retail_return_id""",
    column_list=[
        Column("retail_return_detail_id", "INT", uniqueness=True, key=True),
        Column("retail_return_id", "INT", key=True),
        Column("return_id", "INT", key=True),
        Column("refund_id", "INT", key=True),
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
