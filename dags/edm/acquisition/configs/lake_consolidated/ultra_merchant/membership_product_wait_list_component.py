from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_product_wait_list_component",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_PRODUCT_WAIT_LIST_COMPONENT_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{schema}.MEMBERSHIP_PRODUCT_WAIT_LIST AS MPWL
            ON MPWL.MEMBERSHIP_ID = M.MEMBERSHIP_ID
        INNER JOIN {database}.{source_schema}.membership_product_wait_list_component AS L
            ON L.MEMBERSHIP_PRODUCT_WAIT_LIST_ID = MPWL.MEMBERSHIP_PRODUCT_WAIT_LIST_ID """,
    column_list=[
        Column(
            "membership_product_wait_list_component_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("membership_product_wait_list_id", "INT", key=True),
        Column("component_product_id", "INT", key=True),
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
