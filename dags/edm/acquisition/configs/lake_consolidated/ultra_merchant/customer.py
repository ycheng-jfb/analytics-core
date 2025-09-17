from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer",
    company_join_sql="""
     SELECT DISTINCT
         L.CUSTOMER_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.customer AS L
     ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column("customer_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("store_id", "INT"),
        Column("customer_type_id", "INT"),
        Column("customer_key", "VARCHAR(50)"),
        Column("username", "VARCHAR(75)"),
        Column("password", "VARCHAR(150)"),
        Column("email", "VARCHAR(75)"),
        Column("firstname", "VARCHAR(25)"),
        Column("lastname", "VARCHAR(25)"),
        Column("name", "VARCHAR(50)"),
        Column("company", "VARCHAR(100)"),
        Column("default_address_id", "INT", key=True),
        Column("default_discount_id", "INT", key=True),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("password_salt", "VARCHAR(25)"),
        Column("ph_method", "VARCHAR(25)"),
    ],
    watermark_column="datetime_modified",
)
