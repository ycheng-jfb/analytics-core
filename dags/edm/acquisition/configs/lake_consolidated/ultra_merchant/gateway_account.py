from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="gateway_account",
    company_join_sql="""
    SELECT DISTINCT l.gateway_account_id,
       ds.company_id
    FROM {database}.reference.dim_store AS ds
    INNER JOIN {database}.{schema}.store_gateway_account AS sgc
        ON ds.store_id = sgc.store_id
    INNER JOIN {database}.{schema}.gateway_account AS l
        ON l.gateway_account_id = sgc.gateway_account_id""",
    column_list=[
        Column("gateway_account_id", "INT", uniqueness=True),
        Column("gateway_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("account", "VARCHAR(150)"),
        Column("password", "VARCHAR(200)"),
        Column("merchant_id", "VARCHAR(50)"),
        Column("descriptor_override", "VARCHAR(25)"),
        Column("phone_override", "VARCHAR(25)"),
        Column("enable_creditcard_update", "INT"),
        Column("active", "INT"),
        Column("application_id", "VARCHAR(50)"),
    ],
)
