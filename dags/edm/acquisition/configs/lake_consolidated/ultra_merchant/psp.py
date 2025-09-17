from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="psp",
    company_join_sql="""SELECT DISTINCT L.psp_id, DS.company_id
    from  {database}.REFERENCE.dim_store ds
     join {database}.{schema}.customer c
         on ds.store_group_id=c.store_group_id
     join {database}.{source_schema}.psp L
        on L.customer_id=c.customer_id""",
    column_list=[
        Column("psp_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("address_id", "INT", key=True),
        Column("token", "VARCHAR(100)"),
        Column("payment_method", "VARCHAR(50)"),
        Column("type", "VARCHAR(50)"),
        Column("acct_num", "VARCHAR(50)"),
        Column("acct_name", "VARCHAR(50)"),
        Column("exp_month", "VARCHAR(2)"),
        Column("exp_year", "VARCHAR(2)"),
        Column("bank_name", "VARCHAR(50)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("alias", "VARCHAR"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("contract", "VARCHAR(20)"),
    ],
    watermark_column="datetime_modified",
)
