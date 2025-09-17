from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="credit_transfer_transaction",
    company_join_sql="""
     SELECT DISTINCT
         L.CREDIT_TRANSFER_TRANSACTION_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.credit_transfer_transaction AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("credit_transfer_transaction_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("store_credit_id", "INT", key=True),
        Column("gift_certificate_id", "INT", key=True),
        Column("credit_transfer_transaction_type_id", "INT"),
        Column("credit_transfer_provider_id", "INT"),
        Column("credit_transfer_account_id", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("email", "VARCHAR(100)"),
        Column("code", "VARCHAR(50)"),
        Column("foreign_transaction_id", "VARCHAR(50)"),
        Column("source_reference_number", "VARCHAR(50)", key=True),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_transacted", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
        Column("membership_token_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
