from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_token_transaction",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_TOKEN_TRANSACTION_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{schema}.MEMBERSHIP_TOKEN AS MT
            ON M.MEMBERSHIP_ID = MT.MEMBERSHIP_ID
        INNER JOIN {database}.{source_schema}.membership_token_transaction AS L
            ON L.MEMBERSHIP_TOKEN_ID = MT.MEMBERSHIP_TOKEN_ID""",
    column_list=[
        Column("membership_token_transaction_id", "INT", uniqueness=True, key=True),
        Column("membership_token_id", "INT", key=True),
        Column("membership_token_transaction_type_id", "INT"),
        Column("membership_token_transaction_reason_id", "INT"),
        Column("administrator_id", "INT"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT", key=True),
        Column("amount", "NUMBER(19, 4)"),
        Column("comment", "VARCHAR(512)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
        Column("cancelled", "INT"),
        Column("scenario", "VARCHAR(100)"),
        Column("scenario_version_id", "VARCHAR(12)"),
    ],
    watermark_column="datetime_modified",
)
