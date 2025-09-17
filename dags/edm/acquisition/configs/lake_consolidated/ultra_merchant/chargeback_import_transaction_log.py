from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="chargeback_import_transaction_log",
    company_join_sql="""
     SELECT DISTINCT
         L.chargeback_import_transaction_log_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.chargeback_import_transaction_log AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column(
            "chargeback_import_transaction_log_id", "INT", uniqueness=True, key=True
        ),
        Column("gateway_id", "INT"),
        Column("store_group_id", "INT"),
        Column("case_id", "VARCHAR(30)", key=True),
        Column("merchant_id", "VARCHAR(15)"),
        Column("merchant_txn_id", "VARCHAR(50)"),
        Column("processor_txn_id", "VARCHAR(20)"),
        Column("order_id", "VARCHAR(250)", key=True),
        Column("cycle", "VARCHAR(50)"),
        Column("first_four_digits", "VARCHAR(4)"),
        Column("last_four_digits", "VARCHAR(4)"),
        Column("card_type", "VARCHAR(15)"),
        Column("settlement_amount", "NUMBER(19, 4)"),
        Column("chargeback_amount", "NUMBER(19, 4)"),
        Column("chargeback_currency_type", "VARCHAR(10)"),
        Column("chargeback_type", "VARCHAR(50)"),
        Column("activity_type", "VARCHAR(50)"),
        Column("reason_code", "VARCHAR(20)"),
        Column("reason_code_description", "VARCHAR(500)"),
        Column("from_queue", "VARCHAR(50)"),
        Column("to_queue", "VARCHAR(50)"),
        Column("current_queue", "VARCHAR(50)"),
        Column("batch_label", "VARCHAR(50)"),
        Column("notes", "VARCHAR(1000)"),
        Column("original_transaction_day", "TIMESTAMP_NTZ(3)"),
        Column("dayissued_by_bank", "TIMESTAMP_NTZ(3)"),
        Column("dayreceived_by_processor", "TIMESTAMP_NTZ(3)"),
        Column("activity_date", "TIMESTAMP_NTZ(0)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_added",
)
