from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="creditcard_update_batch",
    company_join_sql="""
    SELECT DISTINCT l.creditcard_update_batch_id,
       ds.company_id
    FROM {database}.reference.dim_store AS ds
    INNER JOIN {database}.{schema}.store_gateway_account AS sgc
        ON ds.store_id = sgc.store_id
    INNER JOIN {database}.{schema}.gateway_account AS ga
        ON ga.gateway_account_id = sgc.gateway_account_id
    INNER JOIN {database}.{schema}.creditcard_update_batch AS l
        ON ga.gateway_account_id = l.gateway_account_id""",
    column_list=[
        Column("creditcard_update_batch_id", "INT", uniqueness=True, key=True),
        Column("gateway_account_id", "INT"),
        Column("comment", "VARCHAR(255)"),
        Column("total_creditcards", "INT"),
        Column("updated_creditcards", "INT"),
        Column("foreign_batch_id", "VARCHAR(50)", key=True),
        Column("foreign_session_id", "VARCHAR(50)", key=True),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("date_next_billing_start", "TIMESTAMP_NTZ(0)"),
        Column("date_next_billing_end", "TIMESTAMP_NTZ(0)"),
        Column("date_posted", "TIMESTAMP_NTZ(0)"),
        Column("date_results_expected", "TIMESTAMP_NTZ(3)"),
        Column("datetime_touched", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("request_file_contents", "VARCHAR"),
        Column("response_file_contents", "VARCHAR"),
    ],
    watermark_column="datetime_modified",
)
