from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="payment_transaction_creditcard_data",
    company_join_sql="""
         SELECT DISTINCT
             L.PAYMENT_TRANSACTION_ID,
             DS.COMPANY_ID
         FROM {database}.REFERENCE.DIM_STORE AS DS
         INNER JOIN {database}.{schema}.CUSTOMER AS C
         ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
         INNER JOIN {database}.{schema}.CREDITCARD AS CC
         ON CC.CUSTOMER_ID=C.CUSTOMER_ID
          INNER JOIN {database}.{source_schema}.payment_transaction_creditcard_data  L
           ON CC.CREDITCARD_ID = L.CREDITCARD_ID """,
    column_list=[
        Column("payment_transaction_id", "INT", uniqueness=True, key=True),
        Column("creditcard_id", "INT", key=True),
        Column("funding_type", "VARCHAR(25)"),
        Column("funding_balance", "NUMBER(19, 4)"),
        Column("funding_reloadable", "INT"),
        Column("is_prepaid", "INT"),
        Column("prepaid_type", "VARCHAR(25)"),
        Column("datetime_advised_retry", "TIMESTAMP_NTZ(0)"),
        Column("card_product_type", "VARCHAR(25)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("issuer_country", "VARCHAR(50)"),
    ],
    watermark_column="datetime_modified",
)
