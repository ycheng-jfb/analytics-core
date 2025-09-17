from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="cart",
    company_join_sql="""
      SELECT DISTINCT
          L.CART_ID,
          DS.COMPANY_ID
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{schema}.Session  AS S
      ON DS.STORE_ID = S.STORE_ID
      INNER JOIN {database}.{source_schema}.cart AS L
      ON S.SESSION_ID = L.SESSION_ID """,
    column_list=[
        Column("cart_id", "INT", uniqueness=True, key=True),
        Column("session_id", "INT", key=True),
        Column("shipping_address_id", "INT", key=True),
        Column("billing_address_id", "INT", key=True),
        Column("payment_method", "VARCHAR(25)"),
        Column("payment_verification_data", "VARCHAR(25)"),
        Column("creditcard_id", "INT", key=True),
        Column("echeck_id", "INT"),
        Column("store_id", "INT"),
        Column("payment_option_id", "INT"),
        Column("code", "VARCHAR(50)"),
        Column("email", "VARCHAR(75)"),
        Column("last_checkout_action", "VARCHAR(35)"),
        Column("subtotal", "NUMBER(19, 4)"),
        Column("shipping", "NUMBER(19, 4)"),
        Column("tax", "NUMBER(19, 4)"),
        Column("discount", "NUMBER(19, 4)"),
        Column("credit", "NUMBER(19, 4)"),
        Column("estimated_weight", "DOUBLE"),
        Column("use_store_credit", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("customer_id", "INT", key=True),
        Column("store_group_id", "INT"),
        Column("datetime_activated", "TIMESTAMP_NTZ(3)"),
        Column("original_session_id", "INT", key=True),
        Column("cart_hash_id", "INT"),
    ],
    watermark_column="repl_timestamp",
)
