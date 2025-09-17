from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_signup",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_SIGNUP_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP_PLAN AS MP
            ON DS.STORE_GROUP_ID = MP.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.membership_signup AS L
            ON L.MEMBERSHIP_plan_ID = MP.MEMBERSHIP_plan_ID """,
    column_list=[
        Column("membership_signup_id", "INT", uniqueness=True, key=True),
        Column("session_id", "INT", key=True),
        Column("membership_plan_id", "INT", key=True),
        Column("membership_signup_type_id", "INT"),
        Column("membership_invitation_id", "INT", key=True),
        Column("customer_quiz_id", "INT", key=True),
        Column("customer_id", "INT", key=True),
        Column("shipping_address_id", "INT", key=True),
        Column("payment_method", "VARCHAR(25)"),
        Column("payment_object_id", "INT"),
        Column("payment_option_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("membership_type_id", "INT"),
        Column("membership_brand_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
