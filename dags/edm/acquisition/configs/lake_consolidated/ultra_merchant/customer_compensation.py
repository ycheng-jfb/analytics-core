from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer_compensation",
    company_join_sql="""
     SELECT DISTINCT
         L.customer_compensation_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.customer_compensation AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("customer_compensation_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("customer_compensation_type_id", "INT"),
        Column("promo_id", "INT", key=True),
        Column("points", "INT"),
        Column("amount", "NUMBER(19, 4)"),
        Column("label", "VARCHAR(50)"),
        Column("tier", "VARCHAR(50)"),
        Column("description", "VARCHAR"),
        Column("requires_approval", "INT"),
        Column("expiration_days", "INT"),
        Column("active", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("customer_compensation_code", "VARCHAR(255)"),
        Column("customer_compensation_system_id", "INT"),
        Column("customer_compensation_expiration_type_id", "INT"),
        Column("membership_type_id", "INT", key=True),
        Column("created_by_administrator_id", "INT"),
        Column("customer_compensation_offer_type_id", "INT"),
        Column("customer_compensation_segment_id", "INT"),
        Column("end_datetime", "TIMESTAMP_NTZ(3)"),
        Column("interval_days", "INT"),
        Column("modified_by_administrator_id", "INT"),
        Column("start_datetime", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_modified",
)
