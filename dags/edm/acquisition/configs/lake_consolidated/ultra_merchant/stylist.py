from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="stylist",
    company_join_sql="""
        SELECT DISTINCT
            L.STYLIST_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.stylist AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("stylist_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("administrator_id", "INT"),
        Column("firstname", "VARCHAR(25)"),
        Column("lastname", "VARCHAR(25)"),
        Column("bio", "VARCHAR(2000)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("display_to_public", "INT"),
        Column("public_bio", "VARCHAR(2000)"),
        Column("active_stylist_discount", "INT"),
        Column("email", "VARCHAR(100)"),
        Column("parent_stylist_id", "INT", key=True),
        Column("datetime_last_login", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
