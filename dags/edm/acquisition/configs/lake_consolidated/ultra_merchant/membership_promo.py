from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_promo",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_PROMO_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.MEMBERSHIP AS M
            ON DS.STORE_ID = M.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_promo AS L
            ON L.MEMBERSHIP_ID = M.MEMBERSHIP_ID """,
    column_list=[
        Column("membership_promo_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("membership_promo_type_id", "INT"),
        Column("promo_id", "INT", key=True),
        Column("membership_promo_trigger_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("date_start", "TIMESTAMP_NTZ(0)"),
        Column("date_end", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
        Column("allow_promos_of_same_type", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("membership_promo_hash_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
