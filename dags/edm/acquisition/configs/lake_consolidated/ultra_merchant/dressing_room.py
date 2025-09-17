from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="dressing_room",
    company_join_sql="""
     SELECT DISTINCT
         L.dressing_room_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.dressing_room AS L
     ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column("dressing_room_id", "INT", uniqueness=True, key=True),
        Column("label", "VARCHAR(200)"),
        Column("store_id", "INT"),
        Column("barcode", "VARCHAR(50)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("serial_number", "VARCHAR(35)"),
        Column("battery_voltage", "INT"),
        Column("statuscode", "INT"),
    ],
    watermark_column="datetime_modified",
)
