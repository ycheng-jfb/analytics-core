from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="dm_gateway_test_site",
    company_join_sql="""
     SELECT DISTINCT
         L.dm_gateway_test_site_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.dm_gateway AS dg
     ON DS.STORE_GROUP_ID = dg.STORE_GROUP_ID
     INNER JOIN {database}.{schema}.dm_gateway_test AS dgt
     ON dgt.dm_gateway_id= dg.dm_gateway_id
     INNER JOIN {database}.{source_schema}.dm_gateway_test_site AS L
     ON L.dm_gateway_test_id= dgt.dm_gateway_test_id """,
    column_list=[
        Column("dm_gateway_test_site_id", "INT", uniqueness=True, key=True),
        Column("dm_gateway_test_id", "INT", key=True),
        Column("dm_site_id", "INT", key=True),
        Column("dm_gateway_test_site_type_id", "INT"),
        Column("dm_gateway_test_site_class_id", "INT"),
        Column("dm_gateway_test_site_location_id", "INT"),
        Column("weight", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
