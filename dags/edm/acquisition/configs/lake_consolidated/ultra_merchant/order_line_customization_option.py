from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_line_customization_option",
    company_join_sql="""
            SELECT DISTINCT
                L.order_line_customization_option_id,
                DS.COMPANY_ID
            FROM {database}.REFERENCE.DIM_STORE AS DS
            INNER JOIN {database}.{schema}."ORDER" AS O
                ON DS.store_id = O.store_id
             INNER JOIN {database}.{source_schema}.order_line_customization_option AS L
             on L.order_id=O.order_id """,
    column_list=[
        Column("order_line_customization_option_id", "INT", uniqueness=True, key=True),
        Column("order_line_customization_id", "INT", key=True),
        Column("order_id", "INT", key=True),
        Column("order_line_id", "INT", key=True),
        Column("customization_template_id", "INT"),
        Column("customization_location_id", "INT"),
        Column("file_name", "VARCHAR(255)"),
        Column("color_choice", "VARCHAR(50)"),
        Column("font_choice", "VARCHAR(50)"),
        Column("icon_choice", "VARCHAR(50)"),
        Column("text_line_1", "VARCHAR(255)"),
        Column("text_line_2", "VARCHAR(255)"),
        Column("text_line_3", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
