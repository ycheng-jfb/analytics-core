from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="chargeback_detail",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("chargeback_detail_id", "INT", uniqueness=True),
        Column("chargeback_id", "INT"),
        Column("source_number", "VARCHAR(255)"),
        Column("external_reference", "INT"),
        Column("line_detail_id", "INT"),
        Column("cost", "NUMBER(19, 4)"),
        Column("quantity", "INT"),
        Column("unit", "VARCHAR(255)"),
        Column("total", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("units_shipped", "INT"),
        Column("po_defect_rate", "DOUBLE"),
        Column("defect_rate_bucket", "DOUBLE"),
        Column("incoterm", "VARCHAR(255)"),
        Column("invoice_status_code_id", "INT"),
    ],
)
