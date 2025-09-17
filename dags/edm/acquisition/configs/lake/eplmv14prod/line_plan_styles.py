from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="eplmv14prod",
    schema="dbo",
    table="LinePlanStyles",
    watermark_column="ModifiedOn",
    initial_load_value="1899-01-01",
    schema_version_prefix="v2",
    column_list=[
        Column("id_column", "INT", uniqueness=True),
        Column("id_style", "INT"),
        Column("id_field", "INT"),
        Column("fvalue", "VARCHAR(100)"),
        Column("actual_value", "VARCHAR(100)", source_name="ActualValue"),
        Column("attachments", "INT", source_name="Attachments"),
        Column("created_by", "VARCHAR(25)", source_name="CreatedBy"),
        Column(
            "created_on", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="CreatedOn"
        ),
        Column("modified_by", "VARCHAR(25)", source_name="ModifiedBy"),
        Column(
            "modified_on", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="ModifiedOn"
        ),
        Column("row_version", "INT", source_name="RowVersion"),
        Column("id_color", "INT"),
    ],
)
