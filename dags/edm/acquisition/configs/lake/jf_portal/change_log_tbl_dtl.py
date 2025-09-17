from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="ChangeLogTblDtl",
    watermark_column="DateUpdate",
    schema_version_prefix="v3",
    column_list=[
        Column(
            "change_log_tbl_i_dtld",
            "INT",
            uniqueness=True,
            source_name="ChangeLogTblIDtld",
        ),
        Column("change_log_tbl_hdr_id", "INT", source_name="ChangeLogTblHdrId"),
        Column("tbl_id", "VARCHAR(50)", source_name="TblId"),
        Column("column_name", "VARCHAR(30)", source_name="ColumnName"),
        Column("old_value", "VARCHAR", source_name="OldValue"),
        Column("new_value", "VARCHAR", source_name="NewValue"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
