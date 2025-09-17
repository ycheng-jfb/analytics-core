from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="PoChangeReason",
    watermark_column="DateUpdate",
    schema_version_prefix="v3",
    column_list=[
        Column(
            "po_change_reason_id",
            "INT",
            uniqueness=True,
            source_name="PoChangeReasonId",
        ),
        Column("po_change_reason_code_id", "INT", source_name="PoChangeReasonCodeId"),
        Column("po_id", "INT", source_name="PoId"),
        Column("version", "INT", source_name="Version"),
        Column("doc_comment_id", "INT", source_name="DocCommentId"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
