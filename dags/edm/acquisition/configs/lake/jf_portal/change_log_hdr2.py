from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="ChangeLogHdr2",
    watermark_column="DateUpdate",
    schema_version_prefix="v3",
    column_list=[
        Column(
            "change_log_hdr_id", "INT", uniqueness=True, source_name="ChangeLogHdrId"
        ),
        Column("doc_comment_id", "INT", source_name="DocCommentId"),
        Column("doc_version", "INT", source_name="DocVersion"),
        Column("doc_type_id", "INT", source_name="DocTypeId"),
        Column("doc_id", "INT", source_name="DocId"),
        Column("po_change_reason_id", "INT", source_name="PoChangeReasonId"),
        Column("user_name", "VARCHAR(100)", source_name="UserName"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
    ],
)
