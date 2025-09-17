from include.airflow.operators.mssql_acquisition import (
    HighWatermarkMaxVarcharDatetimeOffset,
)
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ngc_plm_prodcomp01",
    schema="dbo",
    table="MetadataTable",
    high_watermark_cls=HighWatermarkMaxVarcharDatetimeOffset,
    watermark_column="ModifiedOn",
    schema_version_prefix="v2",
    column_list=[
        Column(
            "id_metadata_table", "INT", uniqueness=True, source_name="ID_MetaDataTable"
        ),
        Column("table_name", "VARCHAR(50)", source_name="TableName"),
        Column("description", "VARCHAR(50)", source_name="Description"),
        Column("is_role_base", "BOOLEAN", source_name="isRoleBase"),
        Column("is_visible", "BOOLEAN", source_name="isVisible"),
        Column("is_data_profile", "BOOLEAN", source_name="isDataProfile"),
        Column("is_mark_delete", "BOOLEAN", source_name="isMarkDelete"),
        Column("is_track_hist", "BOOLEAN", source_name="isTrackHist"),
        Column("table_as", "VARCHAR", source_name="TableAs"),
        Column("history_condition", "VARCHAR(1000)", source_name="HistoryCondition"),
        Column("category", "VARCHAR(500)", source_name="Category"),
        Column("after_save_bp", "VARCHAR(150)", source_name="AfterSaveBP"),
        Column("search_join", "VARCHAR(2800)", source_name="SearchJoin"),
        Column(
            "extended_history_fields", "VARCHAR", source_name="ExtendedHistoryFields"
        ),
        Column("after_load_bp", "VARCHAR(150)", source_name="AfterLoadBP"),
        Column("validate_edit_bp", "VARCHAR(150)", source_name="ValidateEditBP"),
        Column("validate_save_bp", "VARCHAR(150)", source_name="ValidateSaveBP"),
        Column("is_auto_update", "BOOLEAN", source_name="isAutoUpdate"),
        Column("table_attribute", "INT", source_name="TableAttribute"),
        Column("is_model_entity", "BOOLEAN", source_name="isModelEntity"),
        Column("created_by", "VARCHAR(256)", source_name="CreatedBy"),
        Column(
            "created_on",
            "TIMESTAMP_LTZ(7)",
            delta_column=1,
            source_name="convert(VARCHAR, CreatedOn, 127)",
        ),
        Column("modified_by", "VARCHAR(256)", source_name="ModifiedBy"),
        Column(
            "modified_on",
            "TIMESTAMP_LTZ(7)",
            delta_column=0,
            source_name="convert(VARCHAR, ModifiedOn, 127)",
        ),
        Column("row_version", "INT", source_name="RowVersion"),
    ],
)
