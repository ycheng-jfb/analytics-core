from include.airflow.operators.mssql_acquisition import (
    HighWatermarkMaxVarcharDatetimeOffset,
)
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ngc_plm_prodcomp01",
    schema="dbo",
    table="CategoryFieldEntity",
    high_watermark_cls=HighWatermarkMaxVarcharDatetimeOffset,
    watermark_column="ModifiedOn",
    schema_version_prefix="v2",
    column_list=[
        Column(
            "id_category_field_entity",
            "INT",
            uniqueness=True,
            source_name="ID_CategoryFieldEntity",
        ),
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
        Column("id_metadata_table", "INT", source_name="ID_MetaDataTable"),
        Column("id_parent_table", "INT", source_name="ID_ParentTable"),
        Column("id_category_mdf", "INT", source_name="ID_CategoryMDF"),
        Column("id_division_mdf", "INT", source_name="ID_DivisionMDF"),
    ],
)
