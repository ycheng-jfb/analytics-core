from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultrawarehouse",
    schema="dbo",
    table="chargeback_fc_processing_cost",
    schema_version_prefix="v2",
    column_list=[
        Column(
            "chargeback_fc_processing_cost_id",
            "INT",
            source_name="chargeback_FC_processing_cost_id",
        ),
        Column("fcc_bu", "VARCHAR(255)"),
        Column("fcc_market", "VARCHAR(255)"),
        Column("fcc_disposition", "NUMBER(19, 4)"),
        Column("fcc_conversion_rate", "NUMBER(19, 4)"),
        Column("fcc_processing_cost", "NUMBER(19, 4)"),
    ],
)
