from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="membership_reward_plan_transaction_type",
    column_list=[
        Column("membership_reward_plan_transaction_type_id", "INT", uniqueness=True),
        Column("membership_reward_plan_id", "INT"),
        Column("membership_reward_transaction_type_id", "INT"),
        Column("description", "VARCHAR(2000)"),
        Column("points", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
