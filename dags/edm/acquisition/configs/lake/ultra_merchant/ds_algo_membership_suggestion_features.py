from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="ds_algo_membership_suggestion_features",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("ds_algo_membership_suggestion_features_id", "INT", uniqueness=True),
        Column("membership_suggestion_id", "INT"),
        Column("ds_algo_registry_version_id", "INT"),
        Column("feature_body", "VARCHAR"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
