from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultracms",
    schema="dbo",
    table="ui_promo_management_promos_membership_promo_type",
    schema_version_prefix="v2",
    column_list=[
        Column(
            "ui_promo_management_promos_membership_promo_type_id",
            "INT",
            uniqueness=True,
        ),
        Column("ui_promo_management_promo_id", "INT"),
        Column("membership_promo_type_id", "INT"),
    ],
)
