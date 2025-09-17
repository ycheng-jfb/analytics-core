from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="promo_promo_classfication",
    initial_load_value="1",
    watermark_column="promo_promo_classfication_id",
    schema_version_prefix="v2",
    column_list=[
        Column("promo_promo_classfication_id", "INT", uniqueness=True, delta_column=0),
        Column("promo_id", "INT"),
        Column("promo_classification_id", "INT"),
    ],
)
