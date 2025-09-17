from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="jf_portal",
    schema="dbo",
    table="ShippingPort",
    watermark_column="DateUpdate",
    schema_version_prefix="v3",
    column_list=[
        Column(
            "shipping_port_id", "INT", uniqueness=True, source_name="ShippingPortId"
        ),
        Column("country_origin", "VARCHAR(2)", source_name="CountryOrigin"),
        Column("city_origin", "VARCHAR(50)", source_name="CityOrigin"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
        Column("preferred_day_of_week", "INT", source_name="PreferredDayOfWeek"),
        Column(
            "preferred_dow_origin_country_id",
            "INT",
            source_name="PreferredDOWOriginCountryId",
        ),
    ],
)
