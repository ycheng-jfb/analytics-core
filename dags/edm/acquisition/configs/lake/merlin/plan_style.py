from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="merlin",
    schema="dbo",
    table="PlanStyle",
    watermark_column="DateUpdate",
    schema_version_prefix="v2",
    column_list=[
        Column("oid", "INT", uniqueness=True, source_name="OID"),
        Column("description", "VARCHAR(500)", source_name="Description"),
        Column("showroom", "VARCHAR(7)", source_name="Showroom"),
        Column("date_xfd", "TIMESTAMP_NTZ(3)", source_name="DateXFD"),
        Column("style", "INT", source_name="Style"),
        Column("shipping_port", "INT", source_name="ShippingPort"),
        Column("shipping_method", "INT", source_name="ShippingMethod"),
        Column("vendor_sku", "VARCHAR(50)", source_name="VendorSku"),
        Column("free_trade_agreement", "BOOLEAN", source_name="FreeTradeAgreement"),
        Column("fta_type", "INT", source_name="FtaType"),
        Column("incoterm", "INT", source_name="Incoterm"),
        Column("notes", "VARCHAR", source_name="Notes"),
        Column("stage", "INT", source_name="Stage"),
        Column("user_create", "VARCHAR(36)", source_name="UserCreate"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column("user_update", "VARCHAR(36)", source_name="UserUpdate"),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
        Column("investment_meeting", "INT", source_name="InvestmentMeeting"),
        Column("vendor", "INT", source_name="Vendor"),
        Column("country_of_origin", "VARCHAR(2)", source_name="CountryOfOrigin"),
        Column("imu", "DOUBLE", source_name="IMU"),
        Column("reorder", "BOOLEAN", source_name="Reorder"),
        Column("buyer", "VARCHAR(36)", source_name="Buyer"),
        Column("launch_date", "TIMESTAMP_NTZ(3)", source_name="LaunchDate"),
        Column("is_forecast_freight", "BOOLEAN", source_name="IsForecastFreight"),
        Column("factory", "INT", source_name="Factory"),
        Column("img_file_path", "VARCHAR(500)", source_name="ImgFilePath"),
    ],
)
