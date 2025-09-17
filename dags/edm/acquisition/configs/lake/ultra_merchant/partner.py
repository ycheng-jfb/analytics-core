from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultramerchant",
    schema="dbo",
    table="partner",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("partner_id", "INT", uniqueness=True),
        Column("code", "VARCHAR(50)"),
        Column("label", "VARCHAR(50)"),
        Column("ftp_host", "VARCHAR(75)"),
        Column("ftp_username", "VARCHAR(75)"),
        Column("ftp_password", "VARCHAR(75)"),
        Column("ftp_cert_key", "VARCHAR(75)"),
        Column("ftp_root_directory", "VARCHAR(100)"),
        Column("ftp_protocol", "VARCHAR(20)"),
        Column("encryption_key", "VARCHAR(50)"),
        Column("encryption_type", "VARCHAR(50)"),
        Column("console_username", "VARCHAR(75)"),
        Column("console_password", "VARCHAR(75)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("statuscode", "INT"),
    ],
)
