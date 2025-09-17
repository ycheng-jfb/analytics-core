from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="ultraidentity",
    schema="dbo",
    table="[user]",
    watermark_column="datetime_modified",
    schema_version_prefix="v2",
    column_list=[
        Column("user_id", "INT", uniqueness=True),
        Column("user_code", "VARCHAR(36)"),
        Column("firstname", "VARCHAR(255)"),
        Column("lastname", "VARCHAR(255)"),
        Column("email", "VARCHAR(255)"),
        Column("username", "VARCHAR(255)"),
        Column("password", "VARCHAR(8000)"),
        Column("directory_id", "INT"),
        Column("password_policy_id", "INT"),
        Column("datetime_last_login", "TIMESTAMP_NTZ(3)"),
        Column("invalid_login_attempts", "INT"),
        Column("datetime_locked", "TIMESTAMP_NTZ(3)"),
        Column("datetime_password_updated", "TIMESTAMP_NTZ(3)"),
        Column("datetime_token_refreshed", "TIMESTAMP_NTZ(3)"),
        Column("change_password", "BOOLEAN"),
        Column("company_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
        Column("status_code_id", "INT"),
        Column("reference", "VARCHAR(255)"),
        Column("description", "VARCHAR(8000)"),
    ],
)
