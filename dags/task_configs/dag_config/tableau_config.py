from dataclasses import dataclass

from include.utils.snowflake import Column


@dataclass
class Config:
    operator: str
    database: str
    schema: str
    table: str
    column_list: list
    yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
    api_version: str
    schema_version: str

    @property
    def s3_prefix(self):
        return f"lake/{self.database}.{self.schema}.{self.table}/{self.schema_version}"


config_list = [
    Config(
        operator="TableauPermissions",
        database="lake",
        schema="tableau",
        table="permissions",
        column_list=[
            Column("project_id", "VARCHAR", source_name="project_id", uniqueness=True),
            Column(
                "grantee_type", "VARCHAR", source_name="grantee_type", uniqueness=True
            ),
            Column("grantee_id", "VARCHAR", source_name="grantee_id", uniqueness=True),
            Column("capability_name", "VARCHAR", source_name="capability_name"),
            Column("capability_mode", "VARCHAR", source_name="capability_mode"),
            Column(
                "updated_at",
                "TIMESTAMP_LTZ(3)",
                source_name="updated_at",
                delta_column=True,
            ),
        ],
        api_version="3.15",
        schema_version="v1",
    ),
    Config(
        operator="TableauDataSourcesToS3Operator",
        database="lake",
        schema="tableau",
        table="datasources",
        column_list=[
            Column("id", "VARCHAR", source_name="id", uniqueness=True),
            Column("name", "VARCHAR", source_name="name"),
            Column("type", "VARCHAR", source_name="type"),
            Column("content_url", "VARCHAR", source_name="contentUrl"),
            Column("description", "VARCHAR", source_name="description"),
            Column("encrypt_extracts", "VARCHAR", source_name="encryptExtracts"),
            Column("has_extracts", "VARCHAR", source_name="hasExtracts"),
            Column("is_certified", "VARCHAR", source_name="isCertified"),
            Column(
                "use_remote_query_agent", "VARCHAR", source_name="useRemoteQueryAgent"
            ),
            Column("tabl_created_at", "TIMESTAMP_LTZ(3)", source_name="createdAt"),
            Column("tabl_updated_at", "TIMESTAMP_LTZ(3)", source_name="updatedAt"),
            Column("project_id", "VARCHAR", source_name="project_id"),
            Column("project_name", "VARCHAR", source_name="project_name"),
            Column("owner_id", "VARCHAR", source_name="owner_id"),
            Column(
                "updated_at",
                "TIMESTAMP_LTZ(3)",
                source_name="updated_at",
                delta_column=True,
            ),
        ],
        api_version="3.10",
        schema_version="v2",
    ),
    Config(
        operator="TableauWorkbookConnectionsToS3Operator",
        database="lake",
        schema="tableau",
        table="workbook_connections",
        column_list=[
            Column("id", "VARCHAR", source_name="id", uniqueness=True),
            Column(
                "workbook_id", "VARCHAR", source_name="workbook_id", uniqueness=True
            ),
            Column("type", "VARCHAR", source_name="type"),
            Column("embed_password", "VARCHAR", source_name="embedPassword"),
            Column("server_address", "VARCHAR", source_name="serverAddress"),
            Column("server_port", "VARCHAR", source_name="serverPort"),
            Column("user_name", "VARCHAR", source_name="userName"),
            Column(
                "query_tagging_enabled", "VARCHAR", source_name="queryTaggingEnabled"
            ),
            Column("datasource_id", "VARCHAR", source_name="datasource_id"),
            Column("datasource_name", "VARCHAR", source_name="datasource_name"),
            Column(
                "updated_at",
                "TIMESTAMP_LTZ(3)",
                source_name="updated_at",
                delta_column=True,
            ),
        ],
        api_version="3.10",
        schema_version="v2",
    ),
    Config(
        operator="TableauWorkbooksToS3Operator",
        database="lake",
        schema="tableau",
        table="workbooks",
        column_list=[
            Column("id", "VARCHAR", source_name="id", uniqueness=True),
            Column("name", "VARCHAR", source_name="name"),
            Column("description", "VARCHAR", source_name="description"),
            Column("content_url", "VARCHAR", source_name="contentUrl"),
            Column("webpage_url", "VARCHAR", source_name="webpageUrl"),
            Column("show_tabs", "VARCHAR", source_name="showTabs"),
            Column("size", "VARCHAR", source_name="size"),
            Column("tabl_created_at", "VARCHAR", source_name="createdAt"),
            Column("tabl_updated_at", "VARCHAR", source_name="updatedAt"),
            Column("encrypt_extracts", "VARCHAR", source_name="encryptExtracts"),
            Column("default_view_id", "VARCHAR", source_name="defaultViewId"),
            Column("project_id", "VARCHAR", source_name="project_id"),
            Column("project_name", "VARCHAR", source_name="project_name"),
            Column("owner_id", "VARCHAR", source_name="owner_id"),
            Column("owner_name", "VARCHAR", source_name="owner_name"),
            Column(
                "data_acceleration_config_acceleration_enabled",
                "VARCHAR",
                source_name="dataAccelerationConfig_accelerationEnabled",
            ),
            Column(
                "updated_at",
                "TIMESTAMP_LTZ(3)",
                source_name="updated_at",
                delta_column=True,
            ),
        ],
        api_version="3.10",
        schema_version="v2",
    ),
]
