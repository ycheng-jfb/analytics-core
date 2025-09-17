from dataclasses import dataclass

from include.utils.snowflake import Column


@dataclass
class BuilderConfig:
    tfg_brand: str
    content_type: str
    public_key_conn_id: str


config_list = [
    # Fabletics
    # Page Models
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="account",
        public_key_conn_id="builder_fabletics",
    ),
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="landing-page",
        public_key_conn_id="builder_fabletics",
    ),
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="category-page",
        public_key_conn_id="builder_fabletics",
    ),
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="outlet",
        public_key_conn_id="builder_fabletics",
    ),
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="page",
        public_key_conn_id="builder_fabletics",
    ),
    # Section Models
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="header",
        public_key_conn_id="builder_fabletics",
    ),
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="pdp-storytelling",
        public_key_conn_id="builder_fabletics",
    ),
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="quiz-page",
        public_key_conn_id="builder_fabletics",
    ),
    # Structured Data Models
    BuilderConfig(
        tfg_brand="Fabletics",
        content_type="quiz-config",
        public_key_conn_id="builder_fabletics",
    ),
    # FabKids
    # Page Models
    BuilderConfig(
        tfg_brand="FabKids",
        content_type="landing-page",
        public_key_conn_id="builder_fabkids",
    ),
    BuilderConfig(
        tfg_brand="FabKids", content_type="page", public_key_conn_id="builder_fabkids"
    ),
    # JustFab
    # Page Models
    BuilderConfig(
        tfg_brand="JustFab",
        content_type="landing-page",
        public_key_conn_id="builder_justfab",
    ),
    BuilderConfig(
        tfg_brand="JustFab", content_type="page", public_key_conn_id="builder_justfab"
    ),
    # ShoeDazzle
    # Page Models
    BuilderConfig(
        tfg_brand="ShoeDazzle",
        content_type="landing-page",
        public_key_conn_id="builder_shoedazzle",
    ),
    BuilderConfig(
        tfg_brand="ShoeDazzle",
        content_type="page",
        public_key_conn_id="builder_shoedazzle",
    ),
    # SXF
    # Page Models
    BuilderConfig(
        tfg_brand="SXF", content_type="landing-page", public_key_conn_id="builder_sxf"
    ),
    BuilderConfig(
        tfg_brand="SXF", content_type="page", public_key_conn_id="builder_sxf"
    ),
    # Section Models
    BuilderConfig(
        tfg_brand="SXF", content_type="quiz", public_key_conn_id="builder_sxf"
    ),
]


@dataclass
class DBConfig:
    name: str
    db: str
    schema: str
    get_table: str
    column_list: list

    @property
    def get_s3_path(self):
        return f"lake/{self.db}.{self.schema}.{self.name}_details/v1/"


db_configs = {
    "builder_ab_testing_report": DBConfig(
        name="builder_ab_testing_report",
        db="lake",
        schema="builder",
        get_table="builder_api_metadata",
        column_list=[
            Column("brand", "VARCHAR", uniqueness=True),
            Column("content_type", "VARCHAR"),
            Column("published", "VARCHAR"),
            Column("builder_id", "VARCHAR", uniqueness=True),
            Column("path", "VARCHAR"),
            Column("domain", "VARCHAR"),
            Column("site_country", "VARCHAR"),
            Column("adjusted_activated_datetime_utc", "DATE"),
            Column("adjusted_activated_datetime_pst", "DATE"),
            Column("testRatio", "VARCHAR"),
            Column("test_split", "VARCHAR"),
            Column("test_name", "VARCHAR"),
            Column("test_variation_name", "VARCHAR"),
            Column("test_variation_id", "VARCHAR"),
            Column("assignment", "VARCHAR", uniqueness=True),
            Column("psources", "VARCHAR"),
        ],
    ),
    # Please drop 'NOT NULL' constraint of 'value' column manually whenever this table is recreated
    "builder_api_targeting_report": DBConfig(
        name="builder_api_targeting_report",
        db="lake",
        schema="builder",
        get_table="builder_api_targeting",
        column_list=[
            Column("builder_id", "VARCHAR"),
            Column("property", "VARCHAR"),
            Column("operator", "VARCHAR"),
            Column("value", "VARCHAR"),
        ],
    ),
    "called_tests_report": DBConfig(
        name="called_tests_report",
        db="lake",
        schema="builder",
        get_table="called_tests",
        column_list=[
            Column("builder_id", "VARCHAR", uniqueness=True),
            Column("winning_variation", "VARCHAR"),
            Column("called_at_utc", "DATE"),
            Column("called_at_pst", "DATE"),
        ],
    ),
}

hdyh_column_list = [
    Column("query", "VARIANT"),
    Column("folders", "VARIANT"),
    Column("createdDate", "TIMESTAMP_LTZ(3)", delta_column=1),
    Column("id", "VARCHAR", uniqueness=True),
    Column("name", "VARCHAR"),
    Column("modelId", "VARCHAR"),
    Column("published", "VARCHAR"),
    Column("meta", "VARIANT"),
    Column("data", "VARIANT"),
    Column("variations", "VARIANT"),
    Column("lastUpdated", "TIMESTAMP_LTZ(3)", delta_column=0),
    Column("firstPublished", "TIMESTAMP_LTZ(3)"),
    Column("testRatio", "INTEGER"),
    Column("createdBy", "VARCHAR"),
    Column("lastUpdatedBy", "VARCHAR"),
    Column("rev", "VARCHAR"),
]
