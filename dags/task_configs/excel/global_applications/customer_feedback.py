from dataclasses import dataclass
from typing import Any, List

from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

sheets = {
    "ask_nicely": SheetConfig(
        sheet_name=0,
        schema="excel",
        table="customer_feedback_ask_nicely",
        header_rows=1,
        column_list=[
            Column("response_id", "NUMBER(38,0)"),
            Column("person_id", "NUMBER(38,0)"),
            Column("contact_id", "NUMBER(38,0)"),
            Column("answer ", "NUMBER(38,0)"),
            Column("answer_label", "NUMBER(38,0)"),
            Column("data", "STRING"),
            Column("comment", "STRING"),
            Column("note", "STRING"),
            Column("status", "STRING"),
            Column("dont_contact", "STRING"),
            Column("sent", "Datetime"),
            Column("opened", "Datetime"),
            Column("responded", "Datetime"),
            Column("last_emailed", "Datetime"),
            Column("created", "Datetime"),
            Column("segment", "STRING"),
            Column("question_type", "STRING"),
            Column("published", "STRING"),
            Column("published_name", "STRING"),
            Column("published_avatar", "STRING"),
            Column("delivery_method", "STRING"),
            Column("survey_template", "STRING"),
            Column("theme", "STRING"),
            Column("life_cycle", "STRING"),
            Column("country", "STRING"),
            Column("device_os", "STRING"),
            Column("status_new", "STRING"),
            Column("url_completed_on", "STRING"),
            Column("workflow_brand_integrity_na", "STRING"),
            Column("associate", "STRING"),
            Column("custom_property", "STRING"),
            Column("district", "STRING"),
            Column("region", "STRING"),
            Column("source", "STRING"),
            Column("store_number", "STRING"),
            Column("dashboard", "STRING"),
            Column("device", "STRING"),
            Column("force", "STRING"),
            Column("lastslug", "STRING"),
            Column("membership_status_elite", "STRING"),
            Column("storename", "STRING"),
            Column("store", "STRING"),
            Column("membership", "STRING"),
            Column("kaylee_liberty", "STRING"),
            Column("holly_hoffman", "STRING"),
            Column("jay_wood", "STRING"),
            Column("c_tech", "STRING"),
            Column("lauren_gould", "STRING"),
            Column("eric_nelson", "STRING"),
            Column("shaina_stark", "STRING"),
            Column("tag", "STRING"),
            Column("workflow_hello", "STRING"),
            Column("topic", "STRING"),
            Column("dashboard_new", "STRING"),
            Column("email_token", "STRING"),
        ],
    ),
    "reseller_ratings": SheetConfig(
        sheet_name=0,
        schema="excel",
        table="reseller_ratings",
        header_rows=1,
        column_list=[
            Column("name", "String"),
            Column("rating", "NUMBER(38,0)"),
            Column("invoice", "String"),
            Column("datetime", "String"),
            Column("comment", "String"),
            Column("comment_url", "String"),
            Column("pricing", "NUMBER(38,0)"),
            Column("future_purchases", "NUMBER(38,0)"),
            Column("shipping_delivery", "NUMBER(38,0)"),
            Column("customer_service", "NUMBER(38,0)"),
            Column("returns_refunds", "NUMBER(38,0)"),
        ],
    ),
    "trustpilot_sentiment": SheetConfig(
        sheet_name=0,
        schema="excel",
        table="trustpilot_sentiment",
        header_rows=1,
        column_list=[
            Column("review_id", "string"),
            Column("created_date", "String"),
            Column("date", "Datetime"),
            Column("title", "String"),
            Column("review ", "String"),
            Column("stars", "NUMBER(38,0)"),
            Column("reference_id", "STRING"),
            Column("tags", "String"),
            Column("sentiment", "String"),
        ],
    ),
}


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    file_pattern_list: List[str]
    sheet_config: Any

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchOperator(
            task_id=f"{self.task_id}_excel_to_s3",
            smb_dir=self.smb_dir,
            share_name="BI",
            file_pattern_list=self.file_pattern_list,
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=True,
            archive_folder="Processed",
            sheet_configs=[self.sheet_config],
            remove_header_new_lines=True,
            default_schema_version="v2",
        )

    @property
    def to_snowflake(self):
        return SnowflakeIncrementalLoadOperator(
            task_id=f"{self.task_id}_s3_to_snowflake",
            database="lake",
            schema=self.sheet_config.schema,
            table=self.sheet_config.table,
            staging_database="lake_stg",
            view_database="lake_view",
            snowflake_conn_id=conn_ids.Snowflake.default,
            column_list=self.sheet_config.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}.{self.sheet_config.table}/v2/",
            copy_config=CopyConfigCsv(header_rows=1, field_delimiter="|"),
        )


smb_path = "Inbound/GMS/CX"

customer_feedback_data_source = [
    ExcelConfig(
        task_id="ask_nicely_to_s3",
        smb_dir=smb_path,
        sheet_config=sheets["ask_nicely"],
        file_pattern_list=["Ask Nicely*.xlsx"],
    ),
    ExcelConfig(
        task_id="reseller_ratings_to_s3",
        smb_dir=smb_path,
        sheet_config=sheets["reseller_ratings"],
        file_pattern_list=["Reseller Ratings*.xlsx"],
    ),
    ExcelConfig(
        task_id="trustpilot_sentiment_to_s3",
        smb_dir=smb_path,
        sheet_config=sheets["trustpilot_sentiment"],
        file_pattern_list=["Trust Pilot Sentiment Report*.xlsx"],
    ),
]
