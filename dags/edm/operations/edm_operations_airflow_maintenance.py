import csv
import datetime
from io import StringIO
from typing import List, Type

import pendulum
import sqlalchemy
from airflow.models import DAG, BaseOperator, DagRun, SlaMiss, TaskInstance
from airflow.models.base import Base
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import Column, CopyConfigCsv
from sqlalchemy.sql import delete

default_args = {
    "start_date": pendulum.datetime(2023, 10, 1, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_operations_airflow_maintenance",
    default_args=default_args,
    schedule='0 11 * * *',
    catchup=False,
)


class MWAAToS3Operator(BaseOperator):
    """
    Operator to extract airflow backend table data to S3.

    Attributes:
        table: sqlalchemy object of table to extract
        date_col: column to use for date comparison
        s3_conn_id: airflow connection id for S3
        s3_bucket: target S3 bucket
        s3_key: target S3 key
        column_list: list of columns to extract from table
        max_age_in_days: data newer than this param will be extracted to S3
    """

    template_fields = ("s3_key",)

    def __init__(
        self,
        table: Type[Base],
        date_col: sqlalchemy.Column,
        s3_conn_id: str,
        s3_bucket: str,
        s3_key: str,
        column_list: List[Column],
        max_age_in_days: int = 30,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table = table
        self.date_col = date_col
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.column_list = column_list
        self.max_age_in_days = max_age_in_days

    @property
    def column_names(self) -> List[str]:
        return [i.name for i in self.column_list]

    @property
    def column_entities(self) -> list:
        return [getattr(self.table, c.name) for c in self.column_list]

    @property
    def s3_hook(self) -> S3Hook:
        return S3Hook(self.s3_conn_id)

    def execute(self, context=None) -> None:
        with create_session() as session:
            s3_client = self.s3_hook.get_conn()
            query = session.query(*self.column_entities).filter(
                self.date_col >= days_ago(self.max_age_in_days)
            )
            all_rows = query.all()
            if len(all_rows) > 0:
                buf = StringIO()
                csv_writer = csv.DictWriter(buf, fieldnames=self.column_names)
                csv_writer.writeheader()
                for row in all_rows:
                    csv_writer.writerow(dict(zip(self.column_names, row)))
                s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_key, Body=buf.getvalue())
                self.log.info(f"Uploaded {self.s3_key} to {self.s3_bucket}: {len(all_rows)} rows")


class MWAAPruneTableOperator(BaseOperator):
    """
    Operator to prune older data in airflow backend tables.

    Attributes:
        table: sqlalchemy object of table to prune
        date_col: sqlalchemy column to use for date comparison
        max_age_in_days: data older than this param will be pruned
    """

    def __init__(
        self,
        table: Type[Base],
        date_col: sqlalchemy.Column,
        max_age_in_days: int = 30,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table = table
        self.date_col = date_col
        self.max_age_in_days = max_age_in_days

    def execute(self, context=None) -> None:
        with create_session() as session:
            session.execute(
                delete(self.table).where(
                    self.date_col
                    < (
                        datetime.datetime.now(datetime.timezone.utc)
                        - datetime.timedelta(days=self.max_age_in_days)
                    )
                )
            )


column_list = [
    Column("task_id", "VARCHAR(250)", uniqueness=True),
    Column("dag_id", "VARCHAR(250)", uniqueness=True),
    Column("run_id", "varchar(250)", uniqueness=True),
    Column("start_date", "TIMESTAMP_LTZ(3)"),
    Column("end_date", "TIMESTAMP_LTZ(3)"),
    Column("duration", "FLOAT8"),
    Column("state", "VARCHAR(50)"),
    Column("try_number", "NUMBER(10,0)"),
    Column("hostname", "VARCHAR(1000)"),
    Column("unixname", "VARCHAR(1000)"),
    Column("job_id", "NUMBER(10,0)"),
    Column("pool", "VARCHAR(250)"),
    Column("queue", "VARCHAR(250)"),
    Column("priority_weight", "NUMBER(10,0)"),
    Column("operator", "VARCHAR(1000)"),
    Column("queued_dttm", "TIMESTAMP_LTZ(3)"),
    Column("pid", "NUMBER(10,0)"),
    Column("max_tries", "NUMBER(10,0)"),
    Column("executor_config", "VARCHAR(1000)"),
    Column("pool_slots", "NUMBER(10,0)"),
    Column("trigger_id", "NUMBER(10,0)"),
    Column("trigger_timeout", "TIMESTAMP_LTZ(3)"),
    Column("next_method", "varchar(1000)"),
    Column("next_kwargs", "variant"),
]


with dag:
    database = "lake"
    schema = "airflow"
    table = "task_instance"
    s3_bucket = s3_buckets.tsos_da_int_inbound
    s3_prefix = f"lake/{database}.{schema}.{table}/v3"

    to_s3 = MWAAToS3Operator(
        task_id=f"{table}.to_s3",
        table=TaskInstance,
        date_col=TaskInstance.execution_date,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        s3_bucket=s3_bucket,
        s3_key=f"{s3_prefix}/taskinstance_{{{{ ts_nodash }}}}.csv",
        column_list=column_list,
        max_age_in_days=30,
    )

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id=f"{table}.to_snowflake",
        database=database,
        schema=schema,
        table=table,
        staging_database="lake_stg",
        view_database="lake_view",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigCsv(field_delimiter=',', header_rows=1),
    )

    to_s3 >> to_snowflake

    # for obj in [DagRun, SlaMiss]:
    #     prune_table = MWAAPruneTableOperator(
    #         task_id=f"{obj.__tablename__}.prune",
    #         table=obj,
    #         date_col=obj.execution_date,  # type: ignore
    #         max_age_in_days=30,
    #     )
