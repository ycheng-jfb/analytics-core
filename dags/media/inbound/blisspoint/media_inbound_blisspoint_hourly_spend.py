import pendulum
from airflow.models import DAG
from datetime import timedelta


from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.snowflake_export import SnowflakeToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets, stages

column_list = [
    Column("business_unit", "VARCHAR", uniqueness=True),
    Column("country", "VARCHAR", uniqueness=True),
    Column("datetime", "TIMESTAMP_NTZ(9)", uniqueness=True),
    Column("channel", "VARCHAR", uniqueness=True),
    Column("subchannel", "VARCHAR", uniqueness=True),
    Column("vendor", "VARCHAR", uniqueness=True),
    Column("network_group", "VARCHAR", uniqueness=True),
    Column("network", "VARCHAR", uniqueness=True),
    Column("creative", "VARCHAR", uniqueness=True),
    Column("impressions", "NUMBER(38,8)", uniqueness=True),
    Column("media_cost", "NUMBER(38,8)", uniqueness=True),
    Column("agency_fee", "NUMBER(38,8)", uniqueness=True),
    Column("ad_serving", "NUMBER(38,8)", uniqueness=True),
    Column("last_updated", "TIMESTAMP_NTZ(9)", delta_column=True),
    Column("creative_name", "VARCHAR", uniqueness=True),
    Column("creative_parent", "VARCHAR", uniqueness=True),
]

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 6, 10, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": email_lists.airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=1),
    "priority_weight": 15,
    "execution_timeout": timedelta(hours=2),
}


dag = DAG(
    dag_id="media_inbound_blisspoint_hourly_spend",
    default_args=default_args,
    schedule="25 5 * * *",
    catchup=False,
    max_active_tasks=50,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_media,
)

with dag:
    database = "lake"
    schema = "blisspoint"
    table = "hourly_spend"
    full_table_name = f"{database}.{schema}.{table}"
    execution_date = "{{ ds_nodash }}"
    pre_merge_command = f"""DELETE FROM {full_table_name}
                            WHERE to_date(datetime) >= (
                                SELECT min(to_date(datetime))
                                FROM {full_table_name}_stg
                            );
    """
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="load_to_snowflake",
        database=database,
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_vendor}/inbound/svc_blisspoint_prod_s3/blisspoint_hourly_spend_v4/",
        copy_config=CopyConfigCsv(field_delimiter=",", header_rows=1, skip_pct=1),
        trigger_rule="all_done",
        pre_merge_command=pre_merge_command,
    )
    send_to_blisspoint_s3 = SnowflakeToS3Operator(
        task_id="sf_blisspoint_to_s3",
        sql_or_path="""SELECT *
                        FROM lake.blisspoint.hourly_spend
                            WHERE datetime::DATE >= dateadd('day', -60, current_date());
        """,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        bucket=s3_buckets.tsos_da_int_vendor,
        key=f"outbound/svc_blisspoint/spend_validation/tfg_blisspoint_hourly_spend_{execution_date}.csv.gz",
        header=True,
        compression="gzip",
        field_delimiter=",",
    )
    to_snowflake >> send_to_blisspoint_s3
