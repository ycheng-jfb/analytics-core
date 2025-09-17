import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.elasticsearch import ElasticsearchGetSam
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, email_lists, owners, s3_buckets
from task_configs.dag_config.elasticsearch_config import dailyConfig

default_args = {
    "start_date": pendulum.datetime(2020, 4, 1, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_elasticsearch_sam_daily",
    default_args=default_args,
    schedule_interval="23 2 * * *",
    catchup=False,
)

database = "lake"
schema = "elasticsearch"
table = "sam_tool"
yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"

with dag:
    to_s3 = ElasticsearchGetSam(
        task_id="to_s3",
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        key=f"lake/{database}.{schema}.{table}/v1/{yr_mth}/{schema}_{table}_{{{{ ts_nodash }}}}.csv.gz",
        hook_conn_id=conn_ids.Elasticsearch.fabletics_es7_prod,
        column_list=[col.source_name for col in dailyConfig.sam_column_list[1:-5]],
        write_header=True,
        namespace="elasticsearch",
        process_name="sam",
        es_host="fabletics-es7.techstyle.tech",
        es_protocol="http",
        es_port="80",
        es_index="prod_image",
        es_get_query_func=dailyConfig.get_query_func,
        es_sort=dailyConfig.sort_param,
    )
    update_merge = SnowflakeProcedureOperator(
        database="lake",
        procedure="elasticsearch.sam_tool_update_merge.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )
    check_delete = SnowflakeProcedureOperator(
        database="lake",
        procedure="elasticsearch.sam_tool_check_delete.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )
    to_s3 >> update_merge >> check_delete
