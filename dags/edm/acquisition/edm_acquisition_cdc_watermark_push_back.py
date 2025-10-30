import pendulum
from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from functools import cached_property

from edm.acquisition.configs import get_table_config
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.config import conn_ids, owners
from include.config.email_lists import data_integration_support
from include.utils.context_managers import ConnClosing


def is_hex(val: str):
    val = val.strip('"')
    if not val.startswith('0x'):
        return False
    try:
        bytes.fromhex(val.replace('0x', ''))
        return True
    except Exception:
        return False


class CDCSourceWatermarkUpdateOpertor(BaseOperator):
    """
    We record watermarks in postgres airflow metastore.

    This operator reads from the metastore and pushes back to the source server so that they can
    purge records we have pulled.

    """

    TEMP_TABLE_NAME = '#temp'
    TRACKER_GROUP_NAME = 'airflow'
    CDC_CONN_ID = conn_ids.MsSql.dbp61_app_airflow
    TABLE_LIST = [
        "lake.ultra_warehouse_cdc.case_item__del",
        "lake.ultra_warehouse_cdc.inventory__del",
    ]

    @cached_property
    def mssql_hook(self):
        return MsSqlOdbcHook(self.CDC_CONN_ID)

    @cached_property
    def postgres_hook(self):
        return PostgresHook(conn_ids.Postgres.aws_mwaa_prod)

    @property
    def source_query(self):
        return """
        select process_name, value as high_watermark
        from tfg_meta.process_state
        where namespace = 'acquisition_cdc'
        """

    @property
    def target_update_cmd(self):
        return f"""UPDATE t
        SET t.lastwatermarkprocessed = convert(binary(8), s.high_watermark, 1)
        FROM um_replicated.dbo.watermark_tracker t
        JOIN {self.TEMP_TABLE_NAME} s ON s.table_name = t.tablename
            AND t.group_name = '{self.TRACKER_GROUP_NAME}'
        """

    def get_watermarks(self):
        df = self.postgres_hook.get_pandas_df(self.source_query)
        df['table_name'] = df['process_name']
        df = df[['table_name', 'high_watermark']]
        df.high_watermark = df.high_watermark.apply(lambda x: x.strip('"'))
        df = df[df.high_watermark.apply(is_hex)]
        watermark_dict = dict(list(df.itertuples(index=False)))
        result = []
        for cfg in map(get_table_config, self.TABLE_LIST):
            try:
                repl_timestamp = watermark_dict[cfg.full_target_table_name]
                result.append((cfg.table, repl_timestamp))
            except KeyError:
                pass
        return result

    def push_back_watermarks(self, watermark_list):
        with ConnClosing(self.mssql_hook.get_conn()) as cnx:
            cur = cnx.cursor()
            cur.fast_executemany = True
            for table, val in watermark_list:
                proc = 'um_replicated.dbo.watermark_tracker_upd'
                call = (
                    f"EXEC {proc} "
                    f"@tablename='{table}', "
                    f"@repl_timestamp={val}, "
                    f"@groupname='{self.TRACKER_GROUP_NAME}'"
                )
                print(call)
                cur.execute(call)

    def execute(self, context=None):
        watermark_list = self.get_watermarks()
        self.push_back_watermarks(watermark_list)

    def dry_run(self):
        for val in self.get_watermarks():
            print(val)


default_args = {
    'start_date': pendulum.datetime(2019, 11, 19, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": slack_failure_edm,
}


dag = DAG(
    dag_id='edm_acquisition_cdc_watermark_push_back',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
)

with dag:
    task = CDCSourceWatermarkUpdateOpertor(task_id='watermark_push_back')
