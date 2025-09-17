from dataclasses import dataclass

from datetime import timedelta

import pendulum

from airflow.models import DAG, BaseOperator, SkipMixin
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from functools import cached_property

from airflow.utils.task_group import TaskGroup

from include.config import owners, conn_ids
from include.airflow.callbacks.slack import slack_failure_p1, slack_sla_miss_edm_p1
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.utils.context_managers import ConnClosing

from include.airflow.operators.kubernetes import KubernetesPythonOperator
from include.utils.decorators import retry_wrapper

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 6, 3, 7, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.data_integrations,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": slack_failure_p1,
    "sla": timedelta(minutes=10),
    "email_on_retry": False,
    "email_on_failure": True,
    "email": [
        "TFG_Constructor_Alert@techstyle.com",
        "rpoornima@techstyle.com",
        "rtanneeru@techstyle.com",
    ],
}

dag = DAG(
    dag_id="edm_outbound_sftp_constructor_feed",
    default_args=default_args,
    schedule="*/10 * * * *",
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1,
    # dagrun_timeout=timedelta(minutes=10),
    sla_miss_callback=slack_sla_miss_edm_p1,
)

environment = "prod"  # ['dev', 'qa', 'prod']


def check_time(**kwargs):
    execution_time = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour == 6 and execution_time.minute == 0:
        return [full.task_id]
    else:
        return [incremental.task_id]


bcp_export_python_script = '''
import paramiko
import os
import gzip
import shutil
import json

file_name = os.environ.get('file_name')
store_brand = os.environ.get('store_brand')
store_group_region = os.environ.get('store_group_region')
store_region = os.environ.get('store_region')
is_full_load = int(os.environ.get('is_full_load'))

if store_brand == 'fbl':
    server_name = os.environ.get('mssql_fabletics_app_airflow_host')
    user_name = os.environ.get('mssql_fabletics_app_airflow_login')
    password = os.environ.get('mssql_fabletics_app_airflow_password')
    sftp_hostname = os.environ.get('sftp_constructor_fl_host')
    sftp_password = os.environ.get('sftp_constructor_fl_password')
else:
    server_name = os.environ.get('mssql_savagex_app_airflow_host')
    user_name = os.environ.get('mssql_savagex_app_airflow_login')
    password = os.environ.get('mssql_savagex_app_airflow_password')
    sftp_hostname = os.environ.get('sftp_constructor_sxf_host')
    sftp_password = os.environ.get('sftp_constructor_sxf_password')


bcp_command = f"""bcp "SELECT json_object FROM ultrasearch.[dbo].[vw_json_product_feed_{store_brand}] (NOLOCK) \
    WHERE master_product_id in (SELECT DISTINCT master_product_id \
    FROM [ultrasearch].dbo.[personalization_product_feed_{store_brand}_queue] \
    WHERE store_group_region = '{store_group_region}' \
    AND COALESCE(store_region, '') = '{store_region}' \
)" \
    queryout {file_name}.ndjson  -S {server_name} \
            -U {user_name} \
            -P {password} \
            -c \
            -u"""

os.system(bcp_command)

with open(f'{file_name}.ndjson', 'r') as file:
    line_no = 0
    for line in file:
        if line:
            line_no += 1
            try:
                json.loads(line)
            except ValueError as e:
                print(f"File is Invalid. Error at line number {line_no}. Error : {e}")
                raise e

with open(f'{file_name}.ndjson', 'rb') as f_in:
    with gzip.open(f'{file_name}.ndjson.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def upload_file_sftp(hostname, password, file_to_upload, remote_dir=""):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    remote_path = f"{file_to_upload}"

    try:
        client.connect(hostname, password=password, look_for_keys=False)
        sftp = client.open_sftp()
        sftp.put(file_to_upload, remote_path, confirm=False)
        print(f"File uploaded successfully: {remote_path} ")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
    finally:
        if 'sftp' in locals():
            sftp.close()
        client.close()

file_to_upload = f'{file_name}.ndjson.gz'

if os.path.getsize(file_to_upload) > 0:
    upload_file_sftp(sftp_hostname, sftp_password, file_to_upload)
else:
    print('File not uploaded since file size is 0')
'''


class MsSqlConstructorFeedOperator(BaseOperator, SkipMixin):
    def __init__(
        self,
        mssql_conn_id: str,
        is_full_load: bool,
        tgt_database: str,
        store_group_region: str,
        store_region: str = "",
        tgt_schema: str = "dbo",
        is_post_delete: bool = False,
        **kwargs,
    ) -> None:
        self.refresh_date = None
        self.mssql_conn_id = mssql_conn_id
        self.is_full_load = is_full_load
        self.tgt_database = tgt_database
        self.tgt_schema = tgt_schema
        self.store_group_region = store_group_region
        self.store_region = store_region
        self.is_post_delete = is_post_delete
        self.store_brand = "fbl" if "fl" == store_region.lower()[0:2] else "sxf"
        super().__init__(**kwargs)

    @cached_property
    def mssql_hook(self):
        return MsSqlOdbcHook(
            mssql_conn_id=self.mssql_conn_id,
            database=self.tgt_database,
            schema=self.tgt_schema,
        )

    @property
    def watermark_cmd(self):
        store_group_region = (
            f"''{self.store_group_region}''" if self.store_group_region else "NULL"
        )
        store_region = f"''{self.store_region}''" if self.store_region else "NULL"
        watermark_cmd = f"""SELECT TOP 1 CONVERT(VARCHAR, datetime_refresh_to, 21)
                FROM ultrasearch.log.process_log_detail
                WHERE [process_name] = '[ultrasearch].[dbo].[pr_personalization_product_feed_{self.store_brand}_sel]'
                    AND status = 'Success'
                    AND parameters LIKE '%@StoreGroupRegion = {store_group_region}%'
                    AND parameters LIKE '%@StoreRegion = {store_region}%'
                ORDER BY 1 DESC"""
        return watermark_cmd

    def get_watermark(self, con):
        cur = con.cursor()
        print(self.watermark_cmd)
        cur.execute(self.watermark_cmd)
        result = cur.fetchone()

        if self.is_full_load or result is None:
            return "1900-01-01"
        else:
            return result[0]

    def is_product_queue_empty(self, con):
        cur = con.cursor()
        cmd = f"""SELECT COUNT(1)
            FROM ultrasearch.dbo.personalization_product_feed_{self.store_brand}_queue
            WHERE COALESCE(store_region,'') ='{self.store_region}'
                AND COALESCE(store_group_region,'') ='{self.store_group_region}'"""

        print(cmd)
        cur.execute(cmd)
        result = cur.fetchone()

        if result is None or result[0] == 0:
            return True
        else:
            return False

    @property
    def sql_cmd(self) -> str:
        if not self.is_post_delete:
            cmd = f""" EXEC [ultrasearch].[dbo].[pr_personalization_product_feed_{self.store_brand}_sel]
                @RefreshDate = '{self.refresh_date}'
                , @StoreGroupRegion = '{self.store_group_region}'
                """
            cmd += (
                f", @StoreRegion = '{self.store_region}'" if self.store_region else ""
            )

        else:
            cmd = f"""DELETE FROM ultrasearch.dbo.personalization_product_feed_{self.store_brand}_queue
                 WHERE COALESCE(store_region,'') = '{self.store_region}'
                 AND COALESCE(store_group_region,'') = '{self.store_group_region}'"""

        return cmd

    def dry_run(self) -> None:
        if not self.is_post_delete:
            print(self.watermark_cmd, "\n\n")
        print(self.sql_cmd)

    @retry_wrapper(3, Exception, sleep_time=20)
    def execute(self, context=None):
        with ConnClosing(self.mssql_hook.get_conn()) as con:
            if not self.is_post_delete:
                self.refresh_date = self.get_watermark(con)
            cur = con.cursor()
            print(self.sql_cmd)
            cur.execute(self.sql_cmd)

            # Skipping tasks if no records found
            if not self.is_post_delete:
                if self.is_product_queue_empty(con) and context:
                    print("The queue table is empty. Skipping run...")
                    downstream_tasks = context["task"].get_direct_relatives(
                        upstream=False
                    )
                    self.skip(
                        context["dag_run"],
                        context["ti"].execution_date,
                        downstream_tasks,
                    )


@dataclass
class StoreConnectionSlug:
    env: str
    connection_slug: str
    store_group_region: str
    store_region: str = ""

    @property
    def slug_abbr(self):
        return f"{self.store_group_region}.{self.store_region}.{self.env}".lower()


connection_slugs = [
    # Dev Slugs
    # Fabletics
    StoreConnectionSlug("dev", "con_JzzEkWH4YTD0CHMS", "FLEU", "FLDE"),
    StoreConnectionSlug("dev", "con_uaUNuEyYqWnByE0S", "FLEU", "FLDK"),
    StoreConnectionSlug("dev", "con_5w1ZeUMJTFQ944jo", "FLEU", "FLES"),
    StoreConnectionSlug("dev", "con_wPWnjf6zj1zzjmOy", "FLEU", "FLFR"),
    StoreConnectionSlug("dev", "con_eP5VWkNsvD00oMoa", "FLEU", "FLNL"),
    StoreConnectionSlug("dev", "con_zl6znytA0QdDumMe", "FLEU", "FLSE"),
    StoreConnectionSlug("dev", "con_QMZvM9xADkfN1xIn", "FLEU", "FLUK"),
    StoreConnectionSlug("dev", "con_ZPyty9I2UFILKYKO", "FLNA", "FLCA"),
    StoreConnectionSlug("dev", "con_LPXcJ0x0PcJSxm6Z", "FLNA", "FLUS"),
    # Savage X
    StoreConnectionSlug("dev", "con_a367Tei3rZQIAt01", "SXEU", "SXDE"),
    StoreConnectionSlug("dev", "con_scgyVfLLfi2uMunl", "SXEU", "SXES"),
    StoreConnectionSlug("dev", "con_C9RupGM97GZ2cWCq", "SXEU", "SXFR"),
    StoreConnectionSlug("dev", "con_gqKmEeEgPnaMjAOt", "SXEU", "SXUK"),
    StoreConnectionSlug("dev", "con_ghqKxO0earLIGHEp", "SXNA", "SXUS"),
    StoreConnectionSlug("dev", "con_HdVtEr1lp43pFHN7", "SXEU", "SXEU"),
    # QA Slugs
    # Fabletics
    StoreConnectionSlug("qa", "con_Uv0WOoM8zXsbSi3S", "FLEU", "FLDE"),
    StoreConnectionSlug("qa", "con_zayoRDTKkdbSG71N", "FLEU", "FLDK"),
    StoreConnectionSlug("qa", "con_8vtymWM9GWoTy6k1", "FLEU", "FLES"),
    StoreConnectionSlug("qa", "con_p8nsk6PYlY0tLxjQ", "FLEU", "FLFR"),
    StoreConnectionSlug("qa", "con_OvFADv5X5dwVTlY7", "FLEU", "FLNL"),
    StoreConnectionSlug("qa", "con_X2WbSrPrviMu5ZLO", "FLEU", "FLSE"),
    StoreConnectionSlug("qa", "con_c5vrwp2q3qOC7jTr", "FLEU", "FLUK"),
    StoreConnectionSlug("qa", "con_V6ddUiUSMQWi5uPi", "FLNA", "FLCA"),
    StoreConnectionSlug("qa", "con_XzxGPACc6UdzjmqO", "FLNA", "FLUS"),
    # Savage X
    StoreConnectionSlug("qa", "con_crsuj29pCNNUNbrP", "SXEU", "SXDE"),
    StoreConnectionSlug("qa", "con_GDyTThqXoidKizVU", "SXEU", "SXES"),
    StoreConnectionSlug("qa", "con_ZN2Wi22ffZuk26pz", "SXEU", "SXFR"),
    StoreConnectionSlug("qa", "con_T6ghcIeZquVLuAI3", "SXEU", "SXUK"),
    StoreConnectionSlug("qa", "con_ooPpa8zJK4Qeyk8e", "SXNA", "SXUS"),
    StoreConnectionSlug("qa", "con_zlqurmSZxMqhCKWp", "SXEU", "SXEU"),
    # Prod Slugs
    # Fabletics
    StoreConnectionSlug("prod", "con_fVIKwbfmKwFWmhwG", "FLEU", "FLDE"),
    StoreConnectionSlug("prod", "con_5fRID9SHZkYOp0Zj", "FLEU", "FLDK"),
    StoreConnectionSlug("prod", "con_YgYL1aZl47wxXDEI", "FLEU", "FLES"),
    StoreConnectionSlug("prod", "con_DJd3yqxefd2VBZ3J", "FLEU", "FLFR"),
    StoreConnectionSlug("prod", "con_eUl8ZmT39vSJc5VY", "FLEU", "FLNL"),
    StoreConnectionSlug("prod", "con_3SG7uwbJNLRi9iSJ", "FLEU", "FLSE"),
    StoreConnectionSlug("prod", "con_HZwIFIYPjU1KVUJQ", "FLEU", "FLUK"),
    StoreConnectionSlug("prod", "con_VXnRFkhgHIiUj5EQ", "FLNA", "FLCA"),
    StoreConnectionSlug("prod", "con_EBYByVX20xoJzM0z", "FLNA", "FLUS"),
    # Savage X
    StoreConnectionSlug("prod", "con_uBAQCJ6YWOeYKdA0", "SXEU", "SXDE"),
    StoreConnectionSlug("prod", "con_quFOkhFjku5zODiB", "SXEU", "SXES"),
    StoreConnectionSlug("prod", "con_9PKjEbdl5U14WhoX", "SXEU", "SXFR"),
    StoreConnectionSlug("prod", "con_gD8en4JedrAeqnXR", "SXEU", "SXUK"),
    StoreConnectionSlug("prod", "con_uH4txgVqorwceZRK", "SXNA", "SXUS"),
    StoreConnectionSlug("prod", "con_U3Rs2aDMhfO8617f", "SXEU", "SXEU"),
]

with dag:
    check_is_full_load = BranchPythonOperator(
        task_id="check_time", python_callable=check_time
    )

    full_task_group = TaskGroup("full_load")
    incremental_task_group = TaskGroup("incremental_load")

    incremental = EmptyOperator(task_id="incremental")
    full = EmptyOperator(task_id="full")

    for connection_slug in connection_slugs:
        if connection_slug.env == environment:
            if connection_slug.store_group_region.upper().startswith("FL"):
                store_brand = "fbl"
                mssql_conn_id = conn_ids.MsSql.fabletics_app_airflow
                sftp_constructor_conn_id = "sftp_constructor_fl"
            else:
                store_brand = "sxf"
                mssql_conn_id = conn_ids.MsSql.savagex_app_airflow
                sftp_constructor_conn_id = "sftp_constructor_sxf"

            with full_task_group:
                full_load = MsSqlConstructorFeedOperator(
                    task_id=f"exec_pr_personalization_product_feed_{connection_slug.slug_abbr}_full",
                    mssql_conn_id=mssql_conn_id,
                    is_full_load=True,
                    tgt_database="ultrasearch",
                    store_group_region=connection_slug.store_group_region,
                    store_region=connection_slug.store_region,
                )

                bcp_export_full = KubernetesPythonOperator(
                    task_id=f"bcp_to_sftp_export_{connection_slug.slug_abbr}_full",
                    python_script=bcp_export_python_script,
                    conn_id_list=[mssql_conn_id, sftp_constructor_conn_id],
                    env_variables={
                        "file_name": f"{connection_slug.connection_slug}_full",
                        "store_brand": store_brand,
                        "store_group_region": connection_slug.store_group_region,
                        "store_region": connection_slug.store_region,
                        "is_full_load": "1",
                    },
                )

                full_load_completed = MsSqlConstructorFeedOperator(
                    task_id=f"delete_full_load_{connection_slug.slug_abbr}_product_ids",
                    mssql_conn_id=mssql_conn_id,
                    tgt_database="ultrasearch",
                    store_group_region=connection_slug.store_group_region,
                    store_region=connection_slug.store_region,
                    is_full_load=True,
                    is_post_delete=True,
                )

            with incremental_task_group:
                incremental_load = MsSqlConstructorFeedOperator(
                    task_id=f"exec_pr_personalization_product_feed_{connection_slug.slug_abbr}_incremental",
                    mssql_conn_id=mssql_conn_id,
                    is_full_load=False,
                    tgt_database="ultrasearch",
                    store_group_region=connection_slug.store_group_region,
                    store_region=connection_slug.store_region,
                )

                bcp_export_incremental = KubernetesPythonOperator(
                    task_id=f"bcp_to_sftp_export_{connection_slug.slug_abbr}_incremental",
                    python_script=bcp_export_python_script,
                    conn_id_list=[mssql_conn_id, sftp_constructor_conn_id],
                    env_variables={
                        "file_name": f"{connection_slug.connection_slug}_patch_delta_ignore",
                        "store_brand": store_brand,
                        "store_group_region": connection_slug.store_group_region,
                        "store_region": connection_slug.store_region,
                        "is_full_load": "0",
                    },
                )

                incremental_load_completed = MsSqlConstructorFeedOperator(
                    task_id=f"delete_incremental_{connection_slug.slug_abbr}_product_ids",
                    mssql_conn_id=mssql_conn_id,
                    tgt_database="ultrasearch",
                    store_group_region=connection_slug.store_group_region,
                    store_region=connection_slug.store_region,
                    is_full_load=False,
                    is_post_delete=True,
                )

            check_is_full_load >> [full, incremental]

            full >> full_load >> bcp_export_full >> full_load_completed

            full_load >> bcp_export_full

            (
                incremental
                >> incremental_load
                >> bcp_export_incremental
                >> incremental_load_completed
            )
