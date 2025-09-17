import tempfile
from pathlib import Path
from xml.dom.minidom import parseString

import pendulum
from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ftp.hooks.ftp import FTPHook

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.mssql import MsSqlOdbcHook
from include.config import conn_ids, email_lists, owners, s3_buckets
from include.utils.context_managers import ConnClosing

default_args = {
    "start_date": pendulum.datetime(2020, 1, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_outbound_sps_freight_processor",
    default_args=default_args,
    schedule="0 22 * * *",
    catchup=False,
    max_active_tasks=6,
    max_active_runs=1,
)


class MSSQLXMLtoExternal(BaseOperator):
    """
    This operator will execute mssql_stored_procedure and writes the xml file to External location

    Args:
        mssql_conn_id: mssql connection
        mssql_proc_name: mssql stored procedure executed to retrieve data
        remote_dir: share folder for data file
        file_name: name of the file in the FTP remote path
        ftp_conn_id: ftp share connection
        s3_conn_id: s3 connection
        bucket: s3 bucket name
        s3_prefix: target s3 location
    """

    template_fields = ["file_name", "s3_prefix"]

    def __init__(
        self,
        brand,
        mssql_conn_id,
        mssql_proc_name,
        remote_dir,
        file_name,
        ftp_conn_id,
        s3_conn_id,
        bucket,
        s3_prefix,
        **kwargs,
    ):
        self.brand = brand
        self.mssql_conn_id = mssql_conn_id
        self.mssql_proc_name = mssql_proc_name
        self.remote_dir = remote_dir
        self.file_name = file_name
        self.ftp_conn_id = ftp_conn_id
        self.s3_conn_id = s3_conn_id
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        super().__init__(**kwargs)

    @staticmethod
    def get_xml_to_file(cur, local_path):
        with open(local_path, "wb") as f:
            xml_result: str = ""
            while True:
                result = cur.fetchone()
                if not result:
                    break
                else:
                    xml_result += min(result)
            dom = parseString(xml_result)
            xml_data = dom.toprettyxml(indent="  ", newl="\n", encoding="utf-8")
            f.write(xml_data)

    def export_to_file(self, mssql_stored_proc, local_path):
        mssql_hook = MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id)
        with ConnClosing(mssql_hook.get_conn()) as conn:
            cur = conn.cursor()
            cur.execute(f"{mssql_stored_proc} ?", self.brand)
            self.get_xml_to_file(cur, local_path)

    def upload_to_ftp(self, local_path):
        ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)
        remote_file = self.remote_dir + "/" + self.file_name
        ftp_hook.store_file(
            remote_full_path=remote_file, local_full_path_or_buffer=str(local_path)
        )

    def upload_to_s3(self, local_path):
        s3_hook = S3Hook(self.s3_conn_id)
        s3_hook.load_file(
            filename=local_path,
            key=self.s3_prefix,
            bucket_name=self.bucket,
            replace=True,
        )

    def execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td, "tmp")
            print(f"running... {self.mssql_proc_name}")
            self.export_to_file(
                mssql_stored_proc=self.mssql_proc_name, local_path=local_path
            )
            self.upload_to_ftp(local_path=local_path)
            self.upload_to_s3(local_path=local_path)


date = "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}"
with dag:
    brand_list = ["JFB", "FL", "SXF"]
    for brand in brand_list:
        file_name = f"TFG_{brand}_PurchaseOrder_{date}.xml"
        sql_xml_to_external = MSSQLXMLtoExternal(
            brand=brand,
            mssql_conn_id=conn_ids.MsSql.dbp40_app_airflow_rw,
            mssql_proc_name="ultrawarehouse.dbo.pr_xml_for_transmit_freight_forwarder_sel",
            remote_dir="/ftp_justfab/in/",
            file_name=file_name,
            ftp_conn_id=conn_ids.SFTP.ftp_booking_data,
            task_id=f"oocl_outbound_transmit_freight_forwarder_sel_sql_external_{brand}",
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_prefix=f"lake/transmit_freight_forwarder/v2/{file_name}",
        )
