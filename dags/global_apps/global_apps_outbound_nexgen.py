import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.mssql import MsSqlToSmbCsvOperator
from include.config import conn_ids, email_lists, owners

default_args = {
    'start_date': pendulum.datetime(2021, 1, 1, tz="America/Los_Angeles"),
    'retries': 3,
    'owner': owners.data_integrations,
    'email': email_lists.global_applications,
    'on_failure_callback': slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_outbound_nexgen",
    default_args=default_args,
    schedule="0 0,12 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


class NexgenExportOperator(MsSqlToSmbCsvOperator):
    template_fields = ["remote_path"]

    def execute(self, context=None):
        try:
            super().execute(context)
            self.mssql_hook.run('wr_onesource_label_status_upd 1;')  # Mark success
        except Exception as e:
            self.mssql_hook.run('wr_onesource_label_status_upd 0;')  # Mark failure
            raise e


with dag:
    mssql_to_smb = NexgenExportOperator(
        task_id='mssql_to_smb',
        mssql_conn_id=conn_ids.MsSql.dbp40_app_airflow,
        sql='ultrawarehouse.dbo.cron_outbound_onesource_label_sel',
        smb_conn_id=conn_ids.SMB.nas01,
        share_name='outbound_label_data',
        remote_path="nextgen/{{macros.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}}.csv.gz",
        compress=True,
    )
