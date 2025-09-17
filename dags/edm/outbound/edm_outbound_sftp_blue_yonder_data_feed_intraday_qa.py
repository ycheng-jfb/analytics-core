from pathlib import Path
import pendulum
from airflow.models import DAG
from include.utils import blueyonder
from include.airflow.operators.blueyonder import BlueYonderExportFeed
from include.airflow.operators.sftp import SFTPPutTouchFileOperator
from include.config import email_lists, owners, conn_ids
from include.airflow.callbacks.slack import slack_failure_edm
from include import SQL_DIR

default_args = {
    "start_date": pendulum.datetime(2024, 8, 1, 7, tz="America/Los_Angeles"),
    "retries": 2,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_outbound_sftp_blue_yonder_data_feed_intraday_qa",
    default_args=default_args,
    schedule="0 10-16/2 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
)

with dag:
    cfg = blueyonder.config_list["worklist"]
    trigger_test = SFTPPutTouchFileOperator(
        task_id="trigger_file_drop_completed_test",
        ssh_conn_id=conn_ids.SFTP.sftp_blue_yonder_test,
        local_filepath="pdcinbound_trigger_intraday.done",
        remote_filepath="/intraday/pdcinbound_trigger_intraday.done",
    )
    export_data_test = BlueYonderExportFeed(
        task_id=f"{cfg.name}_export_data_feed_test",
        sftp_conn_id=conn_ids.SFTP.sftp_blue_yonder_test,
        file_name=cfg.file_name,
        table_name=f"export_by_{cfg.name}_interface",
        append=cfg.append,
        remote_filepath="/intraday",
        generate_empty_file=cfg.generate_empty_file,
        column_list=cfg.column_list,
        database="reporting_dev",
        sql_or_path=Path(
            SQL_DIR,
            "reporting_dev",
            "procedures",
            f"blue_yonder.export_by_{cfg.name}_interface_qa.sql",
        ),
    )
    export_data_test >> trigger_test
