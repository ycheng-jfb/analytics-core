from pathlib import Path
import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from include.airflow.operators.snowflake import SnowflakeAlertOperator
from include.utils import blueyonder
from include.airflow.operators.blueyonder import BlueYonderExportFeed
from include.airflow.operators.sftp import SFTPPutTouchFileOperator
from include.config import email_lists, owners, conn_ids
from include.airflow.callbacks.slack import slack_failure_edm
from include import SQL_DIR

default_args = {
    'start_date': pendulum.datetime(2024, 8, 1, 7, tz='America/Los_Angeles'),
    'retries': 2,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id='edm_outbound_sftp_blue_yonder_data_feed_daily_qa',
    default_args=default_args,
    schedule='45 6 * * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
)


with dag:

    # will need to enable back in January, these tasks just export without running the procedure
    # so these have to run after the Prod tasks
    trigger_test = SFTPPutTouchFileOperator(
        task_id="trigger_file_drop_completed_test",
        ssh_conn_id=conn_ids.SFTP.sftp_blue_yonder_test,
        local_filepath="pdcinbound_trigger.done",
        remote_filepath="/regular/pdcinbound_trigger.done",
    )
    tasks_test = {}

    interfaces_list = [
        'transportload',
        'schedrcpts',
        'inventory',
        'inventory_transaction',
        'network',
        'dfu',
        'history',
        'worklist',
        'sku',
        'calendar',
        'loc',
        'item',
        'itemhierarchy',
        'lochierarchy',
    ]
    for i in interfaces_list:
        cfg = blueyonder.config_list[i]

        # tasks to Push to QA
        tasks_test[i] = BlueYonderExportFeed(
            task_id=f"{cfg.name}_export_data_feed_test",
            sftp_conn_id=conn_ids.SFTP.sftp_blue_yonder_test,
            file_name=cfg.file_name,
            remote_filepath='/regular',
            table_name=f'export_by_{cfg.name}_interface',
            append=cfg.append,
            generate_empty_file=cfg.generate_empty_file,
            column_list=cfg.column_list,
            database="reporting_dev",
            sql_or_path=Path(
                SQL_DIR,
                'reporting_dev',
                'procedures',
                f'blue_yonder.export_by_{cfg.name}_interface_qa.sql',
            ),
        )
        tasks_test[i] >> trigger_test
    tasks_test['loc'] >> tasks_test['lochierarchy']
    tasks_test['item'] >> tasks_test['itemhierarchy']
