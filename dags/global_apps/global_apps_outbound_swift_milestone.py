import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.swift_milestone import SwiftMilestoneToSMBOperator
from include.config import conn_ids, email_lists, owners

default_args = {
    'start_date': pendulum.datetime(2020, 10, 13, tz="America/Los_Angeles"),
    'retries': 3,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_outbound_swift_milestone",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    doc_md=(
        'This process pulls swift milestone data to SMB location from email attachments.<br>'
        'Downstream system process SSIS package owned by DB-OPS load the data.<br>'
        'On the Evolve production Primary server, the SQL Agent job "Swift Milestone Processor v2"'
        'runs on an hourly schedule from 6:15am to 11:20pm Sun - Sat.<br>'
        'The job has one step that kicks off an SSIS package on the server, located and named '
        r'"\SSISDB\Swift Milestone Processor\SwiftMilestoneImport\SwiftMilestoneImport.dtsx".<br>'
        '<br><strong>Business POC</strong>: Joseph Storey'
    ),
)

with dag:
    swift_milestone_to_smb = SwiftMilestoneToSMBOperator(
        task_id='swift_milestone_to_smb',
        remote_path='Supply Chain/Ops/Swift Transload Upload',
        share_name='JustFab',
        smb_conn_id=conn_ids.SMB.app01,
        from_address='christine_kloos@swifttrans.com',
        subjects=["File For Bill To", "Swift / Just Fab tracking report"],
        resource_address='tfg_swiftmilestones@techstyle.com',
        file_extensions=['csv'],
    )
