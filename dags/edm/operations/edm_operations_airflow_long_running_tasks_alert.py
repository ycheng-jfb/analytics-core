from typing import List, Type

import pendulum
import sqlalchemy
from airflow.models import DAG, BaseOperator, TaskInstance
from airflow.models.base import Base
from airflow.utils.session import create_session
from airflow.utils.email import send_email
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.callbacks.slack import send_slack_message
from include.config import email_lists, owners
from include.utils.snowflake import Column

default_args = {
    "start_date": pendulum.datetime(2023, 10, 1, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_operations_airflow_long_running_tasks_alert",
    default_args=default_args,
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1,
)


class MWAALongRunningDags(BaseOperator):
    """
    Operator to extract long running tasks from airflow metadata db.

    Attributes:
        table: sqlalchemy object of table to extract
        date_col: column to use for date comparison
        state: state of the dag
        column_list: list of columns to extract from table
        max_minutes: minutes after which we need to alert on long running tasks
    """

    def __init__(
        self,
        table: Type[Base],
        date_col: sqlalchemy.Column,
        state: sqlalchemy.Column,
        column_list: List[Column],
        max_minutes: int,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table = table
        self.date_col = date_col
        self.state = state
        self.column_list = column_list
        self.max_minutes = max_minutes

    @property
    def column_names(self) -> List[str]:
        return [i.name for i in self.column_list]

    @property
    def column_entities(self) -> list:
        return [getattr(self.table, c.name) for c in self.column_list]

    @staticmethod
    def send_mail_alert(max_minutes, long_running_tasks, email_to_list, email_cc_list):
        # Generate HTML content for email
        rows_html = "".join(
            f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{(pendulum.now('UTC') - row[3]).in_words()}</td></tr>"
            for row in long_running_tasks
        )
        html_content = f"""
        <html>
            <body>
                <p>Alert: The following tasks have been running for more than {max_minutes} minutes:</p>
                <table border="1">
                    <tr>
                        <th>Task Name</th>
                        <th>Dag Name</th>
                        <th>Runtime</th>
                    </tr>
                    {rows_html}
                </table>
            </body>
        </html>
        """
        send_email(
            to=email_to_list,
            cc=email_cc_list,
            subject="Alert: Long Running Tasks",
            html_content=html_content,
        )

    def send_slack_alert(self, max_minutes, long_running_tasks):
        slack_message = self.format_slack_message(max_minutes, long_running_tasks)

        # sends the slack notification to data_integrations channel
        send_slack_message(message=slack_message, conn_id='slack_data_integrations')

    @staticmethod
    def format_slack_message(max_minutes, long_running_tasks):
        message = (
            f"*Alert:* The following tasks have been running for more than {max_minutes} minutes:\n"
        )
        for row in long_running_tasks:
            task_name = row[0]
            dag_name = row[1]
            runtime = (pendulum.now("UTC") - row[3]).in_words()
            message += f"• *Task:* `{task_name}` | *DAG:* `{dag_name}` | *Runtime:* {runtime}\n"
        return message

    def execute(self, context=None) -> None:
        with create_session() as session:
            threshold_time = pendulum.now("UTC").subtract(minutes=self.max_minutes)
            query = session.query(*self.column_entities).filter(
                self.state == 'running', self.table.start_date <= threshold_time
            )
            all_rows = query.all()
            if len(all_rows) > 0:
                self.send_mail_alert(
                    max_minutes=self.max_minutes,
                    long_running_tasks=all_rows,
                    email_to_list=email_lists.data_integration_support,
                    email_cc_list=[],
                )
                self.send_slack_alert(max_minutes=self.max_minutes, long_running_tasks=all_rows)
            else:
                print('currently no tasks are long running')


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

    list_dags = MWAALongRunningDags(
        task_id="list_tasks",
        table=TaskInstance,
        date_col=TaskInstance.execution_date,
        state=TaskInstance.state,
        column_list=column_list,
        max_minutes=180,
    )
