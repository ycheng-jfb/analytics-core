from airflow.models import BaseOperator, DagBag
from airflow.utils.email import send_email
from functools import cached_property

from include import DAGS_DIR
from include.airflow.utils.db import CLUSTER_BASE_URL
from include.config import conn_ids
from include.airflow.callbacks.slack import send_slack_message  # Import Slack message sender


class DisabledDagsAlertOperator(BaseOperator):
    def __init__(self, dag_ids, slack_conn_id=conn_ids.SlackAlert.media, **kwargs):
        self.dag_ids = dag_ids
        self.slack_conn_id = slack_conn_id
        super().__init__(**kwargs)

    @cached_property
    def disabled_dags(self):
        dagbag = DagBag(DAGS_DIR.as_posix())
        disabled_dags = []

        for dag_id, dag in dagbag.dags.items():
            if dag.is_paused and dag_id in self.dag_ids:
                disabled_dags.append(dag_id)
        return disabled_dags

    @staticmethod
    def make_link(dag_id):
        url = f"https://{CLUSTER_BASE_URL}/tree?dag_id={dag_id}"
        return f'<a href="{url}">{dag_id}</a>'

    @property
    def alert_message(self):
        body = 'The following critical DAGs are disabled in prod.\n'
        body += '\n'.join([self.make_link(dag_id) for dag_id in self.disabled_dags])
        return body.replace('\n', '\n<br>')

    @property
    def slack_message(self):
        message = "*🚨 Airflow Alert: Disabled DAGs 🚨*\n"
        message += "The following critical DAGs are currently disabled in production:\n"
        for dag_id in self.disabled_dags:
            dag_link = self.make_link(dag_id)
            message += f"• `{dag_id}` - {dag_link}\n"
        return message

    def dry_run(self):
        if self.disabled_dags:
            print(self.alert_message)

    def execute(self, context):
        if self.disabled_dags:
            send_email(
                to=self.email,
                subject='🚨 Airflow Alert: Disabled DAGs',
                html_content=self.alert_message,
            )
            send_slack_message(message=self.slack_message, conn_id=self.slack_conn_id)
