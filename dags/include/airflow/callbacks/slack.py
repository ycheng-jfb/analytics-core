import os
from urllib.parse import urlparse

from include.airflow.hooks.slack import SlackHook
from include.config import conn_ids

from airflow import DAG, configuration

IS_PRODUCTION = 'production' in os.environ.get('ENVIRONMENT_STAGE', '').lower()


class SlackFailureCallback:
    def __init__(
        self,
        conn_id=conn_ids.SlackAlert.slack_default,
        channel_name='airflow-alerts-edm',
        is_public=True,
    ):
        self._conn_id = conn_id
        self.channel_name = channel_name
        self.is_public = is_public

    @property
    def conn_id(self):
        return self._conn_id if IS_PRODUCTION else conn_ids.SlackAlert.testing

    def __call__(self, context):
        slack_task_failure(
            context=context,
            conn_id=self.conn_id,
            channel_name=self.channel_name,
            is_public=self.is_public,
        )


slack_failure_edm = SlackFailureCallback(conn_ids.SlackAlert.edm)
slack_failure_p1 = SlackFailureCallback(conn_ids.SlackAlert.edm_p1, 'airflow-edm-p1')
slack_failure_p1_edw = SlackFailureCallback(conn_ids.SlackAlert.edm_p1_edw, 'airflow-edw-p1', False)
slack_failure_media = SlackFailureCallback(conn_ids.SlackAlert.media, 'airflow-alerts-media')
slack_failure_media_p1 = SlackFailureCallback(
    conn_ids.SlackAlert.media_p1, 'airflow-media-p1', False
)
slack_failure_gsc = SlackFailureCallback(conn_ids.SlackAlert.gsc, 'airflow-alerts-gsc')
slack_failure_testing = SlackFailureCallback(conn_ids.SlackAlert.testing)
slack_failure_data_science = SlackFailureCallback(
    conn_ids.SlackAlert.data_science, 'airflow-alerts-data-science'
)
slack_failure_sxf_data_team = SlackFailureCallback(
    'slack_alert_sxf_data_team', 'sxf-data-team-intake-alerts', False
)  # Hardcoded conn_id value to handle AttributeError: sxf_data_team


class SlackSlaMissCallback:
    def __init__(self, conn_id=conn_ids.SlackAlert.slack_default):
        self._conn_id = conn_id

    @property
    def conn_id(self):
        return self._conn_id if IS_PRODUCTION else conn_ids.SlackAlert.testing

    def __call__(self, dag, task_list, blocking_task_list, slas, blocking_tis):
        # Not all of these params are used, but these params must be defined for sla_miss_callback
        sla_miss_alert(dag, task_list, self.conn_id)


slack_sla_miss_edm_p1 = SlackSlaMissCallback(conn_ids.SlackAlert.edm_p1)
slack_sla_miss_edw_p1 = SlackSlaMissCallback(conn_ids.SlackAlert.edm_p1_edw)
slack_sla_miss_edm = SlackSlaMissCallback(conn_ids.SlackAlert.edm)
slack_sla_miss_media = SlackSlaMissCallback(conn_ids.SlackAlert.media)
slack_sla_miss_media_p1 = SlackSlaMissCallback(conn_ids.SlackAlert.media_p1)
slack_sla_miss_gsc = SlackSlaMissCallback(conn_ids.SlackAlert.gsc)
slack_sla_miss_testing = SlackSlaMissCallback(conn_ids.SlackAlert.testing)
slack_sla_miss_data_science = SlackSlaMissCallback(conn_ids.SlackAlert.data_science)
slack_sla_miss_sxf_data_team = SlackSlaMissCallback('slack_alert_sxf_data_team')
# Hardcoded conn_id value to handle AttributeError: sxf_data_team


class SlackTaskSuccessCallback:
    def __init__(
        self, conn_id=conn_ids.SlackAlert.slack_default, channel_name='airflow-alerts-edm'
    ):
        self._conn_id = conn_id
        self.channel_name = channel_name

    @property
    def conn_id(self):
        return self._conn_id

    def __call__(self, context):
        # Not all of these params are used, but these params must be defined for sla_miss_callback
        slack_task_success(context=context, conn_id=self.conn_id, channel_name=self.channel_name)


def get_custom_log_url(log_url: str) -> str:
    parsed = urlparse(log_url)
    replaced = parsed._replace(
        netloc=(
            "tfg-da-prod-airflow.techstyle.net"
            if IS_PRODUCTION
            else "da-int-airflow.techstyle-apps.duplocloud.net"
        )
    )
    return replaced.geturl()


def slack_task_success(context, conn_id, channel_name):
    dag_id = context.get('task_instance').dag_id
    data_interval_start = context.get('data_interval_start')
    message = f""":no_entry: Dag *{context.get('task_instance').dag_id}* completed successfully. <!here>
*DAG*: {dag_id}
*Execution date*: `{data_interval_start}`
<{get_custom_log_url(context.get('task_instance').log_url)}|Log url>"""
    send_slack_alert(message, conn_id, channel_name, dag_id, data_interval_start)


def sla_miss_alert(dag: DAG, task_list: str, conn_id):
    base_url = configuration.get('webserver', 'BASE_URL')
    dag_url = f"<{get_custom_log_url(base_url)}/tree?dag_id={dag.dag_id}|{dag.dag_id}>"
    if task_list.count('\n') >= 5:
        task_list = '\n'.join(task_list.splitlines()[:5]) + '\n...'
    message = f""":exclamation: DAG *{dag_url}* has missed its SLA. <!here>
*Running Task List*: {task_list}
    """
    send_slack_message(message=message, conn_id=conn_id)


def send_slack_message(message, conn_id):
    hook = SlackHook(slack_conn_id=conn_id)
    hook.send_message(message=message)


def send_slack_alert(message, conn_id, channel_name, dag_id, data_interval_start, is_public=True):
    hook = SlackHook(slack_conn_id=conn_id)
    if is_public:
        hook.send_alert(message, channel_name, dag_id, data_interval_start)
    else:
        hook.send_message(message=message)


def slack_task_failure(context, conn_id, channel_name, is_public=True):
    dag_id = context.get('task_instance').dag_id
    data_interval_start = context.get('data_interval_start')
    message = f""":no_entry: Task *{context.get('task_instance').task_id}* failed. <!here>
*DAG*: {dag_id}
*Execution date*: `{data_interval_start}`
<{get_custom_log_url(context.get('task_instance').log_url)}|Log url>"""
    send_slack_alert(message, conn_id, channel_name, dag_id, data_interval_start, is_public)


def slack_task_retry(context, conn_id):
    message = f"""
:small_orange_diamond: Task *{context.get('task_instance').task_id}* failed and will be retried later.
*DAG*: {context.get('task_instance').dag_id}
*Execution date*: `{context.get('data_interval_start')}`
*Next retry*: `{context.get('task_instance').next_retry_datetime()}`
<{get_custom_log_url(context.get('task_instance').log_url)}|Log url>"""
    send_slack_message(message=message, conn_id=conn_id)


def slack_task_warning(context, message, conn_id):
    message = f"""
:small_orange_diamond: Task *{context.get('task_instance').task_id}* warning
*Message*: {message}
*DAG*: {context.get('task_instance').dag_id}
*Execution date*: `{context.get('data_interval_start')}`
<{get_custom_log_url(context.get('task_instance').log_url)}|Log url>"""
    send_slack_message(message=message, conn_id=conn_id)


def test_slack_task_failure():
    class ti:
        dag_id = "some_dag"
        task_id = "fake_task"
        log_url = "s3://abc/123/abc.txt"

    slack_task_failure(context={"task_instance": ti()}, conn_id='slack_dev')


def test_slack_task_failure_cb():
    class ti:
        dag_id = "some_dag"
        task_id = "fake_task"
        log_url = "s3://abc/123/abc.txt"

    slack_failure_data_science({"task_instance": ti()})


def test_slack_task_retry():
    class ti:
        dag_id = "some_dag"
        task_id = "fake_task"
        log_url = "s3://abc/123/abc.txt"

        def next_retry_datetime(self):
            return "2019-01-01"

    slack_task_retry({"task_instance": ti()})
