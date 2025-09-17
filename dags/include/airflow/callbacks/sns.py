import json
from functools import partial
from include.config import conn_ids

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def publish_to_target(target_arn, message, aws_conn_id=conn_ids.AWS.tfg_default):
    """
    Push to target

    .. code-block:: python

        publish_to_target(
            target_arn='arn:aws:sns:us-west-2:195662458535:edm-airflow',
            message='This is a test.',
        )

    """

    hook = AwsBaseHook(aws_conn_id=aws_conn_id)

    client = hook.get_client_type("sns")

    messages = {"default": message}

    return client.publish(
        TargetArn=target_arn, Message=json.dumps(messages), MessageStructure="json"
    )


def task_fail_callback_factory(target_arn, aws_conn_id=conn_ids.AWS.tfg_default):
    def task_fail_callback(target_arn, aws_conn_id, context):
        message = f""":no_entry: *Task Failed*
*dag_id*: {context.get('task_instance').dag_id}
*task_id*: {context.get('task_instance').task_id}
*Execution Time*: `{context.get('data_interval_start')}`
<{context.get('task_instance').log_url}|Log url>"""
        publish_to_target(target_arn=target_arn, aws_conn_id=aws_conn_id, message=message)

    return partial(task_fail_callback, target_arn=target_arn, aws_conn_id=aws_conn_id)
