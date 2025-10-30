import boto3
from airflow.hooks.base import BaseHook
from boto3.session import Session
from functools import cached_property
from include.config import conn_ids


class TatariHook(BaseHook):
    def __init__(self, slug, tatari_conn_id=conn_ids.Tatari.default):
        conn = self.get_connection(tatari_conn_id)
        self.aws_access_key_id = conn.login
        self.aws_secret_access_key = conn.password
        self.slug = slug

    @cached_property
    def session(self):
        client = boto3.client(
            'sts',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        response = client.assume_role(
            RoleArn=f'arn:aws:iam::878256633362:role/ClientRoles/DataShare/ClientRole-DataShare-'
            f'{self.slug}',
            RoleSessionName='my-session',
        )
        return Session(
            aws_access_key_id=response['Credentials']['AccessKeyId'],
            aws_secret_access_key=response['Credentials']['SecretAccessKey'],
            aws_session_token=response['Credentials']['SessionToken'],
        )
