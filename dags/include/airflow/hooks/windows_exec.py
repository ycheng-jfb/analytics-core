from contextlib import AbstractContextManager

from airflow.hooks.base import BaseHook
from pypsexec.client import Client


class ManagedClient(AbstractContextManager):
    """
    Context manager for opening and closing the client connection.

    """

    def __init__(self, client):
        self.client = client

    def __enter__(self):
        self.client.create_service()
        return self.client

    def __exit__(self, *exc_info):
        self.client.cleanup()
        self.client.disconnect()


class WindowsExecutableHook(BaseHook):
    """
    Hook for windows terminal  execution using pypsexec.
    This hook also lets you create service and serve as basis for executing remote commands.

    Args:
        remote_conn_id: connection id from airflow

    """

    def __init__(
        self,
        remote_conn_id,
    ):

        self.remote_conn_id = remote_conn_id
        self.conn = self.get_connection(self.remote_conn_id)
        self.username = self.conn.login
        self.password = self.conn.password
        self.servername = self.conn.host
        self.extras = self.conn.extra_dejson
        self.port = self.extras.get("host")
        self.domain = self.extras.get("domain")

    def get_conn(self):

        conn = Client(
            server=self.servername, username=self.username, password=self.password, port=445
        )
        conn.connect()
        return ManagedClient(conn)
