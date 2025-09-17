import logging
import tempfile

from airflow.hooks.base import BaseHook
from include.config import conn_ids
from O365 import Account, FileSystemTokenBackend


class AzureHook(BaseHook):
    def __init__(
        self,
        azure_conn_id=conn_ids.Azure.azure_default,
    ):
        self.azure_conn_id = azure_conn_id

    def azure_get_conn(self):
        conn_obj = self.get_connection(self.azure_conn_id)
        client_id = conn_obj.extra_dejson["client_id"]
        secret_id = conn_obj.extra_dejson["secret_id"]
        tenant_id = conn_obj.extra_dejson["tenant_id"]
        credentials = (client_id, secret_id)
        with tempfile.TemporaryDirectory() as td:
            token_backend = FileSystemTokenBackend(
                token_path=td, token_filename="tmp_token.txt"
            )
            account = Account(
                credentials,
                auth_flow_type="credentials",
                tenant_id=tenant_id,
                token_backend=token_backend,
            )
            if account.authenticate():
                client = account.connection
                return client
            else:
                logging.info("Access issues when connecting to Azure")
                return None
