import os

from airflow.secrets.base_secrets import BaseSecretsBackend


class MacOSKeychainSecretsBackend(BaseSecretsBackend):
    def __init__(self, account="airflow", **kwargs):
        self.account = account
        super().__init__(**kwargs)

    def get_secret(self, account, key):
        cmd = " ".join(
            ["security", "find-generic-password", f"-a {account} -s {key} -w"]
        )
        with os.popen(cmd) as p:
            s = p.read()
        return s

    def get_conn_value(self, conn_id):
        val = self.get_secret(account=self.account, key=conn_id)
        return val

    def get_conn_uri(self, conn_id):
        """Deprecated in favor of get_conn_value"""
        return self.get_conn_value(conn_id=conn_id)
