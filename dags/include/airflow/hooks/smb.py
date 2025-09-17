import socket

from airflow.hooks.base import BaseHook
from smb.SMBConnection import SMBConnection
from include.config import conn_ids


class SMBHook(BaseHook):
    def __init__(
        self,
        smb_conn_id=conn_ids.SMB.default,
    ):
        self.smb_conn_id = smb_conn_id
        self.conn = self.get_connection(self.smb_conn_id)
        self.username = self.conn.login
        self.password = self.conn.password
        self.server_name = self.conn.host
        self.extras = self.conn.extra_dejson
        self.server_ip_address = self.extras.get("server_ip")
        self.client_machine_name = socket.gethostname()
        self.domain = self.extras.get("domain")

    def get_conn(self):
        conn = SMBConnection(
            username=self.username,
            password=self.password,
            domain=self.domain,
            my_name=self.client_machine_name,
            remote_name=self.server_name,
        )
        conn.connect(ip=self.server_ip_address)
        return conn

    def upload(self, share_name, remote_path, local_path):
        with open(local_path, "rb") as f, self.get_conn() as cnx:
            cnx.storeFile(share_name, remote_path, f)
