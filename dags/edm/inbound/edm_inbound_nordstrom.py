import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.hooks.gpg import GPGHook
from include.config import email_lists, owners, conn_ids
import fnmatch
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from functools import cached_property

from include.airflow.hooks.msgraph import MsGraphSharePointHook

default_args = {
    "start_date": pendulum.datetime(2024, 8, 15, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}


class SFTPtoSharePointNordstromOperator(BaseOperator):
    """
    This operator enables the transferring of files from a SFTP server to Sharepoint with decryption

    :param remote_dir: The sftp remote path. This is the specified file path for downloading the
        file from the SFTP server.
    :param sftp_conn_id: The sftp connection id. The name or identifier for establishing a
        connection to the SFTP server.
    :param sharepoint_nordstrom_conn_id: The SharePoint connection id for uploading files to
        sharepoint.
    :param gpg_conn_id: conn_id for private key to use it for decryption.

    """

    def __init__(
        self,
        remote_dir,
        drive_id: str,
        folder_name: str,
        site_id: str,
        file_pattern=None,
        sftp_conn_id=conn_ids.SFTP.ssh_default,
        sharepoint_nordstrom_conn_id=conn_ids.Sharepoint.default,
        gpg_conn_id=conn_ids.GPG.default,
        files_per_batch=200,
        remove_remote_files: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.remote_dir = remote_dir.rstrip('/')
        self.file_pattern = file_pattern
        self.files_per_batch = files_per_batch
        self.sharepoint_nordstrom_conn_id = sharepoint_nordstrom_conn_id
        self.gpg_conn_id = gpg_conn_id
        self.remove_remote_files = remove_remote_files
        self.drive_id = drive_id
        self.site_id = site_id
        self.folder_name = folder_name

    @cached_property
    def ssh_client(self):
        ssh_hook = SSHHook(self.sftp_conn_id)
        return ssh_hook.get_conn()

    @staticmethod
    def filter_files(file_list, pattern):
        return fnmatch.filter(names=file_list, pat=pattern)

    @cached_property
    def file_list(self) -> List[str]:
        """
        The list of all files in ``self.remote_dir``, subject to ``self.file_pattern``.
        """
        with self.ssh_client.open_sftp() as sftp_client:
            file_list = sftp_client.listdir(self.remote_dir)
            if self.file_pattern:
                file_list = self.filter_files(file_list=file_list, pattern=self.file_pattern)
        return file_list  # type: ignore

    def execute(self, context):
        with TemporaryDirectory() as td:
            temp_dir = Path(td)
            with self.ssh_client.open_sftp() as sftp_client:
                for remote_filename in self.file_list:
                    remote_path = Path(self.remote_dir, remote_filename)
                    local_path = Path(temp_dir, remote_filename)
                    self.log.info(f"pull from sftp: {remote_path}")
                    with open(Path(f"{local_path}"), 'wb') as f:
                        sftp_client.getfo(remote_path.as_posix(), f)
                    with open(str(local_path), 'r') as f:
                        encrypted_content = f.read()
                    gpg_conn = GPGHook(gpg_conn_id=self.gpg_conn_id)
                    decrypted_content = str(gpg_conn.decrypt_file(encrypted_content))
                    with open(local_path, 'w') as csvfile:
                        csvfile.write(decrypted_content)
                    df = pd.read_csv(local_path, delimiter=';')
                    df.to_csv(local_path, index=False)
                    sharepoint_conn = MsGraphSharePointHook(
                        sharepoint_conn_id=self.sharepoint_nordstrom_conn_id
                    )
                    sharepoint_conn.upload(
                        site_id=self.site_id,
                        drive_id=self.drive_id,
                        folder_name=self.folder_name,
                        file_path=local_path,
                    )

            if self.remove_remote_files:
                with self.ssh_client.open_sftp() as sftp_client:
                    for remote_filename in self.file_list:
                        remote_path = Path(f"{self.remote_dir}/{remote_filename}")
                        self.log.info(f"remove from sftp: {remote_path}")
                        sftp_client.rename(
                            str(remote_path),
                            str(f"{remote_path.parent}/archive/{remote_path.name}"),
                        )


dag = DAG(
    dag_id="edm_inbound_nordstrom",
    default_args=default_args,
    schedule="30 10 * * *",
    catchup=False,
    max_active_runs=1,
)

with dag:
    for i in [
        'RECEIPTS',
        'ENHANCED_SALES',
        'INVENTORY_SOH',
    ]:
        to_sharepoint = SFTPtoSharePointNordstromOperator(
            task_id=f'to_sharepoint_{i}',
            remote_dir='Nordstrom',
            file_pattern=f'{i}_*',
            sftp_conn_id=conn_ids.SFTP.sftp_techstyle_nordstrom,
            drive_id='b!gvBm6sONFUeYb7dNKyqncwIMGuc7xGpLoECxQMjza2MrgUK4IVA7SbZLrIkJk3qM',
            site_id='ea66f082-8dc3-4715-986f-b74d2b2aa773',
            sharepoint_nordstrom_conn_id=conn_ids.Sharepoint.nordstrom,
            gpg_conn_id=conn_ids.GPG.nordstrom_private_key,
            folder_name='SavageX/Inbound (Non-PII)/Nordstrom Data Files',
            remove_remote_files=True,
        )
