import gzip
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.models import BaseOperator

from include.airflow.hooks.smb import SMBHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from include.config import conn_ids


class SMBToSFTPOperator(BaseOperator):
    """
    This operator enables the transferring of files from a SMB server to SFTP.

    Args:
        remote_path: The smb remote path. This is the specified file path for downloading the
            file from the SMB server.
        target_dir: the directory of sftp where file needs to be placed
        filename: file that needs to be placed in target_dir
        share_name: This is the specified share in the SMB Server.
        smb_conn_id: The smb connection id. The name or identifier for establishing a
            connection to the SMB server.
        sftp_conn_id: The sftp connection id. The name or identifier for establishing a
        connection to the SFTP server.
        compression: If 'gzip', will gzip compress files not ending in '.gz'
    """

    template_fields = ("remote_path", "filename")

    def __init__(
        self,
        remote_path,
        target_dir,
        filename,
        share_name,
        smb_conn_id=conn_ids.SMB.default,
        sftp_conn_id=conn_ids.SFTP.default,
        compression=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.smb_conn_id = smb_conn_id
        self.remote_path = remote_path
        self.target_dir = target_dir
        self.filename = filename
        self.share_name = share_name
        self.sftp_conn_id = sftp_conn_id
        self.compression = compression
        if self.compression not in (None, "gzip"):
            raise ValueError("compression may only be None or 'gzip'")
        if self.compression and remote_path.endswith(".gz"):
            raise ValueError("file already compressed")

    @property
    def open_func(self):
        if self.compression == "gzip":
            return gzip.open
        else:
            return open

    def upload_to_sftp(self, local_path):
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)

        with sftp_hook.get_conn() as sftp_client:
            sftp_client.put(
                localpath=local_path,
                remotepath=Path(self.target_dir, self.filename).as_posix(),
            )

    def execute(self, context=None):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        smb_client = smb_hook.get_conn()

        with TemporaryDirectory() as td:
            file_to_upload = Path(td) / self.filename
            with self.open_func(file_to_upload, "wb") as f, smb_client as smb_client:
                smb_client.retrieveFile(
                    service_name=self.share_name, path=self.remote_path, file_obj=f
                )
            self.upload_to_sftp(local_path=file_to_upload)
