import fnmatch
import gzip
from pathlib import Path
from tempfile import TemporaryDirectory

import pendulum
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.airflow.hooks.smb import SMBHook
from include.config import conn_ids


class SMBToS3Operator(BaseOperator):
    """
    This operator enables the transferring of files from a SMB server to Amazon S3.

    Args:
        remote_path: The smb remote path. This is the specified file path for downloading the
            file from the SMB server.
        s3_bucket: The targeted s3 bucket. This is the S3 bucket to where the file is uploaded.
        s3_key: The targeted s3 key. This is the specified path for uploading the file to S3.
        share_name: This is the specified share in the SMB Server.
        smb_conn_id: The smb connection id. The name or identifier for establishing a
            connection to the SMB server.
        s3_conn_id: The s3 connection id. The name or identifier for establishing a connection
            to S3
        compression: If 'gzip', will gzip compress files not ending in '.gz'
    """

    template_fields = ("s3_key", "remote_path")

    def __init__(
        self,
        remote_path,
        s3_bucket,
        s3_key,
        share_name,
        smb_conn_id=conn_ids.SMB.default,
        s3_conn_id=conn_ids.AWS.tfg_default,
        compression=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.smb_conn_id = smb_conn_id
        self.remote_path = remote_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.share_name = share_name
        self.s3_conn_id = s3_conn_id
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

    def execute(self, context=None):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)
        smb_client = smb_hook.get_conn()

        with TemporaryDirectory() as td:
            filename = Path(td) / "tmpfile"
            with self.open_func(filename, "wb") as f, smb_client as smb_client:
                smb_client.retrieveFile(
                    service_name=self.share_name, path=self.remote_path, file_obj=f
                )

            s3_hook.load_file(
                filename=filename.as_posix(),
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True,
            )


class SMBToS3BatchOperator(SMBToS3Operator):
    """
    This operator enables the transferring multiple files from a SMB server to Amazon S3.

    Args:
        remote_dir: The smb remote directory. This is the specified file directory for downloading the
            files from the SMB server.
        s3_prefix: The targeted s3 prefix. This is the specified folder for uploading the file
            to S3.
        file_pattern_list: The list of file_patterns that need to be retrieved.
        archive_remote_files: Flag set to True if files need to be archived after processing.
        archive_folder: The folder to which archive files are moved.
    """

    def __init__(
        self,
        remote_dir,
        s3_prefix,
        file_pattern_list,
        archive_remote_files,
        archive_folder,
        s3_replace=True,
        **kwargs,
    ):
        super().__init__(s3_key=None, remote_path="", **kwargs)
        self.remote_dir = remote_dir
        self.s3_prefix = s3_prefix
        self.file_pattern_list = file_pattern_list
        self.archive_remote_files = archive_remote_files
        self.archive_folder = archive_folder
        self.s3_replace = s3_replace

    def get_file_list(self, smb_client):
        file_list = [
            f.filename for f in smb_client.listPath(self.share_name, self.remote_dir)
        ]

        remote_file_list = []
        for file_pattern in self.file_pattern_list:
            remote_file_list.extend(fnmatch.filter(names=file_list, pat=file_pattern))
        return remote_file_list

    def load_file(self, s3_hook, smb_client, remote_file):
        with TemporaryDirectory() as td:
            filename = Path(td) / "tmpfile"
            file_path = Path(self.remote_dir) / remote_file
            with self.open_func(filename, "wb") as f:
                smb_client.retrieveFile(
                    service_name=self.share_name, path=file_path.as_posix(), file_obj=f
                )
            if self.s3_replace:
                s3_key = f"{self.s3_prefix}/{remote_file}.gz"
            else:
                utc_time = (
                    pendulum.DateTime.utcnow()
                    .isoformat()[0:-6]
                    .replace("-", "")
                    .replace(":", "")
                )
                s3_file_name = (
                    f"{Path(remote_file).stem}_{utc_time}{Path(remote_file).suffix}.gz"
                )
                s3_key = f"{self.s3_prefix}/{s3_file_name}"

            s3_hook.load_file(
                filename=filename.as_posix(),
                key=s3_key,
                bucket_name=self.s3_bucket,
                replace=self.s3_replace,
            )

    def archive_file(self, smb_client, remote_filename):
        utc_time = (
            pendulum.DateTime.utcnow()
            .isoformat()[0:-6]
            .replace("-", "")
            .replace(":", "")
        )
        archive_filename = (
            f"{Path(remote_filename).stem}_{utc_time}{Path(remote_filename).suffix}"
        )
        smb_client.rename(
            service_name=self.share_name,
            old_path=Path(self.remote_dir, remote_filename).as_posix(),
            new_path=Path(
                self.remote_dir, self.archive_folder, archive_filename
            ).as_posix(),
        )

    def execute(self, context=None):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        with smb_hook.get_conn() as smb_client:
            remote_files = self.get_file_list(smb_client)

            for remote_file in remote_files:
                self.load_file(
                    s3_hook=s3_hook, smb_client=smb_client, remote_file=remote_file
                )

            if self.archive_remote_files:
                for remote_filename in remote_files:
                    self.archive_file(
                        smb_client=smb_client, remote_filename=remote_filename
                    )
