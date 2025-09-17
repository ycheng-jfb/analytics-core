import fnmatch
import tempfile
from pathlib import Path

from airflow.models import BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperator


class SFTPPutTouchFileOperator(SFTPOperator):
    def __init__(self, *args, **kwargs):
        super(SFTPPutTouchFileOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        with tempfile.TemporaryDirectory() as td:
            local_path = Path(f"{Path(td)}/{self.local_filepath}")
            local_path.write_text("")
            self.local_filepath = str(local_path)
            super(SFTPPutTouchFileOperator, self).execute(context)


class SFTPTransferOperator(BaseOperator):
    template_fields = ["file_mask", "source_dir", "target_dir"]

    def __init__(
        self,
        source_sftp_conn_id: str,
        target_sftp_conn_id: str,
        file_mask: str = None,
        fail_on_no_files: bool = False,
        source_host: str = None,
        target_host: str = None,
        source_dir: str = None,
        target_dir: str = None,
        replace: bool = False,
        remove: bool = False,
        *args,
        **kwargs,
    ):
        self.source_sftp_conn_id = source_sftp_conn_id
        self.target_sftp_conn_id = target_sftp_conn_id
        self.file_mask = file_mask
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.source_host = source_host
        self.target_host = target_host
        self.fail_on_no_files = fail_on_no_files
        self.replace = replace
        self.remove = remove
        super().__init__(*args, **kwargs)

    def copy_files(self, dry_run=False):
        source_hook = SFTPHook(
            ftp_conn_id=self.source_sftp_conn_id, remote_host=self.source_host
        )
        source_client = source_hook.get_conn()
        if self.source_dir:
            source_client.chdir(self.source_dir)
        target_hook = SFTPHook(
            ftp_conn_id=self.target_sftp_conn_id, remote_host=self.target_host
        )
        target_client = target_hook.get_conn()
        if self.target_dir:
            target_client.chdir(self.target_dir)

        files_in_dir = source_client.listdir()
        if self.file_mask:
            files_to_transfer = fnmatch.filter(files_in_dir, self.file_mask)
        else:
            files_to_transfer = files_in_dir

        print(
            f"copying files from {source_hook.remote_host} dir {source_client.getcwd()} "
            f"to {target_hook.remote_host} dir {target_client.getcwd()}"
        )

        if self.fail_on_no_files and not files_to_transfer:
            raise ValueError("No files found")

        for filename in files_to_transfer:
            source_path = (
                self.source_dir
                and Path(self.source_dir, filename).as_posix()
                or filename
            )
            target_path = (
                self.target_dir
                and Path(self.target_dir, filename).as_posix()
                or filename
            )
            print(f"copy file {source_path} to {target_path}")
            if dry_run:
                continue
            elif not self.replace and target_client.exists(target_path):
                print(f"file {target_path} exists; skipping.")
                continue
            else:
                with source_client.open(filename) as f:
                    f.prefetch()
                    target_client.putfo(fl=f, remotepath=filename)
                if self.remove:
                    source_client.remove(filename)

    def execute(self, context):
        self.copy_files(dry_run=False)

    def dry_run(self):
        self.copy_files(dry_run=True)
