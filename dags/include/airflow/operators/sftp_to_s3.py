# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import fnmatch
import gzip
import os
import shutil
from abc import abstractmethod
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import List
from zipfile import BadZipFile, ZipFile

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.hooks.ssh import SSHHook
from functools import cached_property
from include.config import conn_ids

from include.utils.data_structures import chunk_list


def gzip_compress_file(path_in: Path, path_out: Path):
    """
    Take file at ``path_in`` and gzip compress out to ``path_out``.
    """
    with open(path_in.as_posix(), "rb") as f_in:
        with gzip.open(path_out, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def move_file(path_in: Path, path_out: Path):
    """
    Take file at ``path_in`` and gzip compress out to ``path_out``.
    """
    os.rename(path_in.as_posix(), path_out.as_posix())


def is_gzipped(filename):
    return Path(filename).as_posix().endswith("gz")


class SFTPToS3Operator(BaseOperator):
    """
    This operator enables the transferring of files from a SFTP server to Amazon S3.

    :param remote_path: The sftp remote path. This is the specified file path for downloading the
        file from the SFTP server.
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where the file is uploaded.
    :param s3_key: The targeted s3 key. This is the specified path for uploading the file to S3.
    :param sftp_conn_id: The sftp connection id. The name or identifier for establishing a
        connection to the SFTP server.
    :param s3_conn_id: The s3 connection id. The name or identifier for establishing a connection
        to S3
    :param compression: If 'gzip', will gzip compress files not ending in '.gz'
    """

    template_fields = ("s3_key", "remote_path")

    def __init__(
        self,
        remote_path,
        s3_bucket,
        s3_key,
        sftp_conn_id=conn_ids.SFTP.ssh_default,
        s3_conn_id=conn_ids.AWS.tfg_default,
        compression=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.remote_path = remote_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
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

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)
        ssh_client = ssh_hook.get_conn()

        with TemporaryDirectory() as td:
            filename = Path(td) / "tmpfile"
            with self.open_func(
                filename, "w"
            ) as f, ssh_client.open_sftp() as sftp_client:
                sftp_client.getfo(self.remote_path, f)

            s3_hook.load_file(
                filename=filename.as_posix(),
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True,
            )


class BaseSFTPToS3BatchOperator(BaseOperator):
    """
    Base class to copy all files in in ``remote_dir`` matching ``file_pattern`` to s3 prefix
    ``s3://s3_bucket/s3_prefix``

    Will open a new sftp connection every ``files_per_batch`` files.

    Subclasses are created to handle remote files of different types, e.g. gzip, uncompressed, zip.

    :param remote_dir: The sftp remote path. This is the specified file path for downloading the
        file from the SFTP server.
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where the file is uploaded.
    :param s3_prefix: The targeted s3 prefix. This is the specified folder for uploading the file
        to S3.
    :param sftp_conn_id: The sftp connection id. The name or identifier for establishing a
        connection to the SFTP server.
    :param s3_conn_id: The s3 connection id. The name or identifier for establishing a connection
        to S3
    :param files_per_batch: files per batch for keeping the connection live for short time and to
        avoid timeout
    :param file_pattern: pull files from remote_dir matching the pattern specified
    :param remove_remote_files: remove file from SFTP remote location after copied to s3
    """

    template_fields = ["s3_prefix"]

    def __init__(
        self,
        remote_dir,
        s3_bucket,
        s3_prefix,
        file_pattern=None,
        sftp_conn_id=conn_ids.SFTP.ssh_default,
        s3_conn_id=conn_ids.AWS.tfg_default,
        files_per_batch=200,
        remove_remote_files: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.remote_dir = remote_dir.rstrip("/")
        self.file_pattern = file_pattern
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.strip("/")
        self.s3_conn_id = s3_conn_id
        self.files_per_batch = files_per_batch
        self.remove_remote_files = remove_remote_files

    @cached_property
    def ssh_client(self):
        ssh_hook = SSHHook(self.sftp_conn_id)
        return ssh_hook.get_conn()

    @cached_property
    def s3_hook(self):
        return S3Hook(self.s3_conn_id)

    @staticmethod
    def filter_files(file_list, pattern):
        return fnmatch.filter(names=file_list, pat=pattern)

    @abstractmethod
    def get_remote_file_to_local(self, local_dir: Path, remote_path: Path, sftp_client):
        """
        Take file at ``remote_path`` and write to ``local_dir``.
        """
        pass

    @cached_property
    def file_list(self) -> List[str]:
        """
        The list of all files in ``self.remote_dir``, subject to ``self.file_pattern``.
        """
        with self.ssh_client.open_sftp() as sftp_client:
            file_list = sftp_client.listdir(self.remote_dir)
            if self.file_pattern:
                file_list = self.filter_files(
                    file_list=file_list, pattern=self.file_pattern
                )
        return file_list  # type: ignore

    def execute(self, context=None):
        for current_list in chunk_list(self.file_list, self.files_per_batch):
            with TemporaryDirectory() as td:
                temp_dir = Path(td)
                with self.ssh_client.open_sftp() as sftp_client:
                    for remote_filename in current_list:
                        remote_path = Path(self.remote_dir, remote_filename)
                        print(f"pull from sftp: {remote_path}")
                        self.get_remote_file_to_local(
                            local_dir=temp_dir,
                            remote_path=remote_path,
                            sftp_client=sftp_client,
                        )
                for local_file in temp_dir.iterdir():
                    s3_key = f"{self.s3_prefix}/{local_file.name}"
                    print(f"push to s3: s3://{self.s3_bucket}/{s3_key}")
                    self.s3_hook.load_file(
                        filename=local_file.as_posix(),
                        key=s3_key,
                        bucket_name=self.s3_bucket,
                        replace=True,
                    )
            if self.remove_remote_files:
                with self.ssh_client.open_sftp() as sftp_client:
                    for remote_filename in current_list:
                        remote_path = f"{self.remote_dir}/{remote_filename}"
                        print(f"remove from sftp: {remote_path}")
                        sftp_client.remove(remote_path)


class SFTPToS3BatchOperator(BaseSFTPToS3BatchOperator):
    """
    Use when remote files are uncompressed.  Will gzip them.
    """

    def get_remote_file_to_local(self, local_dir: Path, remote_path: Path, sftp_client):
        """
        Take uncompressed remote file, and write to ``local_path`` with gzip compression.
        """
        with gzip.open(Path(local_dir / f"{remote_path.name}.gz"), "wb") as f:
            sftp_client.getfo(remote_path.as_posix(), f)


class SFTPToS3BatchZipOperator(BaseSFTPToS3BatchOperator):
    """
    Use when remote files are zip archives.  Will extract archive and gzip each extracted file to ``local_dir``.
    """

    def get_remote_file_to_local(self, local_dir: Path, remote_path: Path, sftp_client):
        """
        Take remote zip file, extract, and if files are uncompressed, gzip them.
        """
        zip_file_base_name = remote_path.name.replace(".zip", "")
        with NamedTemporaryFile(
            mode="wb", delete=True
        ) as f, TemporaryDirectory() as zip_temp_dir:
            sftp_client.getfo(remote_path.as_posix(), f)
            f.flush()
            try:
                ZipFile(f.name).extractall(path=zip_temp_dir)
            except BadZipFile:
                self.log.warning(f"BadZipFile error on file {remote_path}. skipping.")
                return
            for file in Path(zip_temp_dir).iterdir():
                tgt_file_path = local_dir / f"{zip_file_base_name}_{file.name}"
                if tgt_file_path.as_posix().endswith(".gz"):
                    move_file(file, tgt_file_path)
                    print(f"moving to {local_dir / tgt_file_path}")
                else:
                    tgt_file_path = Path(tgt_file_path.as_posix() + ".gz")
                    gzip_compress_file(file, tgt_file_path)
                    print(f"compressing to {tgt_file_path}")
