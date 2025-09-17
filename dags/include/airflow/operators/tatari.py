import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from functools import cached_property

from include.airflow.hooks.tatari import TatariHook
from include.utils.s3 import get_filename
from include.config import conn_ids


class TatariToS3Operator(BaseOperator):
    """
    Base Operator for copying Tatari files to S3.
    """

    template_fields = ["dst_file_path"]

    def __init__(
        self,
        src_file_path: str,
        dst_file_path: str,
        slug: str,
        s3_bucket: str,
        tatari_conn_id: str = conn_ids.Tatari.default,
        s3_conn_id: str = conn_ids.AWS.tfg_default,
        tatari_bucket: str = "tatari-reports-exports",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.tatari_conn_id = tatari_conn_id
        self.s3_conn_id = s3_conn_id
        self.src_file_path = src_file_path
        self.dst_file_path = dst_file_path
        self.tatari_bucket = tatari_bucket
        self.s3_bucket = s3_bucket
        self.slug = slug

    @cached_property
    def hook(self):
        return TatariHook(slug=self.slug, tatari_conn_id=self.tatari_conn_id)

    def execute(self, context):
        now = datetime.now(timezone.utc) - timedelta(days=4)
        dest_s3 = S3Hook(self.s3_conn_id)
        src_s3_client = self.hook.session.client("s3")
        paginator = src_s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.tatari_bucket, Prefix=f"{self.src_file_path}"
        )
        with tempfile.TemporaryDirectory() as td:
            for page in pages:
                for key_obj in filter(
                    lambda x: x["LastModified"] >= now, page["Contents"]
                ):
                    local_file_path = Path(td, get_filename(key_obj["Key"]))
                    src_s3_client.download_file(
                        Bucket=self.tatari_bucket,
                        Key=key_obj["Key"],
                        Filename=local_file_path.as_posix(),
                    )
                    dest_s3.load_file(
                        filename=local_file_path.as_posix(),
                        key=f'{self.dst_file_path}/{get_filename(key_obj["Key"])}',
                        bucket_name=self.s3_bucket,
                        replace=True,
                    )
                    self.log.info(
                        f"Uploaded {self.dst_file_path}/{get_filename(key_obj['Key'])} to S3"
                    )
