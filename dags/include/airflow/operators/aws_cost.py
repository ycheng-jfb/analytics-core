import re
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from airflow.models import BaseOperator
from functools import cached_property

from include.airflow.hooks.s3 import S3Hook


class AwsCostCopy(BaseOperator):
    template_fields = ["current_timestamp"]

    def __init__(
        self,
        source_bucket,
        source_prefix,
        target_output_bucket,
        target_output_prefix,
        target_copy_bucket,
        target_copy_prefix,
        s3_conn_id,
        column_list,
        current_timestamp,
        delete_all_files=False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.source_prefix = source_prefix
        self.target_output_bucket = target_output_bucket
        self.target_output_prefix = target_output_prefix
        self.target_copy_bucket = target_copy_bucket
        self.target_copy_prefix = target_copy_prefix
        self.s3_conn_id = s3_conn_id
        self.column_list = column_list
        self.current_timestamp = current_timestamp
        self.delete_all_files = delete_all_files

    @cached_property
    def hook(self):
        return S3Hook(self.s3_conn_id)

    def get_keys(self):
        keys = self.hook.list_keys(
            bucket_name=self.source_bucket, prefix=self.source_prefix
        )
        return keys

    def create_new_file(self, temp_dir, src_key):
        temp_dir_path = Path(temp_dir)
        temp_file_name = self.hook.download_file(
            src_key,
            bucket_name=self.source_bucket,
            local_path=temp_dir_path,
        )

        df_blank = pd.DataFrame(columns=self.column_list)
        df_data = pd.read_csv(temp_file_name, dtype=object, compression="gzip")
        df_merged = pd.concat([df_blank, df_data], axis=0, ignore_index=True)
        df_target = df_merged[self.column_list]
        df_target["updated_at"] = self.current_timestamp
        key_split = src_key.split("/")
        new_file = f"{key_split[3]}_{self.current_timestamp}_{key_split[4]}"

        self.log.info(f"uploading {new_file}")
        new_file_path = Path(temp_dir_path / new_file)
        df_target.to_csv(new_file_path, index=False, compression="gzip")

        if self.target_output_prefix[-1] == "/":
            key = f"{self.target_output_prefix}{key_split[3]}/{new_file}"
        else:
            key = f"{self.target_output_prefix}/{key_split[3]}/{new_file}"

        return new_file_path, key

    def execute(self, context=None):
        keys = self.get_keys()
        if keys is None:
            return

        successfully_copied = []
        for src_key in keys:
            pattern = re.compile(r".*DA_COST-[0-9]{5}\.csv\.gz")
            if pattern.match(src_key):
                with TemporaryDirectory() as temp_dir:
                    new_file_path, key = self.create_new_file(temp_dir, src_key)

                    self.hook.load_file(
                        filename=str(new_file_path),
                        key=key,
                        bucket_name=self.target_output_bucket,
                        replace=True,
                    )

            self.log.info(f"copying {src_key}")
            self.hook.copy(
                source_bucket=self.source_bucket,
                target_bucket=self.target_copy_bucket,
                source_key=src_key,
                target_key=src_key.replace(self.source_prefix, self.target_copy_prefix),
            )
            successfully_copied.append(src_key)

        self.hook.delete_objects(
            bucket=self.source_bucket,
            keys=successfully_copied,
        )
