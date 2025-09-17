from abc import ABC, abstractmethod
from pathlib import Path
from typing import Type

from airflow.models import BaseOperator
from functools import cached_property

from include.airflow.hooks.s3 import S3Hook


class BaseKeyMapper(ABC):
    """
    Used with :class:`~.MoveS3FilesOperator`.

    Determines target_key when copying and renaming from a prefix.
    """

    def __init__(self, source_key):
        self.source_key = source_key

    def __repr__(self):
        return self.source_key

    @property
    def filename(self):
        return Path(self.source_key).name

    @property
    @abstractmethod
    def target_key(self):
        pass

    def debug_info(self):
        print(
            f"""
        source: {self.source_key}
        target: {self.target_key}
        """
        )


class MoveS3FilesOperator(BaseOperator):
    """
    Custom operator to move s3 files from one bucket to another.

    We also adjust the naming for more efficient snowflake loading (to avoid need for regex
    matching) and cleaner s3 organization (grouping the files under a prefix by date).

    Applies canned ACl ``bucket-owner-full-control``.

    Deletes source files after successful copy.

    Args:
        source_bucket: the source bucket
        source_prefix: the prefix for source files
        target_bucket: the target bucket
        s3_conn_id: the conn id to use for the boto3 client we use to copy the files
        key_mapper: implementation of :class:`~BaseKeyMapper`. Used for determining how to
            rename key.
        **kwargs: passed to BaseOperator
    """

    def __init__(
        self,
        source_bucket,
        source_prefix,
        target_bucket,
        s3_conn_id,
        key_mapper: Type[BaseKeyMapper],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.source_prefix = source_prefix
        self.target_bucket = target_bucket
        self.s3_conn_id = s3_conn_id
        self.key_mapper = key_mapper

    @cached_property
    def hook(self) -> S3Hook:
        """

        Returns:
            S3Hook: our internal implementation of airflow :class:`~airflow.hooks.S3_hook.S3Hook`.

        """
        return S3Hook(self.s3_conn_id)

    def get_keys(self):
        keys = self.hook.list_keys(
            bucket_name=self.source_bucket, prefix=self.source_prefix
        )
        return keys

    def dry_run(self):
        """
        Prints out the keys that would be processed in the next run, and what they would be renamed
        to upon being copied.

        """
        keys = self.get_keys()
        for src_key in keys:
            f = self.key_mapper(src_key)
            f.debug_info()

    def execute(self, context=None):
        keys = self.get_keys()
        if keys is None:
            return
        successfully_copied = []
        for src_key in keys:
            print(f"copying {src_key}")
            km = self.key_mapper(src_key)
            self.hook.copy(
                source_bucket=self.source_bucket,
                target_bucket=self.target_bucket,
                source_key=src_key,
                target_key=km.target_key,
            )
            successfully_copied.append(src_key)
        self.hook.delete_objects(
            bucket=self.source_bucket,
            keys=successfully_copied,
        )
