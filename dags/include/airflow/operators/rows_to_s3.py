import csv
import gzip
import json
import tempfile
from abc import abstractmethod
from typing import Dict, Iterable, List

from airflow.models import BaseOperator
from include.config import conn_ids
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.airflow.operators.watermark import BaseProcessWatermarkOperator


class BaseRowsToS3CsvOperator(BaseOperator):
    """
    Subclass this operator and override :meth:`get_rows` to produce an operator that writes to csv
    and stores on s3.

    E.g. if you hit a paginated api, override :meth:`get_rows` to yield rows.

    Args:
        key: key to write to on s3.  do not include bucket name. (templated)
        bucket: bucket name.
        column_list: list of columns yielded in :meth:`get_rows`.  Determines order in csv.
        s3_conn_id: conn id of s3 connection for ``bucket``
        write_header: bool flag on whether to write header to csv file
        hook_conn_id: conn_id for hook used to extract data
        endpoint: api endpoint when hitting a web api
        kwargs: passed to BaseOperator
    """

    template_fields = ["key"]

    def __init__(
        self,
        key: str,
        bucket: str,
        column_list: List[str],
        s3_conn_id=conn_ids.AWS.tfg_default,
        write_header: bool = False,
        hook_conn_id=None,
        endpoint=None,
        **kwargs,
    ):

        self.s3_conn_id = s3_conn_id
        self.bucket = bucket
        self.key = key
        self.column_list = column_list
        self.write_header = write_header
        self.hook_conn_id = hook_conn_id
        self.endpoint = endpoint
        super().__init__(**kwargs)

    @abstractmethod
    def get_rows(self) -> Iterable[Dict]:
        raise NotImplementedError

    def upload_to_s3(self, filename: str, bucket, key, s3_replace=True):
        print(f"uploaded to {key}")
        s3_hook = S3Hook(self.s3_conn_id)
        s3_hook.load_file(
            filename=filename,
            key=key,
            bucket_name=bucket,
            replace=s3_replace,
        )

    @staticmethod
    def write_dict_rows_to_file(
        rows: Iterable[Dict], filename: str, column_list: List[str], write_header: bool = False
    ):
        """
        Iterates through ``rows`` and writes to file as csv.

        Args:
            rows: iterable of row dicts
            filename: where to write the file
            column_list: List of columns in ``rows`` to write to csv.  Determines order.  Other
            dict elements are ignored.
        """
        with gzip.open(filename, "wt") as f:
            writer = csv.DictWriter(
                f=f,
                delimiter="\t",
                dialect="unix",
                extrasaction="ignore",
                fieldnames=column_list,
                quoting=csv.QUOTE_MINIMAL,
            )

            if write_header:
                writer.writerow(dict(zip(column_list, column_list)))

            if isinstance(rows, list):
                writer.writerows(rows)
            else:
                for row in rows:
                    writer.writerow(row)

    def BaseRowsToS3CsvOperator_execute(self):
        with tempfile.NamedTemporaryFile(mode='wb', delete=True) as temp_file:
            rows = self.get_rows()
            self.write_dict_rows_to_file(
                rows=rows,
                filename=temp_file.name,
                column_list=self.column_list,
                write_header=self.write_header,
            )
            temp_file.flush()
            self.upload_to_s3(temp_file.name, bucket=self.bucket, key=self.key)

    def execute(self, context=None):
        self.BaseRowsToS3CsvOperator_execute()


class BaseRowsToS3JsonOperator(BaseOperator):
    """
    Subclass this operator and override :meth:`get_rows` to produce an operator that writes to
    ndjson file and stores on s3.

    E.g. if you hit a paginated api, override :meth:`get_rows` to yield rows.

    Args:
        key: key to write to on s3.  do not include bucket name. (templated)
        bucket: bucket name.
        s3_conn_id: conn id of s3 connection for ``bucket``
        hook_conn_id: conn_id for hook used to extract data
        endpoint: api endpoint when hitting a web api
        kwargs: passed to BaseOperator
    """

    template_fields = ["key"]

    def __init__(
        self,
        key: str,
        bucket: str,
        s3_conn_id=conn_ids.AWS.tfg_default,
        hook_conn_id=None,
        endpoint=None,
        **kwargs,
    ):
        self.s3_conn_id = s3_conn_id
        self.bucket = bucket
        self.key = key
        self.hook_conn_id = hook_conn_id
        self.endpoint = endpoint
        super().__init__(**kwargs)

    @abstractmethod
    def get_rows(self) -> Iterable[Dict]:
        raise NotImplementedError

    def upload_to_s3(self, filename: str, bucket, key, s3_replace=True):
        print(f"uploaded to {key}")
        s3_hook = S3Hook(self.s3_conn_id)
        s3_hook.load_file(
            filename=filename,
            key=key,
            bucket_name=bucket,
            replace=s3_replace,
        )

    @staticmethod
    def write_dict_rows_to_file(rows: Iterable[Dict], filename: str):
        """
        Iterates through ``rows`` and write to file as ndjson.

        Args:
            rows: iterable of row dicts
            filename: where to write the file
        """
        with gzip.open(filename, "wt") as f:
            for row in rows:
                f.write(json.dumps(row))
                f.write('\n')

    def BaseRowsToS3JsonOperator_execute(self):
        with tempfile.NamedTemporaryFile(mode='wb', delete=True) as temp_file:
            rows = self.get_rows()
            self.write_dict_rows_to_file(rows=rows, filename=temp_file.name)
            temp_file.flush()
            self.upload_to_s3(temp_file.name, bucket=self.bucket, key=self.key)

    def execute(self, context=None):
        self.BaseRowsToS3JsonOperator_execute()


class BaseRowsToS3CsvWatermarkOperator(BaseProcessWatermarkOperator, BaseRowsToS3CsvOperator):
    """
    Subclass this operator and override :meth:`get_rows` to produce an operator that writes to csv
    and stores on s3, and override :meth: `get_high_watermark` to get current high_watermark value.

    E.g. if you hit a paginated api, override :meth:`get_rows` to yield rows.

    Args:
        key: key to write to on s3.  do not include bucket name. (templated)
        bucket: bucket name.
        column_list: list of columns yielded in :meth:`get_rows`.  Determines order in csv.
        process_name: name of the process e.g. "thisdb.thisschema.this_table"
        namespace: group name for process.  e.g. "acquisition_copy_paste"
        s3_conn_id: conn id of s3 connection for ``bucket``
        write_header: bool flag on whether to write header to csv file
        initial_load_value: on first run, the value that should be used for low watermark
        hook_conn_id: conn_id for hook used to extract data
        endpoint: api endpoint when hitting a web api
        kwargs: passed to BaseOperator
    """

    def __init__(
        self,
        key,
        bucket,
        column_list,
        process_name,
        namespace,
        s3_conn_id=conn_ids.AWS.tfg_default,
        write_header=False,
        initial_load_value: str = '1900-01-01T00:00:00+00:00',
        hook_conn_id=None,
        endpoint=None,
        **kwargs,
    ):
        super().__init__(
            key=key,
            bucket=bucket,
            column_list=column_list,
            s3_conn_id=s3_conn_id,
            write_header=write_header,
            process_name=process_name,
            namespace=namespace,
            initial_load_value=initial_load_value,
            hook_conn_id=hook_conn_id,
            endpoint=endpoint,
            **kwargs,
        )

    @abstractmethod
    def get_rows(self) -> Iterable[Dict]:
        raise NotImplementedError

    def watermark_execute(self, context=None):
        self.BaseRowsToS3CsvOperator_execute()


class BaseRowsToS3JsonWatermarkOperator(BaseProcessWatermarkOperator, BaseRowsToS3JsonOperator):
    """
    Subclass this operator and override :meth:`get_rows` to produce an operator that writes to
    ndjson file and stores on s3, and override :meth: `get_high_watermark` to get current
    high_watermark value.

    E.g. if you hit a paginated api, override :meth:`get_rows` to yield rows.

    Args:
        key: key to write to on s3.  do not include bucket name. (templated)
        bucket: bucket name.
        column_list: list of columns yielded in :meth:`get_rows`.  Determines order in csv.
        process_name: name of the process e.g. "thisdb.thisschema.this_table"
        namespace: group name for process.  e.g. "acquisition_copy_paste"
        s3_conn_id: conn id of s3 connection for ``bucket``
        initial_load_value: on first run, the value that should be used for low watermark
        hook_conn_id: conn_id for hook used to extract data
        endpoint: api endpoint when hitting a web api
        kwargs: passed to BaseOperator
    """

    def __init__(
        self,
        key,
        bucket,
        process_name,
        namespace,
        s3_conn_id=conn_ids.AWS.tfg_default,
        initial_load_value='1900-01-01T00:00:00+00:00',
        hook_conn_id=None,
        endpoint=None,
        **kwargs,
    ):
        super().__init__(
            key=key,
            bucket=bucket,
            s3_conn_id=s3_conn_id,
            process_name=process_name,
            namespace=namespace,
            initial_load_value=initial_load_value,
            hook_conn_id=hook_conn_id,
            endpoint=endpoint,
            **kwargs,
        )

    @abstractmethod
    def get_rows(self) -> Iterable[Dict]:
        raise NotImplementedError

    def watermark_execute(self, context=None):
        self.BaseRowsToS3JsonOperator_execute()
