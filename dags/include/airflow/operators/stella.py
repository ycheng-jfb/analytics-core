import gzip
import json
import tempfile
from pathlib import Path

import jwt
import pendulum
import requests
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from functools import cached_property
from include.config import conn_ids

from include.utils.decorators import retry_wrapper


class RetryException(Exception):
    """Throw this to retry a function call"""


class StellaToS3Operator(BaseOperator):
    """
    This will retrieve data from the Stella API and put it into s3

    Args:
        from_date: str of date to pull from
        to_date: str of date to pull to
        key: s3 key name
        bucket: s3 bucket name
        stella_conn_id: stella connection id. Defaults to stella_default
        s3_conn_id: s3 connection id. Defaults to stella_default
        after_sequence_id: Optionally pass a sequence id to pull from. If this is passed, then
            "from_date" will be ignored
    """

    template_fields = ["key", "from_date", "to_date"]

    def __init__(
        self,
        from_date: str,
        to_date: str,
        key,
        bucket,
        stella_conn_id=conn_ids.Stella.default,
        s3_conn_id=conn_ids.AWS.tfg_default,
        after_sequence_id=None,
        *args,
        **kwargs,
    ):
        self.from_date = from_date
        self.to_date = to_date
        self.key = key
        self.bucket = bucket
        self.stella_conn_id = stella_conn_id
        self.s3_conn_id = s3_conn_id
        self.after_sequence_id = after_sequence_id
        self._session = None
        self.rows_exported = 0
        super().__init__(*args, **kwargs)

    @cached_property
    def headers(self):
        creds = BaseHook.get_connection(self.stella_conn_id)  # type: Connection
        claims = {'iat': int(pendulum.DateTime.utcnow().replace(microsecond=0).timestamp())}
        encoded_jwt = jwt.encode(claims, creds.password, algorithm='HS256')
        return {
            'x-api-key': creds.login,
            'Authorization': encoded_jwt,
            "Content-Type": "application/json",
        }

    @retry_wrapper(2, RetryException, sleep_time=15)
    def make_get_request(self, url, params=None):
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
        except requests.exceptions.SSLError:
            raise RetryException
        except requests.exceptions.HTTPError as e:
            if response.status_code == 500:  # Internal server error
                raise RetryException
            else:
                print(response.text)
                raise e
        return response.json()

    def get_data(self):
        # First call
        api_call_counter = 0
        if self.after_sequence_id:
            params = {'after': self.after_sequence_id, "created_at_lte": self.to_date}
            print(f"Running after sequence ID {self.after_sequence_id} until {self.to_date}")
        else:
            params = {"created_at_gte": self.from_date, "created_at_lte": self.to_date}
            print(f"Running from {self.from_date} to {self.to_date}")
        url = 'https://api.stellaconnect.net/v2/data'
        data = self.make_get_request(url, params)

        # Get paginated results based on sequence_id. Data will no longer be returned once it
        # reaches the end.
        while data:
            api_call_counter += 1
            for row in data:
                yield row
            last_sequence_id = max(d['sequence_id'] for d in data)
            params = {'after': last_sequence_id}
            data = self.make_get_request(url, params)
        print(f"Made {api_call_counter} api calls")

    def write_to_file(self, filename):
        if not filename.endswith(".gz"):
            raise ValueError(f"File will be gzipped but filename {filename} does not end in .gz")

        with gzip.open(filename, "wt") as f:
            data = self.get_data()
            for row in data:
                self.rows_exported += 1
                f.write(json.dumps(row))
                f.write("\n")
        print(f"rows exported: {self.rows_exported}")

    def execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            local_path = (Path(td) / Path(self.key).name).as_posix()
            self.write_to_file(local_path)

            if self.rows_exported:
                s3_hook = S3Hook(self.s3_conn_id)
                s3_hook.load_file(
                    filename=local_path,
                    key=self.key,
                    bucket_name=self.bucket,
                    replace=True,
                )
