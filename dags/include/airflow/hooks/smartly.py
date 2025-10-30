import gzip
import tempfile
from datetime import date
from pathlib import Path

import requests
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.utils.data_structures import chunk_list
from include.config import conn_ids


class SmartlyHook(BaseHook):
    def __init__(
        self,
        smartly_conn_id=conn_ids.Smartly.default,
        s3_conn_id=conn_ids.AWS.tfg_default,
        version="v1.2",
        access_token=None,
    ):
        self.smartly_conn_id = smartly_conn_id
        self.s3_conn_id = s3_conn_id
        self.version = version
        conn = self.get_connection(self.smartly_conn_id)
        self.access_token = access_token or conn.password
        self.session = requests.session()
        self.method = "GET"

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: dict = None,
        data: dict = None,
        json: dict = None,
        stream=False,
    ):
        if not params:
            params = {}
        params.update(api_token=self.access_token)
        response = self.session.request(
            method=method,
            url=endpoint,
            data=data,
            params=params,
            json=json,
            stream=stream,
        )
        if response.status_code not in [200]:
            content = response.content
            self.log.error(f"request failed with message {content}")
            print(response.url)
            response.raise_for_status()
        return response

    def get_report_to_file(self, filename, endpoint: str, params: dict, compression="gzip"):
        open_func = gzip.open if compression == "gzip" else open
        with open_func(filename, "wb") as f:  # type: ignore
            for batch in chunk_list(params["account_id"]):
                params["account_id"] = ",".join(batch)
                print(f'fetching data for accounts: {params["account_id"]}')
                response = self.make_request(
                    method=self.method, endpoint=endpoint, params=params, stream=True
                )
                for chunk in response.iter_content():
                    f.write(chunk)

    def get_report_to_s3(self, bucket, key, endpoint: str, params: dict, s3_replace=True):
        with tempfile.TemporaryDirectory() as td:
            filename = Path(td, str(date.today()) + ".csv.gz")
            print(filename)
            self.get_report_to_file(filename=filename, endpoint=endpoint, params=params)
            s3_hook = S3Hook(self.s3_conn_id)
            print(f"uploading to {key}")
            s3_hook.load_file(
                filename=filename.as_posix(),
                key=key,
                bucket_name=bucket,
                replace=s3_replace,
            )
            print(f"uploaded to {key}")
