import csv
import gzip

import aiohttp
import requests
from aiohttp import ClientTimeout
from airflow.hooks.base import BaseHook
from include.config import conn_ids


class CreatorIQHook(BaseHook):
    def __init__(
        self,
        creatoriq_conn_id=conn_ids.CreatorIQ.creatoriq_default,
        api_key=None,
        key_name="X-SOCIALEDGE-ID",
    ):
        conn = self.get_connection(creatoriq_conn_id)
        self.api_key = api_key or conn.password
        self.key_name = key_name

    def get_conn(self):
        headers = {
            self.key_name: self.api_key,
            "Accept": "application/json; charset=UTF-8",
        }
        session = requests.session()
        session.headers = headers
        return session

    def make_request(
        self,
        url: str,
        params: dict = None,
        method: str = "GET",
    ):
        session = self.get_conn()
        response = session.request(method=method, url=url, params=params)
        if response.status_code not in [200]:
            content = response.content
            self.log.error(f"request failed with message {content}")
            response.raise_for_status()
        return response

    def get_dict_rows_to_file(self, filename, column_list, rows, compression="gzip"):
        open_func = gzip.open if compression == "gzip" else open
        with open_func(filename, "at") as f:
            writer = csv.DictWriter(
                f=f,
                fieldnames=column_list,
                extrasaction="ignore",
                delimiter="\t",
                dialect="unix",
                quoting=csv.QUOTE_MINIMAL,
            )
            for row in rows:
                writer.writerow(row)

    def get_async_conn(self):
        headers = {
            self.key_name: self.api_key,
            "Accept": "application/json; charset=UTF-8",
        }
        timeout = ClientTimeout(total=60 * 60 * 1.2)
        return aiohttp.ClientSession(headers=headers, timeout=timeout)
