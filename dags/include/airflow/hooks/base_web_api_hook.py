from abc import abstractmethod

import requests
from airflow.hooks.base import BaseHook
from functools import cached_property


class BaseWebApiHook(BaseHook):
    def __init__(
        self,
        conn_id,
        host=None,
        version=None,
    ):
        self.conn_id = conn_id
        self.conn = self.get_connection(self.conn_id)
        self.extras = self.conn.extra_dejson

        self.login = self.conn.login
        self.password = self.conn.password
        self.host = host or self.conn.host or self.extras.get("host")
        self.version = version or self.extras.get("version")

        self.base_url = self.get_base_url()
        super().__init__()

    @abstractmethod
    def get_base_url(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_base_headers(self) -> dict:
        raise NotImplementedError

    @cached_property
    def session(self):
        session = requests.session()
        session.headers = self.get_base_headers()
        return session

    def make_request(self, method, endpoint, headers=None, params=None, json=None, data=None):
        endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
        base_url = self.base_url[:-1] if self.base_url[-1] == "/" else self.base_url
        url = f'{base_url}/{endpoint}'

        r = self.session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json,
            data=data,
        )
        r.raise_for_status()
        return r
