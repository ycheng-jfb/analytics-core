import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class RokuHook(BaseHook):
    def __init__(
        self,
        roku_conn_id=conn_ids.Roku.default,
        version="v2",
        api_key=None,
        host=None,
    ):
        self.roku_conn_id = roku_conn_id
        self.conn = self.get_connection(self.roku_conn_id)
        self.extras = self.conn.extra_dejson

        self.login = self.conn.login
        self.password = self.conn.password
        self.host = host or self.conn.host or self.extras.get("host")
        self.version = version or self.extras.get("version")
        self.api_key = api_key or self.extras.get("x-api-key")

        self.base_url = f"https://{self.host}/{self.version}"
        self.site_id = None
        self.user_id = None

    @cached_property
    def session(self):
        payload = {
            "username": self.login,
            "password": self.password,
        }
        signin_url = f"{self.base_url}/oauth/token"
        session = requests.session()
        session.headers = {
            "Accept": "application/json",
            "Content-type": "application/json",
            "x-api-key": self.api_key,
        }
        r = session.post(url=signin_url, json=payload)
        r.raise_for_status()

        rdict = r.json()
        token = rdict["id_token"]
        session.headers.update({"Authorization": "Bearer " + token})
        return session

    def make_request(self, method, endpoint, params=None, json=None, data=None):
        endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
        r = self.session.request(
            method=method,
            url=f"{self.base_url}/{endpoint}",
            params=params,
            json=json,
            data=data,
        )
        r.raise_for_status()
        return r
