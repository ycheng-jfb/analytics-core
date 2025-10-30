import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class RoktHook(BaseHook):
    def __init__(
        self,
        rokt_conn_id=conn_ids.Rokt.default,
    ):
        self.rokt_conn_id = rokt_conn_id
        self.conn = self.get_connection(self.rokt_conn_id)

        self.password = self.conn.password
        self.base_url = self.conn.host

    @cached_property
    def session(self):
        signin_url = f"{self.base_url}/auth/oauth2/token"
        session = requests.session()

        session.headers.update(
            {
                'Content-Type': 'application/x-www-form-urlencoded',
                "Authorization": f"Basic {self.password}",
            }
        )
        payload = 'grant_type=client_credentials'

        r = session.post(url=signin_url, data=payload)
        r.raise_for_status()

        rdict = r.json()
        token = rdict['access_token']
        session.headers.update({"Authorization": f"Bearer {token}"})
        return session

    def make_request(self, method, endpoint, params=None, json=None, data=None):
        endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
        r = self.session.request(
            method=method, url=f"{self.base_url}/{endpoint}", params=params, json=json, data=data
        )
        r.raise_for_status()
        return r
