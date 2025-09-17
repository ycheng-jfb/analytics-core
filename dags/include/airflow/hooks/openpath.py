import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class OpenpathHook(BaseHook):
    def __init__(self, openpath_conn_id=conn_ids.Openpath.default):
        conn = self.get_connection(openpath_conn_id)
        self.login = conn.login
        self.password = conn.password
        self.base_url = conn.host

    @cached_property
    def session(self):
        auth_url = f"https://{self.base_url}/auth/login"
        payload = {"email": self.login, "password": self.password}
        headers = {"accept": "application/json", "content-type": "application/json"}
        response = requests.post(auth_url, json=payload, headers=headers)
        token = response.json()['data']['token']
        headers = {"accept": "application/json", "Authorization": token}
        session = requests.session()
        session.headers = headers
        return session
