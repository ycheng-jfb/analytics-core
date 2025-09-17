import math

import requests
from airflow.hooks.base import BaseHook
from functools import cached_property
from include.config import conn_ids


class TableauHook(BaseHook):
    def __init__(
        self,
        tableau_conn_id=conn_ids.Tableau.default,
        version="3.24",
        host=None,
        content_url=None,
        *args,
        **kwargs,
    ):
        self.tableau_conn_id = tableau_conn_id
        self.conn = self.get_connection(self.tableau_conn_id)
        self.extras = self.conn.extra_dejson

        self.login = self.conn.login
        self.password = self.conn.password
        self.host = host or self.conn.host or self.extras.get("host")
        self.content_url = content_url or self.extras.get("content_url")
        self.version = version or self.extras.get("version")

        self.base_url = f"https://{self.host}/api/{self.version}"
        self.site_id = None
        self.user_id = None

    @cached_property
    def session(self):
        payload = {
            "credentials": {
                "name": self.login,
                "password": self.password,
                "site": {"contentUrl": self.content_url},
            }
        }
        signin_url = f"{self.base_url}/auth/signin"
        session = requests.session()
        session.headers = {
            "Accept": "application/json",
            "Content-type": "application/json",
        }
        r = session.post(url=signin_url, json=payload)
        r.raise_for_status()

        rdict = r.json()
        token = rdict["credentials"]["token"]
        self.site_id = rdict["credentials"]["site"]["id"]
        self.content_url = rdict["credentials"]["site"]["contentUrl"]
        self.user_id = rdict["credentials"]["user"]["id"]
        session.headers.update({"X-Tableau-Auth": token})
        return session

    def make_request(self, method, endpoint, params=None, json=None, data=None):
        endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
        r = self.session.request(
            method=method,
            url=f"{self.base_url}/sites/{self.site_id}/{endpoint}",
            params=params,
            json=json,
            data=data,
        )
        r.raise_for_status()
        return r

    def get_data_source_id(self, data_source_name):
        r = self.make_request(
            method="GET", endpoint=f"datasources?filter=name:eq:{data_source_name}"
        )
        data_source_id = r.json()["datasources"]["datasource"][0]["id"]
        return data_source_id

    def refresh_data_source(self, data_source_id):
        r = self.make_request(
            method="POST", endpoint=f"datasources/{data_source_id}/refresh", json={}
        )
        return r.json()

    def sign_out(self):
        signout_url = f"{self.base_url}/auth/signout"
        r = self.session.post(signout_url, json={})
        r.raise_for_status()
        self.log.info("Sign out successful.")

    def get_job_status(self, job_id):
        """
        See https://onlinehelp.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobstasksschedules.htm#query_job
        :param job_id:
        :return:
        The finishCode indicates the status of the job: 0 for success, 1 for error, or 2 for cancelled.
        """
        r = self.make_request(method="GET", endpoint=f"jobs/{job_id}")
        return r.json()["job"]

    def get_groups(self):
        r = self.make_request(method="GET", endpoint="groups")
        return r.json()["groups"]["group"]

    # Should remove and refactor operator that uses this to instead use api_get_call
    def get_users_in_group(self, group_id):
        page_number = 0
        while True:
            page_number += 1
            r = self.make_request(
                method="GET", endpoint=f"groups/{group_id}/users?pageNumber={page_number}"
            )
            data = r.json()
            yield from data["users"]["user"] if data["users"].get("user") else []

            total_available = int(data["pagination"]["totalAvailable"])
            page_size = int(data["pagination"]["pageSize"])
            page_count = math.ceil(total_available / page_size)

            if page_number >= page_count:
                break

    def api_get_call(self, api_path, api_object_name, is_list=True):
        page_number = 0
        while True:
            page_number += 1
            r = self.make_request(method="GET", endpoint=f"{api_path}?pageNumber={page_number}")
            data = r.json()
            if is_list:
                yield from (
                    data[api_object_name][api_object_name[:-1]]
                    if data[api_object_name].get(api_object_name[:-1])
                    else []
                )
            else:
                yield data[api_object_name]

            if data.get("pagination"):
                total_available = int(data["pagination"]["totalAvailable"])
                page_size = int(data["pagination"]["pageSize"])
                page_count = math.ceil(total_available / page_size)

                if page_number >= page_count:
                    break
            else:
                break
