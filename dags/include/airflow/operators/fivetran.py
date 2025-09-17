from typing import Dict
import json
from functools import cached_property

from include.airflow.hooks.fivetran import FivetranHook

from airflow.models import BaseOperator
from include.config import conn_ids


class FivetranOperator(BaseOperator):
    """ """

    template_fields = [
        "request_params",
        "path",
    ]

    def __init__(
        self,
        request_params: Dict,
        path: str,
        method: str = "GET",
        fivetran_conn_id: str = conn_ids.Fivetran.default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.request_params = request_params
        self.fivetran_conn_id = fivetran_conn_id
        self.method = method
        self.path = path

    @cached_property
    def hook(self):
        return FivetranHook(fivetran_conn_id=self.fivetran_conn_id)

    def execute(self, context=None):
        response = self.hook.make_request(
            method=self.method, path=self.path, params=self.request_params
        )
        print(response.json())


class FivetranUserMapOperator(FivetranOperator):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

    def execute(self, context=None):
        user_response = self.hook.make_request(path="users")
        users = user_response.json()["data"]["items"]
        team_response = self.hook.make_request(path="teams")
        teams = team_response.json()["data"]["items"]
        for team in teams:
            print(f"checking users for team {team['name']}")
            team_details = self.hook.make_request(path="teams/" + team["id"] + "/users")
            team_members = team_details.json()["data"]["items"]
            team_member_ids = [member["user_id"] for member in team_members]
            user_ids = [
                user["id"]
                for user in users
                if team["name"]
                and user["role"]
                and team["name"].lower() in user["role"].lower()
            ]
            missing_user_ids = [
                user_id for user_id in user_ids if user_id not in team_member_ids
            ]
            print(f"users missing from team: {missing_user_ids}")
            for id in missing_user_ids:
                data = json.dumps({"user_id": id, "role": "Team Member"})
                res = self.hook.make_request(
                    method="POST", path=f"teams/{team['id']}/users", data=data
                )
                print(res.json())
