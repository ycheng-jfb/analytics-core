import urllib.parse
from time import sleep

import requests
from airflow.configuration import secrets_backend_list
from airflow.hooks.base import BaseHook
from functools import cached_property

from include.utils.decorators import retry_wrapper
from include.config import conn_ids


class RetryException(Exception):
    """Throw to retry a function"""


class RateLimitException(Exception):
    """Throw when the API hits its rate limit"""


class TooMuchDataException(Exception):
    """Throw when errors due to a result set that is too large"""


def get_password_state_backend(list_name):
    cpssb = secrets_backend_list[0]
    for psb in cpssb.password_state_backend_list:
        if psb.list_name == list_name:
            return psb
    raise Exception("list not found")


class SprinklrHook(BaseHook):
    def __init__(self, sprinklr_conn_id=conn_ids.Sprinklr.default, **kwargs):
        self.sprinklr_conn_id = sprinklr_conn_id

    @cached_property
    def headers(self):
        creds = BaseHook.get_connection(self.sprinklr_conn_id)
        return {
            "Authorization": f"Bearer {creds.extra_dejson['access_token']}",
            "key": creds.login,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def refresh_token(self):
        """Refreshes token and updates the tokens in PasswordState"""
        creds = BaseHook.get_connection(self.sprinklr_conn_id)
        refresh_url = "https://api2.sprinklr.com/oauth/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        params = {
            "client_id": creds.login,
            "client_secret": creds.password,
            "redirect_uri": creds.extra_dejson["redirect_uri"],
            "grant_type": "refresh_token",
            "refresh_token": creds.extra_dejson["refresh_token"],
        }
        response = requests.post(refresh_url, params=params, headers=headers)
        rj = response.json()
        self.headers["Authorization"] = f"Bearer {rj['access_token']}"

        # Update password state
        sb = get_password_state_backend("developer")
        new_uri = creds.get_uri().split("?")[0] + (
            f'?redirect_uri={creds.extra_dejson["redirect_uri"]}'
            f'&access_token={urllib.parse.quote(rj["access_token"])}'
            f'&refresh_token={urllib.parse.quote(rj["refresh_token"])}'
        )
        sb.set_password(title=self.sprinklr_conn_id, value=new_uri)

    @retry_wrapper(3, RetryException, sleep_time=10)
    def make_request(self, method, url, params=None, payload=None):
        try:
            r = requests.request(
                method, url, headers=self.headers, params=params, json=payload
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(r.status_code, r.reason, r.text)
            if r.status_code == 401:
                self.refresh_token()
                raise RetryException
            elif r.status_code == 504:  # Gateway time out
                raise RetryException
            elif "Developer Over Rate" in r.text:
                print("Hit API rate limit.")
                raise RateLimitException
            elif "Developer Over Qps" in r.text:
                print("Hit throttling limit. Waiting 1 minute.")
                sleep(60)
                raise RetryException
            elif r.status_code == 400:
                rj = r.json()
                error_message = rj.get("errors", [{}])[0].get("message") or rj.get(
                    "message"
                )
                if error_message == "Please try again":
                    raise RetryException
                elif error_message == "Cannot fetch more than 10000 messages overall":
                    raise TooMuchDataException
                elif "Invalid request/response" in error_message:
                    print(payload)
                raise e
            else:
                raise e
        return r
