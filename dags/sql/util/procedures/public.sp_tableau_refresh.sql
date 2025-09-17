USE DATABASE UTIL;
USE SCHEMA PUBLIC;

CREATE OR REPLACE PROCEDURE sp_tableau_refresh (
    data_source_type varchar,
    data_source varchar,
    check_completion boolean DEFAULT TRUE
)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (tableau_ext_acc_int)
SECRETS = ('tableau_creds' = tableau_creds)
HANDLER = 'run'
AS
$$
import json
import logging
import time
from random import random
from typing import Any, Dict, Optional, Union

import requests
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import current_timestamp, when_matched, when_not_matched

try:
    import _snowflake
except ImportError:
    pass

log = logging.getLogger(__name__)

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d - %(name)s [%(funcName)s()] - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def _make_request(
    session: requests.Session,
    method: str,
    endpoint: str,
    base_url: str,
    site_id: str,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
) -> requests.Response:
    endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
    r = session.request(
        method=method,
        url=f"{base_url}/sites/{site_id}/{endpoint}",
        params=params,
        json=json,
        data=data,
    )
    r.raise_for_status()
    return r


def exponential_backoff_wait_times(
    growth_param: float = 1.3,
    max_wait_time_seconds: int = 60 * 10,
    initial_wait: Union[int, float] = 5,
    max_sleep_interval: int = 60 * 1,
):
    total_wait_secs = 0.0
    wait_count = 0
    curr_wait = 0.0
    while True:
        wait_count += 1
        if total_wait_secs > max_wait_time_seconds:
            raise TimeoutError(
                f"total wait seconds exceed max wait time of {max_wait_time_seconds}"
            )
        if curr_wait > max_sleep_interval:
            curr_wait = max_sleep_interval
        else:
            curr_wait = growth_param ** (wait_count - 1) + initial_wait - 1 + random()
            curr_wait = round(curr_wait, 4)
            if curr_wait > max_sleep_interval:
                curr_wait = max_sleep_interval
        total_wait_secs += curr_wait
        yield curr_wait


class Waiter:
    def __init__(
        self,
        growth_param: float = 1.3,
        max_wait_time_seconds: int = 60 * 10,
        initial_wait: Union[int, float] = 5,
        max_sleep_interval: int = 60 * 1,
    ):
        self.growth_param = growth_param
        self.max_wait_time_seconds = max_wait_time_seconds
        self.initial_wait = initial_wait
        self.max_sleep_interval = max_sleep_interval
        self.waiter = exponential_backoff_wait_times(
            growth_param=growth_param,
            max_wait_time_seconds=max_wait_time_seconds,
            initial_wait=initial_wait,
            max_sleep_interval=max_sleep_interval,
        )
        self.elapsed_time = 0

    def next(self):
        next_val = next(self.waiter)
        self.elapsed_time += next_val
        return next_val


def run(snowflake_session: snowpark.Session, data_source_type: str, data_source: str, check_completion: bool) -> snowpark.DataFrame:
    waiter = Waiter(
        growth_param=1.2,
        initial_wait=10,
        max_wait_time_seconds=2 * 60 * 60,
    )

    secret_obj = _snowflake.get_username_password('tableau_creds')
    host = "10ay.online.tableau.com"
    login = secret_obj.username
    password = secret_obj.password
    content_url = "techstyle"
    version = "3.24"
    base_url = f"https://{host}/api/{version}"
    payload = {
        "credentials": {
            "name": login,
            "password": password,
            "site": {"contentUrl": content_url},
        }
    }
    signin_url = f"{base_url}/auth/signin"
    session = requests.session()
    session.headers = {
        "Accept": "application/json",
        "Content-type": "application/json",
    }
    r = session.post(url=signin_url, json=payload)
    r.raise_for_status()
    rdict = r.json()
    token = rdict["credentials"]["token"]
    site_id = rdict["credentials"]["site"]["id"]
    content_url = rdict["credentials"]["site"]["contentUrl"]
    session.headers.update({"X-Tableau-Auth": token})

    if data_source_type.lower() == "data_source_name":
        data_source_name = data_source
        r = _make_request(
            session,
            method="GET",
            endpoint=f"datasources?filter=name:eq:{data_source_name}",
            base_url=base_url,
            site_id=site_id,
        )
        data_source_id = r.json()["datasources"]["datasource"][0]["id"]
    else:
        data_source_id = data_source

    try:
        r = _make_request(
            session,
            method="POST",
            endpoint=f"datasources/{data_source_id}/refresh",
            json={},
            base_url=base_url,
            site_id=site_id,
        )
        j = r.json()
        job_id = j['job']['id']
        while True and check_completion:
            log.info(f'{job_id} - pulling job status')
            job_status_resp = _make_request(
                session,
                method="GET",
                endpoint=f"jobs/{job_id}",
                base_url=base_url,
                site_id=site_id,
            )
            status = job_status_resp.json()["job"]
            if 'finishCode' in status.keys():
                if status['finishCode'] == '0':
                    log.info(f'{job_id} - Extract completed {status}')
                    break
                else:
                    raise Exception(f"job {job_id} failed with response {status}")
            else:
                wait_seconds = waiter.next()
                log.info(f"{job_id} - waiting {wait_seconds} seconds before next try")
                time.sleep(wait_seconds)
        status = [(data_source_id, data_source, "SUCCESS", "")]
        if not check_completion:
            status = [(data_source_id, data_source, "NO STATUS CHECK", "NO LOG")]
    except requests.exceptions.HTTPError as e:
        status = [(data_source_id, data_source, "FAILED", str(e))]

    status_df = snowflake_session.create_dataframe(
        status, schema=["data_source_id", "data_source_name", "status", "error_log"]
    )
    log_table = snowflake_session.table("util.public.tableau_refresh_log")
    log_table.merge(
        status_df,
        (log_table["data_source_id"] == status_df["data_source_id"])
        & (log_table["data_source_name"] == status_df["data_source_name"]),
        [
            when_matched().update(
                {
                    "refresh_status": status_df["status"],
                    "error_log": status_df["error_log"],
                    "last_refresh_ts": current_timestamp(),
                }
            ),
            when_not_matched().insert(
                {
                    "data_source_id": status_df["data_source_id"],
                    "data_source_name": status_df["data_source_name"],
                    "refresh_status": status_df["status"],
                    "error_log": status_df["error_log"],
                    "last_refresh_ts": current_timestamp(),
                }
            ),
        ],
    )
    return status_df
$$;
