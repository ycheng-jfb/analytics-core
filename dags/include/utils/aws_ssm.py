import json
from dataclasses import dataclass
from typing import Iterator, List, Optional

import boto3

from include.utils.data_structures import PrunedDict

print_ = lambda x: print(json.dumps(x, indent=2, default=lambda x: repr(x)))


@dataclass
class Conn:
    conn_id: str
    conn_uri: str
    description: Optional[str] = None

    @property
    def add_connection_cmd(self):
        return (
            f"airflow connections -a --conn_id {self.conn_id.lower()} --conn_uri '{self.conn_uri}'"
        )

    @property
    def env_var(self):
        return f"export AIRFLOW_CONN_{self.conn_id.upper()}='{self.conn_uri}'"


def conn_from_env_var(key, value) -> Optional[Conn]:
    if key.lower()[0:13] != "airflow_conn_":
        print(f"env var {key.lower()} is not a conn_id; skipping")
        return None
    conn_id = key.lower().replace("airflow_conn_", "")

    return Conn(conn_id, value)


def get_conns_from_bash_script(path):
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            if line[0:6] == "export":
                var, _, val = line.partition("=")
                var = var.replace("export", "").strip()
                val = val.strip(" '\"\n")
                yield conn_from_env_var(var, val)


class ParamManager:
    def __init__(self, profile_name=None):
        self.profile_name = profile_name
        self._session = None
        self._client = None

    @property
    def client(self):
        if self._client:
            return self._client
        else:
            self._session = boto3.Session(**PrunedDict(profile_name=self.profile_name))
            self._client = self._session.client("ssm")
            return self._client

    def get_params(self, path):
        next_token = None
        while True:
            response = self.client.get_parameters_by_path(
                **PrunedDict(
                    Path=path,
                    Recursive=False,
                    WithDecryption=True,
                    MaxResults=10,
                    NextToken=next_token,
                )
            )
            curr_params = response["Parameters"]
            for param in curr_params:
                yield param
            next_token = response.get("NextToken")
            if not next_token:
                break

    def get_conns(self, path, exclusion_list=None) -> Iterator[Conn]:
        _exclusion_list = exclusion_list or {}
        excl_list_lower = set(map(lambda x: x.lower(), _exclusion_list))

        def normalize_path(path):
            req_path = path + "/" if not path[-1] == "/" else path
            return req_path

        req_path = normalize_path(path)
        params_iter = self.get_params(path=req_path)
        for param in params_iter:
            key = param["Name"].replace(req_path, "")
            value = param["Value"]
            conn = conn_from_env_var(key, value)

            if not conn:
                continue

            if conn.conn_id in excl_list_lower:
                print(f"{conn.conn_id} is in excl list; skipping")
                continue

            yield conn

    def get_connection_add_statements(self, path, exclusion_list=None):
        for conn in self.get_conns(path=path, exclusion_list=exclusion_list):
            yield conn.add_connection_cmd

    def get_conn_env_vars(self, path, exclusion_list=None):
        for conn in self.get_conns(path=path, exclusion_list=exclusion_list):
            yield conn.env_var

    def print_connection_add_statements(self, path, exclusion_list=None):
        for stmt in self.get_connection_add_statements(path=path, exclusion_list=exclusion_list):
            print(stmt)

    def print_conn_env_vars(self, path, exclusion_list=None):
        for stmt in self.get_conn_env_vars(path=path, exclusion_list=exclusion_list):
            print(stmt)

    def set_conn_param(self, prefix, conn: Conn, overwrite=False):
        def norm_prefix(prefix):
            return prefix + "/" if prefix[-1] != "/" else prefix

        name = norm_prefix(prefix) + conn.conn_id.upper()

        response = self.client.put_parameter(
            **PrunedDict(
                Name=name,
                Value=conn.conn_uri,
                Type="SecureString",
                Overwrite=overwrite,
                Description=conn.description,
            )
        )

        print(response)
        return response

    def set_conn_params_from_list(self, conn_list: List[Conn], prefix, overwrite=False):
        for conn in conn_list:
            self.set_conn_param(
                prefix=prefix,
                conn=conn,
                overwrite=overwrite,
            )

    def write_conns_to_file(self, local_path, ssm_path, exclusion_list=None):
        with open(local_path, "wt") as f:
            for conn in self.get_conn_env_vars(path=ssm_path, exclusion_list=exclusion_list):
                f.write(conn)
                f.write("\n")
