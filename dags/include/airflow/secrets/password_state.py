import json
from pathlib import Path
from typing import Dict, List

import requests
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.secrets.environment_variables import EnvironmentVariablesBackend


class PasswordStateSecretsBackend(BaseSecretsBackend):
    """
    Retrieve airflow connections from Password State creds management app.

    Only works for a single password list.  To use multiple, see :class:`~CompositePasswordStateSecretsBackend`.

    Args:
        api_key: key for the password list
        list_id: integer id for the list
        list_name: name of the list

    To use, make your airflow config look like this::

        [secrets]
        backend = include.airflow.secrets.password_state.PasswordStateSecretsBackend
        backend_kwargs = {"api_key": "ha9hp29ipudkh", "list_id": 1234, "list_name": "abc"}

    """

    def __init__(self, api_key, list_id, list_name, **kwargs):
        self.session = requests.Session()
        self.session.headers = {'APIKey': api_key}
        self.base_url = 'https://secrets.techstyle.net/api'
        self.list_id = list_id
        self.list_name = list_name
        super().__init__(**kwargs)

    def make_request(self, rel_url, params=None, json=None, method='GET'):
        r = self.session.request(
            method=method, url=f"{self.base_url}/{rel_url}", params=params, json=json
        )
        return r

    def search_title(self, title):
        response = self.make_request(f'searchpasswords/{self.list_id}?title={title.lower()}')
        data = response.json()
        if not data:
            return
        if 'errors' in data[0]:
            return
        for row in data:
            if row['Title'].lower() == title.lower():
                return row

    def get_conn_value(self, conn_id):
        row = self.search_title(conn_id)
        if row:
            return row['Password']

    def get_conn_uri(self, conn_id):
        """Deprecated in favor of get_conn_value"""
        return self.get_conn_value(conn_id=conn_id)

    def set_password(self, title, value):
        if not self.list_id:
            raise ValueError('password_list_id is not set')
        row = self.search_title(title)
        json_data = {
            "Title": title.lower(),
            "password": value,
        }
        if row:
            print(f"password '{title}' exists; updating")
            method = 'PUT'
            json_data["PasswordID"] = row['PasswordID']
        else:
            print(f"password '{title}' not found; adding")
            method = 'POST'
            json_data["PasswordListID"] = self.list_id
        r = self.make_request(rel_url='passwords', json=json_data, method=method)
        r.raise_for_status()
        return r

    def __repr__(self):
        return f"{self.__class__.__name__}(list_id='{self.list_id}', list_name='{self.list_name}')"

    def get_variable(self, key):
        return None


class CompositePasswordStateSecretsBackend(BaseSecretsBackend):
    """
    Retrieve airflow connections from Password State creds management app.

    You may specify api keys for password lists and they will be searched in order.

    Env vars will still be searched first.

    If you only need to enable a single password list, you can alternatively use :class:`~.PasswordStateSecretsBackend`.

    Args:
        kwargs_list: List of ``backend_kwargs`` as you would supply to :class:`~.PasswordStateSecretsBackend`.

    To use, make your airflow config look like this::

        [secrets]
        backend = include.airflow.secrets.password_state.CompositePasswordStateSecretsBackend
        backend_kwargs = {"kwargs_list": [{"api_key": "abcd1234", "list_id": 1234, "list_name": "abc1"}, {"api_key": "efgh5678", "list_id": 1235, "list_name": "abc2"}]}

    Once you have enabled it, you can use it to manage password lists.  For example::

        >>> from airflow.secrets import secrets_backend_list
        >>> s = secrets_backend_list[0]  # the first one will be CompositePasswordStateSecretsBackend
        >>> admin_backend = s.password_state_backend_dict['admin']  # assuming you have an "admin" list defined
        >>> admin_backend.search_title('tableau_prod')
        'tableau://abc:abc111@host.com'
        >>> admin_backend.set_password('tableau_prod', 'tableau://abc:abc222@host.com')

    """  # noqa: E501

    def __init__(self, kwargs_list, **kwargs):
        super().__init__(**kwargs)
        self.password_state_backend_list: List[PasswordStateSecretsBackend] = [
            PasswordStateSecretsBackend(**k) for k in kwargs_list
        ]
        self.password_state_backend_dict: Dict[str, PasswordStateSecretsBackend] = {
            x.list_name: x for x in self.password_state_backend_list
        }
        self.backend_list = [EnvironmentVariablesBackend(), *self.password_state_backend_list]

    def get_conn_value(self, conn_id):
        for backend in self.backend_list:
            uri = backend.get_conn_value(conn_id)
            if uri:
                return uri

    def get_conn_uri(self, conn_id):
        return self.get_conn_value(conn_id=conn_id)

    @classmethod
    def load_backend(cls):
        """
        Use this method on an adhoc basis to retrieve a :class:`~.PasswordStateSecretsBackend` instance
        which you can use to manage creds in password state.

        Examples:
            >>> s = CompositePasswordStateSecretsBackend.load_backend()
            >>> admin_backend = s.password_state_backend_dict['admin']
            >>> admin_backend.set_password('tableau_prod', 'tableau://abc:abc132@host.com')
        """
        from airflow.configuration import conf

        kwargs_list = json.loads(conf.get('secrets', 'backend_kwargs'))['kwargs_list']
        return cls(kwargs_list)

    def get_variable(self, key):
        for backend in self.backend_list:
            var = backend.get_variable(key=key)
            if var:
                return var


def parse_connections_from_file(filename):
    """Convenience function to parse a shell script where connection env vars are defined."""

    conns = {}
    for line in Path(filename).read_text().splitlines():
        prefix = 'export AIRFLOW_CONN_'
        if not line.startswith(prefix):
            continue
        conn_id, conn_uri = line.replace(prefix, '').split('=', maxsplit=1)
        conns[conn_id.lower()] = conn_uri.strip("'")
    return conns
