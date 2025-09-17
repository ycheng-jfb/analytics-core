import logging
import os

import keyring
from airflow.models import Connection
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.secrets.environment_variables import CONN_ENV_PREFIX

logger = logging.getLogger()


class KeyringSecretsBackend(BaseSecretsBackend):
    def __init__(
        self,
        connections_prefix="airflow.conn.",
        variables_prefix="airflow.var.",
        **kwargs,
    ):
        self.connections_prefix = connections_prefix
        self.variables_prefix = variables_prefix
        super().__init__(**kwargs)

    def _conn_service_name(self, conn_id):
        """Derive keyring ``service_name`` parameter based on given connection ``conn_id``"""
        return f"{self.connections_prefix}{conn_id}".lower()

    def _var_service_name(self, key):
        """Derive keyring ``service_name`` parameter based on given variable ``key``"""
        return f"{self.variables_prefix}{key}".lower()

    def get_conn_value(self, conn_id):
        """Retrieve conn uri from keyring"""
        return keyring.get_password(
            service_name=self._conn_service_name(conn_id), username=None
        )

    def get_conn_uri(self, conn_id):
        """Deprecated in favor of get_conn_value"""
        return self.get_conn_value(conn_id=conn_id)

    def get_variable(self, key):
        """Retrieve variable from keyring"""
        return keyring.get_password(
            service_name=self._var_service_name(key), username=None
        )

    def set_conn_uri(self, conn_id, conn_uri):
        """Given airflow conn uri, store with keyring"""
        keyring.set_password(
            service_name=self._conn_service_name(conn_id),
            username=None,
            password=conn_uri,
        )

    def set_connection(self, connection: Connection):
        """Given an airflow Connection object, store with keyring"""
        keyring.set_password(
            service_name=self._conn_service_name(connection.conn_id),
            username=None,
            password=connection.get_uri(),
        )

    def set_all_connections_from_env(self):
        """Convenience function to load connections from env vars to keychain.
        Useful when switching from env vars no keychain."""
        for env_var_name, conn_uri in os.environ.items():
            if CONN_ENV_PREFIX not in env_var_name:
                continue
            conn_id = env_var_name.replace(CONN_ENV_PREFIX, "").lower()
            logger.info(f"setting conn id {conn_id}")
            self.set_conn_uri(conn_id=conn_id, conn_uri=conn_uri)


keyring.get_password("airflow.conn.", None)
