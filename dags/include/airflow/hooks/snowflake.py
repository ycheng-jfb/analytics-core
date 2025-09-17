import os
from io import StringIO
from typing import Dict, Optional

import urllib.parse
from airflow.hooks.dbapi import DbApiHook
from asn1crypto.core import Any
from snowflake import connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.util_text import split_statements as _split_statements


class SnowflakeHook(DbApiHook):
    """
    A client to interact with Snowflake.

    This hook requires the snowflake_conn_id connection. The snowflake host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation. If used with the S3ToSnowflakeOperator
    add 'aws_access_key_id' and 'aws_secret_access_key' to extra field in the connection.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :type snowflake_conn_id: str
    :param account: snowflake account name
    :type account: Optional[str]
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :type authenticator: Optional[str]
    :param warehouse: name of snowflake warehouse
    :type warehouse: Optional[str]
    :param database: name of snowflake database
    :type database: Optional[str]
    :param region: name of snowflake region
    :type region: Optional[str]
    :param role: name of snowflake role
    :type role: Optional[str]
    :param schema: name of snowflake schema
    :type schema: Optional[str]
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: Optional[dict]

    .. note::
        get_sqlalchemy_engine() depends on snowflake-sqlalchemy

    .. seealso::
        For more information on how to use this Snowflake connection, take a look at the guide:
        :ref:`howto/operator:SnowflakeOperator`
    """

    conn_name_attr = "snowflake_conn_id"
    default_conn_name = "snowflake_default"
    conn_type = "snowflake"
    hook_name = "Snowflake"
    supports_autocommit = True

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import (
            BS3PasswordFieldWidget,
            BS3TextFieldWidget,
        )
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "extra__snowflake__account": StringField(
                lazy_gettext("Account"), widget=BS3TextFieldWidget()
            ),
            "extra__snowflake__warehouse": StringField(
                lazy_gettext("Warehouse"), widget=BS3TextFieldWidget()
            ),
            "extra__snowflake__database": StringField(
                lazy_gettext("Database"), widget=BS3TextFieldWidget()
            ),
            "extra__snowflake__region": StringField(
                lazy_gettext("Region"), widget=BS3TextFieldWidget()
            ),
            "extra__snowflake__aws_access_key_id": StringField(
                lazy_gettext("AWS Access Key"), widget=BS3TextFieldWidget()
            ),
            "extra__snowflake__aws_secret_access_key": PasswordField(
                lazy_gettext("AWS Secret Key"), widget=BS3PasswordFieldWidget()
            ),
            "extra__snowflake__role": StringField(
                lazy_gettext("Role"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        import json

        return {
            "hidden_fields": ["port"],
            "relabeling": {},
            "placeholders": {
                "extra": json.dumps(
                    {
                        "authenticator": "snowflake oauth",
                        "private_key_file": "private key",
                        "session_parameters": "session parameters",
                    },
                    indent=1,
                ),
                "host": "snowflake hostname",
                "schema": "snowflake schema",
                "login": "snowflake username",
                "password": "snowflake password",
                "extra__snowflake__account": "snowflake account name",
                "extra__snowflake__warehouse": "snowflake warehouse name",
                "extra__snowflake__database": "snowflake db name",
                "extra__snowflake__region": "snowflake hosted region",
                "extra__snowflake__aws_access_key_id": "aws access key id (S3ToSnowflakeOperator)",
                "extra__snowflake__aws_secret_access_key": "aws secret access key (S3ToSnowflakeOperator)",
                "extra__snowflake__role": "snowflake role",
            },
        }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.private_key = kwargs.pop("private_key", None)
        self.account = kwargs.pop("account", None)
        self.warehouse = kwargs.pop("warehouse", None)
        self.database = kwargs.pop("database", None)
        self.region = kwargs.pop("region", None)
        self.role = kwargs.pop("role", None)
        self.schema = kwargs.pop("schema", None)
        self.authenticator = kwargs.pop("authenticator", None)
        self.session_parameters = kwargs.pop("session_parameters", None)
        self.timezone = kwargs.pop("timezone", None)

    def _get_conn_params(self) -> Dict[str, Optional[str]]:
        """
        One method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(self.snowflake_conn_id)  # type: ignore[attr-defined]

        account = conn.host or conn.extra_dejson.get("account", "")
        warehouse = conn.extra_dejson.get(
            "extra__snowflake__warehouse", ""
        ) or conn.extra_dejson.get("warehouse", "")
        database = conn.extra_dejson.get(
            "extra__snowflake__database", ""
        ) or conn.extra_dejson.get("database", "")
        region = conn.extra_dejson.get(
            "extra__snowflake__region", ""
        ) or conn.extra_dejson.get("region", "")
        role = conn.extra_dejson.get(
            "extra__snowflake__role", ""
        ) or conn.extra_dejson.get("role", "")
        schema = conn.schema or ""
        authenticator = conn.extra_dejson.get("authenticator", "snowflake")
        session_parameters = conn.extra_dejson.get("session_parameters")
        timezone = conn.extra_dejson.get("timezone")

        # Extract private key (if present)
        private_key = conn.extra_dejson.get(
            "extra__snowflake_private_key", ""
        ) or conn.extra_dejson.get("private_key", "")

        # Prepare connection config

        with open("dags/rsa_key.p8", "rb") as key_file:  # The guidance is in README.md
            p_key = serialization.load_pem_private_key(
                key_file.read(), password=None, backend=default_backend()
            )

        # Convert private key to Snowflake-compatible format
        private_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        conn_config = {
            "user": conn.login,
            "schema": self.schema or schema,
            "database": self.database or database,
            "account": self.account or account,
            "warehouse": self.warehouse or warehouse,
            "region": self.region or region,
            "role": self.role or role,
            "authenticator": self.authenticator or authenticator,
            "session_parameters": self.session_parameters or session_parameters,
            "application": os.environ.get("AIRFLOW_SNOWFLAKE_PARTNER", "AIRFLOW"),
            "timezone": self.timezone or timezone,
            "private_key": private_key,
        }

        return conn_config

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""
        conn_config = self._get_conn_params()
        uri = (
            "snowflake://{user}:{password}@{account}/{database}/{schema}"
            "?warehouse={warehouse}&role={role}&authenticator={authenticator}"
        )
        return uri.format(**conn_config)

    def get_conn(self) -> SnowflakeConnection:
        """Returns a snowflake.connection object"""
        conn_config = self._get_conn_params()
        conn = connector.connect(**conn_config)
        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)

    @staticmethod
    def split_statements(cmd):
        for x in list(_split_statements(StringIO(cmd), remove_comments=True)):
            next_val = x[0].rstrip(";")
            if len(next_val) > 0:
                yield next_val

    def execute_multiple(self, sql: str, autocommit=True, parameters=None):
        with self.get_conn() as cnx:
            if autocommit is False:
                cnx.autocommit(autocommit)
            self.execute_multiple_with_cnx(cnx=cnx, sql=sql, parameters=parameters)

    def execute_multiple_with_cnx(self, cnx, sql: str, parameters=None):
        self.log.info(f"snowflake session_id: {cnx.session_id}")
        cur = cnx.cursor(cursor_class=DictCursor)
        cmd_list = list(self.split_statements(sql))

        for stmt in cmd_list:
            self.log.info(f"Executing: \n{stmt}")
            cur.execute(stmt, params=parameters)
            if cur.messages:
                print(f"messages: {cur.messages}")
            results = cur.fetchmany(100)
            cur.reset()
            for row in results:
                print(row)

    def execute_file(self, path, autocommit=False, parameters=None):
        with open(path, "rt") as f:
            cmd = f.read()
        self.execute_multiple(sql=cmd, autocommit=autocommit, parameters=parameters)

    def execute_file_with_cnx(self, cnx, path, parameters=None):
        with open(path, "rt") as f:
            cmd = f.read()
        self.execute_multiple_with_cnx(cnx=cnx, sql=cmd, parameters=parameters)
