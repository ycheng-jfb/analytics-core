from urllib.parse import quote, urlencode

from include.utils.environment import temporary_env_vars


def get_conn(metastore_conn_uri, fernet_key, conn_id):
    env_dict = {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": metastore_conn_uri,
        "AIRFLOW__CORE__FERNET_KEY": fernet_key,
        "TFG_CREATE_META_TABLES": "False",
    }
    with temporary_env_vars(env_dict):
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
    return conn


def add_conn(metastore_conn_uri, fernet_key, conn_kwargs):
    env_dict = {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": metastore_conn_uri,
        "AIRFLOW__CORE__FERNET_KEY": fernet_key,
        "TFG_CREATE_META_TABLES": "False",
    }
    with temporary_env_vars(env_dict):
        from airflow.models import Connection
        from airflow.utils.db import merge_conn

        conn = Connection(**conn_kwargs)
        merge_conn(conn)


def get_uri(conn):
    """
    Returns airflow connection URI representation

    Args:
        conn: :class:`~.airflow.models.Connection` object

    Returns:
        str: airflow connection URI representation of connection
    """

    def get_scheme(conn_type):
        return "{}://".format(str(conn_type).lower().replace("_", "-"))

    def get_authority_block(login, password):
        authority_block = ""
        if login is not None:
            authority_block += quote(login, safe="")
        if password is not None:
            authority_block += ":" + quote(password, safe="")
        if authority_block > "":
            authority_block += "@"
        return authority_block

    def get_host_block(host, port, schema):
        host_block = ""
        if host:
            host_block += quote(host, safe="")
        if conn.port:
            if host_block > "":
                host_block += ":{}".format(port)
            else:
                host_block += "@:{}".format(port)
        if schema:
            host_block += "/{}".format(quote(schema, safe=""))
        return host_block

    def get_extra(extra_dejson):
        return "?{}".format(urlencode(extra_dejson)) if extra_dejson else ""

    uri = get_scheme(conn_type=conn.conn_type)
    uri += get_authority_block(login=conn.login, password=conn.password)
    uri += get_host_block(host=conn.host, port=conn.port, schema=conn.schema)
    uri += get_extra(extra_dejson=conn.extra_dejson)
    return uri


def get_uri_from_db(metastore_uri, fernet_key, conn_id):
    """
    For retrieving airflow connection URI from metastore database.

    Args:
        metastore_uri: airflow conn uri for metastore database
        fernet_key: fernet key for metastore database
        conn_id: conn id you want to retrieve

    Returns:
        str: env var definition for conn

    Examples:
        >>> import os
        >>> metastore_uri = os.environ['AIRFLOW_CONN_PROD_METASTORE_DB']
        >>> fernet_key = os.environ['FERNET_KEY_AIRFLOW_PROD']
        >>> uri = get_uri_from_db(metastore_uri=metastore_uri, fernet_key=fernet_key, conn_id='tableau_default')
        >>> print(uri)
        'https://abc:123@host.com?this_param=some_val'

    """

    conn = get_conn(metastore_uri, fernet_key, conn_id)
    uri = get_uri(conn)
    return f"export AIRFLOW_CONN_{conn_id.upper()}='{uri}'"
