import os
from urllib.parse import quote_plus, urlencode


def generate_cmd_line_create_statement(conn_id, uri):
    return f"airflow connections -a --conn_id {conn_id} --conn_uri '{uri}'"


def print_conn_envs():
    for k, v in os.environ.items():
        if "_CONN_" in k:
            conn_id = k.replace("AIRFLOW_CONN_", "")
            uri = v
            print(generate_cmd_line_create_statement(conn_id, uri))


def generate_conn_env_variable(
    conn_id, uri_base, extras_dict, wrap_single_quotes=False
):
    extra = "?" + urlencode(extras_dict) if extras_dict else ""
    uri = f"{uri_base}{extra}"
    if wrap_single_quotes:
        uri = f"'{uri}'"
    return f"AIRFLOW_CONN_{conn_id.upper()}={uri}"


def generate_google_conn_env_variable(conn_id, keyfile_path, scopes, user=None):
    with open(keyfile_path, "rt") as f:
        key_json = f.read()
    uri_base = "google_cloud_platform://"
    if user:
        uri_base += f"{user}@"
    conn = {
        "conn_id": conn_id,
        "uri_base": "google_cloud_platform://",
        "extras_dict": {
            "extra__google_cloud_platform__scope": scopes,
            "extra__google_cloud_platform__keyfile_dict": key_json,
        },
    }
    conn_uri = generate_conn_env_variable(**conn)
    return conn_uri


def generate_yaml_conn_env_variable(conn_id, yaml_path, user=None):
    with open(yaml_path, "rt") as f:
        yaml_cred = f.read()
    uri_base = "yaml://"
    if user:
        uri_base += f"{user}@"
    conn = {
        "conn_id": conn_id,
        "uri_base": uri_base,
        "extras_dict": {"yaml": yaml_cred},
    }
    conn_uri = generate_conn_env_variable(**conn)
    return conn_uri


def generate_mssql_odbc_conn_env_variable(
    conn_id, username, password, host, database, driver
):
    conn = {
        "conn_id": conn_id,
        "uri_base": "mssql+pyodbc://{username}:{password}@{host}:{port}/{database}".format(
            username=username,
            password=password,
            host=host,
            database=database,
            port="1433",
        ),
        "extras_dict": {"driver": driver},
    }
    conn_uri = generate_conn_env_variable(**conn)
    return conn_uri


def generate_snowflake_conn_env_variable(
    conn_id,
    username,
    account,
    database,
    password=None,
    private_key=None,
    warehouse=None,
    schema=None,
    authenticator=None,
):
    extras_dict = {}
    if warehouse:
        extras_dict["warehouse"] = warehouse
    if authenticator:
        extras_dict["authenticator"] = authenticator

    if private_key:
        extras_dict["private_key"] = quote_plus(private_key)

    uri_base = "snowflake://{username}:{password}@{account}/{schema}".format(
        username=username,
        password=quote_plus(password),
        account=account,
        schema=f"{database}/{schema}" if schema else database,
    )

    conn = {
        "conn_id": conn_id,
        "uri_base": uri_base,
        "extras_dict": extras_dict,
    }

    conn_uri = generate_conn_env_variable(**conn)
    return conn_uri


def generate_slack_conn_env_variable(conn_id, webhook_token):
    conn = {
        "conn_id": conn_id,
        "uri_base": f"https://{quote_plus('https://hooks.slack.com/services')}",
        "extras_dict": {"webhook_token": webhook_token},
    }
    conn_uri = generate_conn_env_variable(**conn)
    return conn_uri


def generate_ssh_conn_env_variable(conn_id, username, password, host, extras_dict={}):
    conn = {
        "conn_id": conn_id,
        "uri_base": "ssh://{username}:{password}@{host}".format(
            username=username, password=password, host=host
        ),
        "extras_dict": extras_dict,
    }
    conn_uri = generate_conn_env_variable(**conn)
    return conn_uri


def generate_aws_conn_env_variable(conn_id, s3_id, s3_key):
    conn = {
        "conn_id": conn_id,
        "uri_base": "aws://",
        "extras_dict": {"aws_access_key_id": s3_id, "aws_secret_access_key": s3_key},
    }
    conn_uri = generate_conn_env_variable(**conn)
    return conn_uri
