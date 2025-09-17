import contextlib
import os
from contextlib import contextmanager
from functools import wraps
from urllib.parse import urlparse

import airflow.settings
from airflow.models import DagModel
from include.airflow.configuration import AIRFLOW_HOME
from include.airflow.configuration import conf as tfg_conf
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from airflow import configuration as airflow_conf

SQL_ALCHEMY_CONN_URI = None
SQL_ALCHEMY_SCHEMA = None
CLUSTER_BASE_URL = airflow_conf.get(
    "webserver", "base_url", fallback="http://localhost:8080"
).rstrip("/")


def initialize():
    """
    Sets up variables for plugins that use tfg_meta schema.

    """
    global SQL_ALCHEMY_CONN_URI
    global SQL_ALCHEMY_SCHEMA

    default_conn_uri = f"sqlite:///{AIRFLOW_HOME}/airflow.db"
    conf_conn = tfg_conf.get("core", "sql_alchemy_conn", fallback=default_conn_uri)
    conn = (
        os.environ.get("AIRFLOW__TFGMETA__SQL_ALCHEMY_CONN")
        or os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        or conf_conn
    )
    if urlparse(conn).scheme == "sqlite":
        SQL_ALCHEMY_CONN_URI = conn.replace("airflow.db", "tfg_meta.db")
        SQL_ALCHEMY_SCHEMA = None
    else:
        SQL_ALCHEMY_CONN_URI = conn
        SQL_ALCHEMY_SCHEMA = "tfg_meta"


@contextlib.contextmanager
def create_session():
    """
    Get sqlalchemy session for use with tfg_meta schema

    """
    engine = create_engine(SQL_ALCHEMY_CONN_URI)

    Session = sessionmaker(
        autocommit=False, autoflush=False, bind=engine, expire_on_commit=False
    )
    """
    Contextmanager that will create and teardown a session.
    """
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def provide_session(func):
    """
    For use with tfg_meta schema.

    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = "session"

        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and func_params.index(
            arg_session
        ) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session() as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)

    return wrapper


initialize()


@contextmanager
def session_scope():
    """
    This will return airflow.settings session
    """
    session = airflow.settings.Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def enable_dag(dag_id):
    """
    This will un_pause the dag if it is paused.

    Args:
        dag_id: Dag_id of the DAG to un_pause
    """
    with session_scope() as session:
        query = session.query(DagModel).filter(DagModel.dag_id == dag_id)
        d = query.first()
        d.is_paused = False
