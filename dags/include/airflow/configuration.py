import configparser
import os


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    ``expandvars`` and ``expanduser`` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def get_airflow_home():
    return expand_env_var(os.environ.get('AIRFLOW_HOME', '~/airflow'))


def get_airflow_config(airflow_home):
    if 'AIRFLOW_CONFIG' not in os.environ:
        return os.path.join(airflow_home, 'airflow.cfg')
    return expand_env_var(os.environ['AIRFLOW_CONFIG'])


AIRFLOW_HOME = get_airflow_home()
AIRFLOW_CONFIG = get_airflow_config(AIRFLOW_HOME)

conf = configparser.ConfigParser()
conf.read(AIRFLOW_CONFIG)
