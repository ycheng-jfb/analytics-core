import os
from contextlib import contextmanager


@contextmanager
def temporary_env_var(key, value):
    os.environ[key] = value
    yield
    del os.environ[key]


@contextmanager
def temporary_env_vars(env_vars: dict):
    for k, v in env_vars.items():
        os.environ[k] = v
    yield
    for k, v in env_vars.items():
        del os.environ[k]
