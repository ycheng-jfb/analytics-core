import logging
import os
import pickle
import time
import traceback
from functools import wraps
from pathlib import Path
from tempfile import gettempdir

from airflow.utils.email import send_email


def exponential_backoff(retry_count):
    def wrap(f):
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            i = 0
            while True:
                i += 1
                logging.info(f"executing {f.__name__}: attempt {i} of {retry_count}")
                try:
                    return f(*args, **kwargs)
                except Exception:
                    if i == retry_count:
                        raise
                    else:
                        wait_secs = 2 ** (i - 1)
                        logging.info(f"attempt failed. waiting {wait_secs} seconds")
                        time.sleep(wait_secs)

        return wrapped_f

    return wrap


def retry_wrapper(retry_count, *exceptions, sleep_time=2):
    """
    Decorator to retry a function ``retry_count`` times if the exception thrown is in list of ``exceptions``.

    Args:
        retry_count: number of times to retry before raising exception
        *exceptions: list of exceptions to suppress.  if a different exception is thrown by wrapped
            function, it will be raised immediately.
        sleep_time: seconds between each retry

    Returns: callable

    """

    def wrap(f):
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            for retry_num in range(retry_count):
                try:
                    return f(*args, **kwargs)
                except tuple(exceptions) as e:
                    print(f"exception {e.__class__.__name__} caught")
                    if retry_num == retry_count - 1:
                        raise e
                print(f"waiting {sleep_time} seconds before retry")
                time.sleep(sleep_time)

        return wrapped_f

    return wrap


def alert_wrapper(recipients, process_name):
    def _alert_wrapper(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            try:
                f(*args, **kwargs)
            except Exception as e:
                logging.error(e, exc_info=True)
                send_email(
                    to=recipients,
                    subject=f"FAILURE: {process_name}",
                    html_content=traceback.format_exc(),
                )
                raise e

        return wrap

    return _alert_wrapper


def cached(cachefile_path, ttl_hours=8):
    """
    A function that creates a decorator which will use "cachefile_path" for caching the results of the decorated
    function "fn".
    Only works if function takes no args
    """

    def decorator(fn):  # define a decorator for a function "fn"
        @wraps(fn)
        def wrapped():  # define a wrapper that will finally call "fn" with all arguments
            # if cache exists -> load it and return its content
            if os.path.exists(cachefile_path):
                mtime = os.path.getmtime(cachefile_path)
                nowtime = time.time()
                cache_age = (nowtime - mtime) / 3600
                if cache_age < ttl_hours:
                    try:
                        with open(cachefile_path, "rb") as cachehandle:
                            print("using cached result from '%s'" % cachefile_path)
                            return pickle.load(cachehandle)
                    except EOFError:
                        pass
            # execute the function with all arguments passed
            res = fn()

            # write to cache file
            with open(cachefile_path, "wb") as cachehandle:
                print("saving result to cache '%s'" % cachefile_path)
                pickle.dump(res, cachehandle)

            return res

        return wrapped

    return decorator  # return this "customized" decorator that uses "cachefile_path"


pickle_dir = Path(gettempdir(), '.pickle')


def pickle_cache(name, folder=pickle_dir):
    if not os.path.exists(pickle_dir):
        os.mkdir(pickle_dir)
    """
    Experimental decorator to pickle an object as cache.  Is never expired

    Args:
        retry_count: number of times to retry before raising exception
        *exceptions: list of exceptions to suppress.  if a different exception is thrown by wrapped
            function, it will be raised immediately.
        sleep_time: seconds between each retry

    Returns: callable

    """
    pickle_path = Path(folder, name).with_suffix('.pickle')

    def wrap(func):
        @wraps(func)
        def wrapped_f(*args, **kwargs):
            if os.path.exists(pickle_path):
                with open(pickle_path, 'rb') as f:
                    obj = pickle.load(f)
            else:
                obj = func(*args, **kwargs)
                with open(pickle_path, 'wb') as f:
                    pickle.dump(obj, f)
            return obj

        return wrapped_f

    return wrap
