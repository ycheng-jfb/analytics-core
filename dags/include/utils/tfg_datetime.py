import re

from dateutil.parser import parse


def datetime_to_str_int(dt, no_tz=True):
    pattern = r"T|\.|:|-|000$"
    if no_tz:
        pattern += r"|(\+|\-)\d{2}:\d{2}$"
    if hasattr(dt, 'isoformat'):
        return re.sub(pattern, "", dt.isoformat())


def datetime_to_str_iso(dt, no_tz=True):
    pattern = r"\.\d+$|-|:"
    if no_tz:
        pattern += r"|(\+|\-)\d{2}:\d{2}$"
    if hasattr(dt, 'isoformat'):
        return re.sub(pattern, "", dt.isoformat())


def datetime_str_to_unix(dt: str):
    if dt is not None:
        return int(datetime_str_to_dt(dt).timestamp())


def datetime_str_to_dt(dt_str):
    dt = parse(dt_str)
    return dt


def datetime_to_str_date(dt):
    if dt is not None:
        return dt.isoformat()[0:10]


def datetime_to_str_sql(dt):
    if dt is not None:
        return dt.isoformat(sep=" ")


def datetime_str_to_str_sql(dt):
    if dt is not None:
        return datetime_to_str_sql(datetime_str_to_dt(dt))


def date_trunc(dt):
    if dt is not None:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
