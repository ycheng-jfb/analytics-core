import socket

import pendulum
from airflow.models import TaskInstance


def hostname_callable():
    try:
        return socket.gethostbyname(socket.gethostname())
    except socket.gaierror:
        return socket.gethostname()


def render_template(task, attr_name, data_interval_start=pendulum.today()):
    """
    Render template for task.  Convenience method for validating template fields.

    Example usage

    .. code-block:: python

        render_template(to_s3)
        print(to_s3.key)
    """
    TaskInstance(task, execution_date=data_interval_start).render_templates()
    print(getattr(task, attr_name))


def flatten_json(y, flatten_list=True):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list and flatten_list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def remove_listed_columns(data, columns):
    def remove_column(x, fields):
        if len(fields) > 1:
            field = fields.pop(0)
            y = x.get(field)
            if y:
                remove_column(y, fields)
                if not x[field]:
                    x.pop(field)
        else:
            if x.get(fields[0]):
                x.pop(fields[0])

    for column in columns:
        column_fields = column.split('_')
        remove_column(data, column_fields)


def scrub_nones(x, repl=''):
    if type(x) is list:
        for i in range(len(x)):
            if x[i] is None:
                x[i] = repl
            else:
                scrub_nones(x[i])
    elif type(x) is dict:
        for k in x:
            if x[k] is None:
                x[k] = repl
            else:
                scrub_nones(x[k])


def recursive_lookup(obj, string_list):
    value = obj.get(string_list[0])

    if value is not None:
        if len(string_list) == 1:
            return value
        else:
            return recursive_lookup(value, string_list[1:])
    else:
        return None


def flatten_json_lookup_list(obj, lookup_list, prefix=None):
    if not prefix:
        prefix = ''
    else:
        prefix = prefix + '_'
    output = {}
    for i in lookup_list:
        value = recursive_lookup(obj, i.split(':'))
        output[prefix + i.replace(':', '_')] = value
    return output
