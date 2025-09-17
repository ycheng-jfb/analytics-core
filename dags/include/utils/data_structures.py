from typing import Dict, List


def prune_dict(dict_) -> dict:
    """
    Remove keys from dict where the values are empty.

    Args:
        dict_: dict for which you want to prune the empty values

    Examples:
        >>> prune_dict({'a': '1', 'b': None, 'c': ''})
        {'a': '1'}

    Returns:
        dict: dict pruned of empty values

    """
    return {k: v for k, v in dict_.items() if v}


class PrunedDict(dict):
    """
    Replacement for :class:`dict` which removes empty elements.

    Examples:
        >>> PrunedDict(a='1', b=None, c='')
        {'a': '1'}

    """

    def __init__(self, **kwargs):
        super().__init__(**prune_dict(kwargs))


def split_list_by_field(field_name, list_: List) -> Dict[str, List]:
    """
    Splits a list of dicts according to a particular dict value for key passed in param fieldname.
    Returns dict whose keys are the distinct values present.

    Examples:
        >>> split_list_by_field([
        ...     {'id': 1, 'val':123},
        ...     {'id': 2, 'val': 345}
        ... ])
        {1: [{'id': 1, 'val':123}], 2: [{'id': 2, 'val': 345}]}

    Args:
        field_name: the field by which you want to split the list of dicts
        list_:

    """

    ret = {}  # type: Dict[str, List]
    for obj in list_:
        val = obj[field_name] if isinstance(obj, dict) else getattr(obj, field_name)
        if val not in ret:
            ret[val] = []
        ret[val].append(obj)
    return ret


def batch_elem(relative_url, method="GET", **kwargs):
    """
    Generates batch request elements.  To use, place elements in list and pass to _batch_get.
    """
    elem = {"method": method, "relative_url": relative_url}
    elem.update(kwargs)
    return elem


def chunk_list(list_: List, size=50):
    """
    Takes a list and yields slices of length ``size`` until the full list has been consumed.

    Examples:
        >>> my_list = list(range(0, 51))
        >>> g = chunk_list(my_list, size=25)
        >>> len(next(g))
        25
        >>> len(next(g))
        25
        >>> len(next(g))
        1

    Yields:
        list: the next chunk from param ``list_``

    """
    start = 0
    end = size
    while True:
        ret = list_[start:end]
        start += size
        end += size
        if len(ret) > 0:
            yield ret
        else:
            break


def chunk_df(df, chunk_size=10000):
    """
    Generator to yield a df in chunks

    Args:
        df: the df you want to split
        chunk_size: how many rows per chunk

    Yields:
        pandas.DataFrame: the next chunk from param ``df``

    """
    for i in range(0, len(df), chunk_size):
        yield df[i : i + chunk_size]


def greatest(*args):
    """
    Returns greatest element in a list.  None values are least.

    Examples:
        >>> greatest(*['abc', 'bbc', None])
        'bbc'

    """
    ret_val = None
    for val in args:
        if val is None:
            continue
        if ret_val is None or val > ret_val:
            ret_val = val
    return ret_val
