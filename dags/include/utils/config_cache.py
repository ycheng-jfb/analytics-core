import datetime
import pickle
import tempfile
from pathlib import Path
from typing import Callable

from peewee import BlobField, CharField, DateTimeField, Model, SqliteDatabase

cache_db_path = Path(tempfile.gettempdir(), "python_cache_db.db")

db = SqliteDatabase(cache_db_path)


class Config(Model):
    name = CharField(primary_key=True)
    value = BlobField()
    updated_time = DateTimeField(default=datetime.datetime.utcnow)

    class Meta:
        database = db
        without_rowid = True


class Cache:
    """
    Pickles the result of a function call in a cache DB.
    After ttl_seconds, refreshes the cache next time get is called.

    Ex:
    >>> # define costly function you want to cache
    >>> def slow_func(num):
    >>>     result = []
    >>>     for i in range(0, num):
    >>>         time.sleep(0.2)
    >>>         result.append(i)
    >>>     return result
    >>>
    >>>
    >>> # define cached getter
    >>> c = Cache(
    >>>     name='slow-func-cache',
    >>>     update_fn=partial(func=slow_func, num=10),
    >>> )
    >>>
    >>> # get the value (initial run will load the
    >>> print(c.get())

    """

    def __init__(self, name: str, update_fn: Callable, ttl_seconds: int = 3600 * 8):
        self.name = name
        self.update_fn = update_fn
        self.ttl_seconds = ttl_seconds
        self.utc_now = datetime.datetime.utcnow()
        self.record = Config()
        self.value = None
        Config.create_table()

    def get(self):
        with db.atomic("EXCLUSIVE"):
            try:
                self.utc_now = datetime.datetime.utcnow()
                self.record = Config.select().where(Config.name == self.name).get()

                last_updated_time = self.record.updated_time
                diff = (self.utc_now - last_updated_time).seconds

                if diff < self.ttl_seconds:
                    self.value = pickle.loads(self.record.value)
                    return self.value
                else:
                    return self.__update_record()
            except Config.DoesNotExist:
                return self.__initialize_record()

    def __update_record(self):
        self.value = self.update_fn()
        self.record.value = pickle.dumps(self.value)
        self.record.updated_time = self.utc_now
        self.record.save()
        return self.value

    def __initialize_record(self):
        self.value = self.update_fn()
        self.record.name = self.name
        self.record.value = pickle.dumps(self.value)
        self.record.updated_time = self.utc_now
        self.record.save(force_insert=True)
        return self.value
