import logging
from contextlib import AbstractContextManager
from time import time


class Timer:
    def __init__(self, description):
        self.description = description

    def __enter__(self):
        self.start = time()
        print(f"{self.description}: timer start")

    def __exit__(self, type, value, traceback):
        self.end = time()
        print(f"{self.description}: timer end - {self.end - self.start}")


class ConnClosing(AbstractContextManager):
    """Context to automatically close something at the end of a block.
    Also, commit if it has commit attr.

    When used like this:

    .. code-block:: python

        with ConnClosing(<module>.open(<arguments>)) as f:
            <block>

    It is equivalent to this:

    .. code-block:: python

        f = <module>.open(<arguments>)
        try:
            <block>
        finally:
            f.commit()
            f.close()

    """

    def __init__(self, thing):
        self.thing = thing

    def __enter__(self):
        return self.thing

    def __exit__(self, *exc_info):
        if hasattr(self.thing, 'autocommit') and not self.thing.autocommit:
            self.thing.commit()
        self.thing.close()


class LateWriter(AbstractContextManager):
    def __init__(self):
        self.writer = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if hasattr(self.writer, 'close'):
            self.writer.close()


class TempLoggingWarning:
    def __init__(self):
        self.logger = None

    def __enter__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)

    def __exit__(self, *exc_info):
        self.logger.setLevel(logging.INFO)
