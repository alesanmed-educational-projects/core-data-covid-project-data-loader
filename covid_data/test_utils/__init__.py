from typing import Any


class MockDB(object):
    execute_return: Any = None

    def cursor(self, *args, **kwargs):
        return self

    def execute(self, *args, **kwargs):
        return self.execute_return

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass
