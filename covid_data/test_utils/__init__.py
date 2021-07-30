class MockDB(object):
    execute_return = None

    def connect(self, *args, **kwargs):
        return self

    def exec_driver_sql(self, *args, **kwargs):
        return self.execute_return

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass
