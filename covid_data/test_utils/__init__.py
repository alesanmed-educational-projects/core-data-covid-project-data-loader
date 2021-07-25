class MockDB:
    execute_return = None

    def connect(self, *args, **kwargs):
        return self

    def execute(self, *args, **kwargs):
        return self.execute_return
