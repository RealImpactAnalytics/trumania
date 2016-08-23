from datagenerator import operations


class FakeOp(operations.Operation):
    """
    just returning hard-coded results as output
    """

    def __init__(self, output, logs):
        self.output = output
        self.logs = logs

    def __call__(self, action_data):
        return self.output, self.logs

