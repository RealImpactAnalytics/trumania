from datagenerator.core import operations


class FakeOp(operations.Operation):
    """
    just returning hard-coded results as output
    """

    def __init__(self, output, logs):
        self.output = output
        self.logs = logs

    def __call__(self, action_data):
        return self.output, self.logs


class MockDropOp(operations.Operation):
    """
    simulating an action that drops rows
    """

    def __init__(self, from_idx, to_idx):
        self.from_idx = from_idx
        self.to_idx = to_idx

    def __call__(self, action_data):
        return action_data.iloc[self.from_idx: self.to_idx, :], {}


