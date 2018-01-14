from trumania.core import operations


class FakeOp(operations.Operation):
    """
    just returning hard-coded results as output
    """

    def __init__(self, output, logs):
        self.output = output
        self.logs = logs

    def __call__(self, story_data):
        return self.output, self.logs


class FakeRecording(operations.Operation):

    def __init__(self):
        self.last_seen_population_ids = []

    def __call__(self, story_data):
        self.last_seen_population_ids = story_data.index.tolist()
        return story_data, {}

    def reset(self):
        self.last_seen_population_ids = []


class MockDropOp(operations.Operation):
    """
    simulating an story that drops rows
    """

    def __init__(self, from_idx, to_idx):
        self.from_idx = from_idx
        self.to_idx = to_idx

    def __call__(self, story_data):
        return story_data.iloc[self.from_idx: self.to_idx, :], {}
