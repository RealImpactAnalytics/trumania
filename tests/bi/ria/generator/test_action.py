from bi.ria.generator.action import ActorAction
from bi.ria.generator.operations import Operation

import pandas as pd
import numpy as np


def test_one_execution_should_merge_empty_data_correctly():

    # empty previous
    prev_df = pd.DataFrame(columns=[])
    prev_log = {}
    nop = Operation()

    output, logs = ActorAction._one_execution((prev_df, prev_log), nop)

    assert logs == {}
    assert output.equals(prev_df)


class FakeOp(Operation):

    def __init__(self, output, logs):
        self.output = output
        self.logs = logs

    def apply(self, data):
        return self.output, self.logs


def test_one_execution_should_merge_one_op_with_nothing_into_one_result():

    # empty previous
    prev = pd.DataFrame(columns=[]), {}

    cdrs = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    input = pd.DataFrame(np.random.rand(10, 2), columns=["C", "D"])
    op = FakeOp(input, logs={"cdrs": cdrs})

    output, logs = ActorAction._one_execution(prev, op)

    assert logs == {"cdrs": cdrs}
    assert input.equals(output)


def test_one_execution_should_merge_2_ops_correctly():

    # previous results
    init = pd.DataFrame(columns=[])
    mobility_logs = pd.DataFrame(np.random.rand(12, 3),
                                 columns=["A", "CELL", "duration"])

    cdrs = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    input = pd.DataFrame(np.random.rand(10, 2), columns=["C", "D"])
    op = FakeOp(input, {"cdrs" :cdrs})

    output, logs = ActorAction._one_execution((init,
                                               {"mobility": mobility_logs}),
                                              op)

    assert logs == {"cdrs": cdrs, "mobility": mobility_logs}
    assert input.equals(output)
