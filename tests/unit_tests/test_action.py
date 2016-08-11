import numpy as np
import pandas as pd
from datagenerator.actor import Actor
from datagenerator.action import ActorAction
from datagenerator.operations import Operation
from datagenerator.random_generators import *


def test_one_execution_should_merge_empty_data_correctly():

    # empty previous
    prev_df = pd.DataFrame(columns=[])
    prev_log = {}
    nop = Operation()

    output, logs = ActorAction.execute_operation((prev_df, prev_log), nop)

    assert logs == {}
    assert output.equals(prev_df)


class FakeOp(Operation):

    def __init__(self, output, logs):
        self.output = output
        self.logs = logs

    def __call__(self, data):
        return self.output, self.logs


def test_one_execution_should_merge_one_op_with_nothing_into_one_result():

    # empty previous
    prev = pd.DataFrame(columns=[]), {}

    cdrs = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    input = pd.DataFrame(np.random.rand(10, 2), columns=["C", "D"])
    op = FakeOp(input, logs={"cdrs": cdrs})

    output, logs = ActorAction.execute_operation(prev, op)

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

    output, logs = ActorAction.execute_operation((init,
                                                  {"mobility": mobility_logs}),
                                                 op)

    assert logs == {"cdrs": cdrs, "mobility": mobility_logs}
    assert input.equals(output)


def test_get_activity_default():

    actor = Actor(size=10)
    default_action = ActorAction(name="tested",
                         triggering_actor=actor,
                         actorid_field="",
                         operations=[])

    # by default, each actor should be in the default state with activity 1
    assert [1.] * 10 == default_action._get_activity(actor.ids).tolist()
    assert default_action.get_possible_states() == ["normal"]


def test_get_activity_default_not_impacted_by_other_states():

    excited_call_activity = ScaledParetoGenerator(m=2, a=1.4,
                                                 seed=1234)

    back_to_normal_prob = NumpyRandomGenerator(method="beta",
                                               a=7, b=3)

    actor = Actor(size=10)
    default_action = ActorAction(name="tested",
                         triggering_actor=actor,
                         actorid_field="",
                         states={
                           "excited": {
                             "activity": excited_call_activity,
                             "back_to_normal_probability": back_to_normal_prob}
                         },
                         operations=[])

    # by default, each actor should be in the default state with activity 1
    assert [1.] * 10 == default_action._get_activity(actor.ids).tolist()
    assert sorted(default_action.get_possible_states()) == ["excited", "normal"]






