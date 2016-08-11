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
    action = ActorAction(name="tested",
                         triggering_actor=actor,
                         actorid_field="",
                         operations=[])

    # by default, each actor should be in the default state with activity 1
    assert [1.] * 10 == action.get_param("activity", actor.ids).tolist()
    assert action.get_possible_states() == ["normal"]


def test_get_activity_should_be_aligned_for_each_state():

    excited_call_activity = ConstantGenerator(value=10)
    back_to_normal_prob = ConstantGenerator(value=.3)

    actor = Actor(size=10, prefix="ac_", max_length=1)
    action = ActorAction(name="tested",
                         triggering_actor=actor,
                         actorid_field="",
                         states={
                             "excited": {
                                 "activity": excited_call_activity,
                                 "back_to_normal_probability": back_to_normal_prob}
                         },
                         operations=[])

    # by default, each actor should be in the default state with activity 1
    assert [1] * 10 == action.get_param("activity", actor.ids).tolist()
    assert [1] * 10 == action.get_param("back_to_normal_probability",
                                        actor.ids).tolist()
    assert sorted(action.get_possible_states()) == ["excited", "normal"]

    action.transit_to_state(["ac_2", "ac_5", "ac_9"],
                            ["excited", "excited", "excited"])

    # activity and probability of getting back to normal should now be updated
    expected_activity = [1, 1, 10, 1, 1, 10, 1, 1, 1, 10]
    assert expected_activity == action.get_param("activity",
                                                 actor.ids).tolist()
    expected_probs = [1, 1, .3, 1, 1, .3, 1, 1, 1, .3]
    assert expected_probs == action.get_param("back_to_normal_probability",
                                              actor.ids, ).tolist()







