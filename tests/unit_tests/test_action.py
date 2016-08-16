from datagenerator.actor import Actor
from datagenerator.action import ActorAction
from datagenerator.clock import *
from tests.mocks.random_generators import *


def test_empty_action_should_do_nothing_and_not_crash():

    customers = Actor(size=1000)
    empty_action = ActorAction(
        name="purchase",
        triggering_actor=customers,
        actorid_field="AGENT")

    logs = empty_action.execute()

    # no logs should be produced
    assert logs is None


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
                         actorid_field="")

    # by default, each actor should be in the default state with activity 1
    assert [1.] * 10 == action.get_param("activity", actor.ids).tolist()
    assert action.get_possible_states() == ["default"]


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
                                 "back_to_default_probability":
                                     back_to_normal_prob}
                         })

    # by default, each actor should be in the default state with activity 1
    assert [1] * 10 == action.get_param("activity", actor.ids).tolist()
    assert [1] * 10 == action.get_param("back_to_default_probability",
                                        actor.ids).tolist()
    assert sorted(action.get_possible_states()) == ["default", "excited"]

    action.transit_to_state(["ac_2", "ac_5", "ac_9"],
                            ["excited", "excited", "excited"])

    # activity and probability of getting back to normal should now be updated
    expected_activity = [1, 1, 10, 1, 1, 10, 1, 1, 1, 10]
    assert expected_activity == action.get_param("activity",
                                                 actor.ids).tolist()

    # also, doing a get_param for some specific actor ids should return the
    # correct values (was buggy if we requested sth else than the whole list)
    assert expected_activity[2:7] == action.get_param("activity",
                                                      actor.ids[2:7]).tolist()

    assert [1, 10] == action.get_param("activity",actor.ids[-2:]).tolist()

    expected_probs = [1, 1, .3, 1, 1, .3, 1, 1, 1, .3]
    assert expected_probs == action.get_param("back_to_default_probability",
                                              actor.ids, ).tolist()


def test_scenario_transiting_to_state_should_remain_there():
    """
    we create an action with a transit_to_state operation and 0 probability
    of going back to normal => after the execution, all triggered actors should
    still be in that starte
    """

    actor = Actor(size=10, prefix="ac_", max_length=1)

    # here we are saying that some action on actors 5 to 9 is triggering a
    # state change on actors 0 to 4
    active_ids_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + actor.ids[:5])

    excited_state_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + ["excited"] * 5)

    excited_call_activity = ConstantGenerator(value=10)

    # forcing to stay excited
    back_to_normal_prob = ConstantGenerator(value=0)

    action = ActorAction(
        name="tested",
        triggering_actor=actor,
        actorid_field="ac_id",
        states={
         "excited": {
             "activity": excited_call_activity,
             "back_to_default_probability": back_to_normal_prob}
        },

        # forcing the timer of all actors to be initialized to 0
        timer_gen=ConstantProfiler(0)
    )

    action.set_operations(
        # first 5 actor are "active"
        active_ids_gens.ops.generate(named_as="active_ids"),
        excited_state_gens.ops.generate(named_as="new_state"),

        # forcing a transition to "excited" state of the 5 actors
        action.ops.transit_to_state(actor_id_field="active_ids",
                                    state_field="new_state")
    )

    # before any execution, the state should be default for all
    assert ["default"] * 10 == action.timer["state"].tolist()

    logs = action.execute()

    # no logs are expected as output
    assert logs is None

    # the first 5 actors should still be in "excited", since
    # "back_to_normal_probability" is 0, the other 5 should not have
    # moved
    expected_state = ["excited"] * 5 + ["default"] * 5
    assert expected_state == action.timer["state"].tolist()


def test_scenario_transiting_to_state_should_go_back_to_normal():
    """
    similar test to above, though this time we are using
    back_to_normal_prob = 1 => all actors should be back to "normal" state
    at the end of the execution
    """

    actor = Actor(size=10, prefix="ac_", max_length=1)

    # this one is slightly tricky: actors
    active_ids_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + actor.ids[:5])

    excited_state_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + ["excited"] * 5)

    excited_call_activity = ConstantGenerator(value=10)

    # this time we're forcing to stay in the transited state
    back_to_normal_prob = ConstantGenerator(value=1)

    action = ActorAction(
        name="tested",
        triggering_actor=actor,
        actorid_field="ac_id",
        states={
         "excited": {
             "activity": excited_call_activity,
             "back_to_default_probability": back_to_normal_prob}
        },

        # forcing the timer of all actors to be initialized to 0
        timer_gen=ConstantProfiler(0)
    )

    action.set_operations(
        # first 5 actor are "active"
        active_ids_gens.ops.generate(named_as="active_ids"),
        excited_state_gens.ops.generate(named_as="new_state"),

        # forcing a transition to "excited" state of the 5 actors
        action.ops.transit_to_state(actor_id_field="active_ids",
                                    state_field="new_state")
    )

    # before any execution, the state should be default for all
    assert ["default"] * 10 == action.timer["state"].tolist()

    logs = action.execute()

    # no logs are expected as output
    assert logs is None

    # this time, all actors should have transited back to "normal" at the end
    print action.timer["state"].tolist()
    assert ["default"] * 10 == action.timer["state"].tolist()

