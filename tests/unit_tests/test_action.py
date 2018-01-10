import pandas as pd
import numpy as np

from trumania.core.operations import Operation
from trumania.core.random_generators import SequencialGenerator, ConstantGenerator, ConstantDependentGenerator
from trumania.core.actor import Actor
from trumania.core.action import Action

from tests.mocks.random_generators import MockTimerGenerator, ConstantsMockGenerator
from tests.mocks.operations import MockDropOp, FakeRecording


def test_empty_action_should_do_nothing_and_not_crash():

    customers = Actor(circus=None, size=1000,
                      ids_gen=SequencialGenerator(prefix="a"))
    empty_action = Action(
        name="purchase",
        initiating_actor=customers,
        actorid_field="AGENT")

    logs = empty_action.execute()

    # no logs should be produced
    assert logs == {}


def test_all_actors_should_be_inactive_when_timers_are_positive():

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )

    assert ([], actor.ids.tolist()) == action.active_inactive_ids()


def test_active_inactive_ids_should_mark_timer_0_as_active():

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([0] * 5 + [1] * 5, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )
    assert (actor.ids[:5].tolist(), actor.ids[5:].tolist()) == action.active_inactive_ids()


def test_active_inactive_ids_should_mark_all_actors_active_when_all_timers_0():

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([0] * 10, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )
    assert (actor.ids.tolist(), []) == action.active_inactive_ids()


def test_get_activity_should_be_default_by_default():

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))
    action = Action(name="tested",
                    initiating_actor=actor,
                    actorid_field="")

    # by default, each actor should be in the default state with activity 1
    assert [1.] * 10 == action.get_param("activity", actor.ids).tolist()
    assert action.get_possible_states() == ["default"]


def test_actors_with_zero_activity_should_never_have_positive_timer():

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    action = Action(
        name="tested",
        initiating_actor=actor,
        # fake generator that assign zero activity to 3 actors
        activity_gen=ConstantsMockGenerator([1, 1, 1, 1, 0, 1, 1, 0, 0, 1]),
        timer_gen=ConstantDependentGenerator(value=10),
        actorid_field="")

    action.reset_timers()

    # all non zero actors should have been through the profiler => timer to 10
    # all others should be locked to -1, to reflect that activity 0 never
    # triggers anything
    expected_timers = [10, 10, 10, 10, -1, 10, 10, -1, -1, 10]

    assert expected_timers == action.timer["remaining"].tolist()


def test_get_activity_should_be_aligned_for_each_state():

    excited_call_activity = ConstantGenerator(value=10)
    back_to_normal_prob = ConstantGenerator(value=.3)

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))
    action = Action(name="tested",
                    initiating_actor=actor,
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

    assert [1, 10] == action.get_param("activity", actor.ids[-2:]).tolist()

    expected_probs = [1, 1, .3, 1, 1, .3, 1, 1, 1, .3]
    assert expected_probs == action.get_param("back_to_default_probability",
                                              actor.ids, ).tolist()


def test_scenario_transiting_to_state_with_0_back_to_default_prob_should_remain_there():
    """
    we create an action with a transit_to_state operation and 0 probability
    of going back to normal => after the execution, all triggered actors should
    still be in that starte
    """

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # here we are saying that some action on actors 5 to 9 is triggering a
    # state change on actors 0 to 4
    active_ids_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + actor.ids[:5].tolist())

    excited_state_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + ["excited"] * 5)

    excited_call_activity = ConstantGenerator(value=10)

    # forcing to stay excited
    back_to_normal_prob = ConstantGenerator(value=0)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",
        states={
         "excited": {
             "activity": excited_call_activity,
             "back_to_default_probability": back_to_normal_prob}
        },

        # forcing the timer of all actors to be initialized to 0
        timer_gen=ConstantDependentGenerator(value=0)
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
    assert logs == {}

    # the first 5 actors should still be in "excited", since
    # "back_to_normal_probability" is 0, the other 5 should not have
    # moved
    expected_state = ["excited"] * 5 + ["default"] * 5
    assert expected_state == action.timer["state"].tolist()


def test_scenario_transiting_to_state_with_1_back_to_default_prob_should_go_back_to_normal():
    """
    similar test to above, though this time we are using
    back_to_normal_prob = 1 => all actors should be back to "normal" state
    at the end of the execution
    """

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # this one is slightly tricky: actors
    active_ids_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + actor.ids[:5].tolist())

    excited_state_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + ["excited"] * 5)

    excited_call_activity = ConstantGenerator(value=10)

    # this time we're forcing to stay in the transited state
    back_to_normal_prob = ConstantGenerator(value=1)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",
        states={
         "excited": {
             "activity": excited_call_activity,
             "back_to_default_probability": back_to_normal_prob}
        },

        # forcing the timer of all actors to be initialized to 0
        timer_gen=ConstantDependentGenerator(value=0)
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
    assert logs == {}

    # this time, all actors should have transited back to "normal" at the end
    print(action.timer["state"].tolist())
    assert ["default"] * 10 == action.timer["state"].tolist()


def test_action_autoreset_true_not_dropping_rows_should_reset_all_timers():
    # in case an action is configured to perform an auto-reset, after one
    # execution,
    #  - all executed rows should have a timer back to some positive value
    #  - all non executed rows should have gone down one tick

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )

    # empty operation list as initialization
    action.set_operations(Operation())

    # initial timers should be those provided by the generator
    assert action.timer["remaining"].equals(init_timers)

    # after one execution, no actor id has been selected and all counters
    # are decreased by 1
    action.execute()
    assert action.timer["remaining"].equals(init_timers - 1)

    # this time, the last 5 should have executed => go back up to 1. The
    # other 5 should now be at 0, ready to execute at next step
    action.execute()
    expected_timers = pd.Series([0] * 5 + [1] * 5, index=actor.ids)
    assert action.timer["remaining"].equals(expected_timers)


def test_action_autoreset_true_and_dropping_rows_should_reset_all_timers():
    # in case an action is configured to perform an auto-reset, but also
    # drops some rows, after one execution,
    #  - all executed rows (dropped or not) should have a timer back to some
    # positive value
    #  - all non executed rows should have gone down one tick

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )

    # simulating an operation that drop the last 2 rows
    action.set_operations(MockDropOp(0, 2))

    # initial timers should be those provided by the generator
    assert action.timer["remaining"].equals(init_timers)

    # after one execution, no actor id has been selected and all counters
    # are decreased by 1
    action.execute()
    assert action.timer["remaining"].equals(init_timers - 1)

    # this time, the last 5 should have executed => and the last 3 of them
    # should have been dropped. Nonetheless, all 5 of them should be back to 1
    action.execute()
    expected_timers = pd.Series([0] * 5 + [1] * 5, index=actor.ids)
    assert action.timer["remaining"].equals(expected_timers)


def test_action_autoreset_false_not_dropping_rows_should_reset_all_timers():
    # in case an action is configured not to perform an auto-reset, after one
    # execution:
    #  - all executed rows should now be at -1
    #  - all non executed rows should have gone down one tick

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=False
    )

    # empty operation list as initialization
    action.set_operations(Operation())

    # we have no auto-reset => all timers should intially be at -1
    all_minus_1 = pd.Series([-1] * 10, index=actor.ids)
    assert action.timer["remaining"].equals(all_minus_1)

    # executing once => should do nothing, and leave all timers at -1
    action.execute()
    assert action.timer["remaining"].equals(all_minus_1)

    # triggering explicitally the action => timers should have the hard-coded
    # values from the mock generator
    action.reset_timers()
    assert action.timer["remaining"].equals(init_timers)

    # after one execution, no actor id has been selected and all counters
    # are decreased by 1
    action.execute()
    assert action.timer["remaining"].equals(init_timers - 1)

    # this time, the last 5 should have executed, but we should not have
    # any timer reste => they should go to -1.
    # The other 5 should now be at 0, ready to execute at next step
    action.execute()
    expected_timers = pd.Series([0] * 5 + [-1] * 5, index=actor.ids)
    assert action.timer["remaining"].equals(expected_timers)

    # executing once more: the previously at -1 should still be there, and the
    # just executed at this stage should be there too
    action.execute()
    expected_timers = pd.Series([-1] * 10, index=actor.ids)
    assert action.timer["remaining"].equals(expected_timers)


def test_action_autoreset_false_and_dropping_rows_should_reset_all_timers():
    # in case an action is configured not to perform an auto-reset, after one
    # execution:
    #  - all executed rows should now be at -1 (dropped or not)
    #  - all non executed rows should have gone down one tick

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=False
    )

    # empty operation list as initialization
    # simulating an operation that drop the last 2 rows
    action.set_operations(MockDropOp(0, 2))

    # we have no auto-reset => all timers should intially be at -1
    all_minus_1 = pd.Series([-1] * 10, index=actor.ids)
    assert action.timer["remaining"].equals(all_minus_1)

    # executing once => should do nothing, and leave all timers at -1
    action.execute()
    assert action.timer["remaining"].equals(all_minus_1)

    # triggering explicitaly the action => timers should have the hard-coded
    # values from the mock generator
    action.reset_timers()
    assert action.timer["remaining"].equals(init_timers)

    # after one execution, no actor id has been selected and all counters
    # are decreased by 1
    action.execute()
    assert action.timer["remaining"].equals(init_timers - 1)

    # this time, the last 5 should have executed, but we should not have
    # any timer reste => they should go to -1.
    # The other 5 should now be at 0, ready to execute at next step
    action.execute()
    expected_timers = pd.Series([0] * 5 + [-1] * 5, index=actor.ids)
    assert action.timer["remaining"].equals(expected_timers)

    # executing once more: the previously at -1 should still be there, and the
    # just executed at this stage should be there too
    action.execute()
    expected_timers = pd.Series([-1] * 10, index=actor.ids)
    assert action.timer["remaining"].equals(expected_timers)


def test_bugfix_collisions_force_act_next():
    # Previously, resetting the timer of reset actors was cancelling the reset.
    #
    # We typically want to reset the timer when we have change the activity
    # state => we want to generate new timer values that reflect the new state.
    #
    # But force_act_next should still have priority on that: if somewhere else
    # we force some actors to act at the next clock step (e.g. to re-try
    # buying an ER or so), the fact that their activity level changed should
    # not cancel the retry.

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen
    )

    timer_values = action.timer["remaining"].copy()

    forced = pd.Index(["ac_1", "ac_3", "ac_7", "ac_8", "ac_9"])
    not_forced = pd.Index(["ac_0", "ac_2", "ac_4", "ac_4", "ac_6"])

    # force_act_next should only impact those ids
    action.force_act_next(forced)
    assert action.timer.loc[forced]["remaining"].tolist() == [0, 0, 0, 0, 0]
    assert action.timer.loc[not_forced]["remaining"].equals(
        timer_values[not_forced]
    )

    # resetting the timers should not change the timers of the actors that
    # are being forced
    action.reset_timers()
    assert action.timer.loc[forced]["remaining"].tolist() == [0, 0, 0, 0, 0]

    # Ticking the timers should not change the timers of the actors that
    # are being forced.
    # This is important for actor forcing themselves to act at the next clock
    # step (typical scenario for retry) => the fact of thick the clock at the
    # end of the execution should not impact them.
    action.timer_tick(actor.ids)
    assert action.timer.loc[forced]["remaining"].tolist() == [0, 0, 0, 0, 0]
    assert action.timer.loc[not_forced]["remaining"].equals(
        timer_values[not_forced] - 1
    )


def test_bugfix_force_actors_should_only_act_once():

    actor = Actor(circus=None, size=10,
                  ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 actors should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [5] * 5, index=actor.ids)
    timers_gen = MockTimerGenerator(init_timers)

    action = Action(
        name="tested",
        initiating_actor=actor,
        actorid_field="ac_id",

        # forcing the timer of all actors to be initialized to 0
        timer_gen=timers_gen)

    recording_op = FakeRecording()
    action.set_operations(recording_op)

    forced = pd.Index(["ac_1", "ac_3", "ac_7", "ac_8", "ac_9"])

    # force_act_next should only impact those ids
    action.force_act_next(forced)

    assert action.timer["remaining"].tolist() == [2, 0, 2, 0, 2, 5, 5, 0, 0, 0]

    action.execute()
    assert recording_op.last_seen_actor_ids == ["ac_1", "ac_3", "ac_7", "ac_8", "ac_9"]
    print(action.timer["remaining"].tolist())
    assert action.timer["remaining"].tolist() == [1, 2, 1, 2, 1, 4, 4, 5, 5, 5]
    recording_op.reset()

    action.execute()
    assert recording_op.last_seen_actor_ids == []
    assert action.timer["remaining"].tolist() == [0, 1, 0, 1, 0, 3, 3, 4, 4, 4]

    action.execute()
    assert recording_op.last_seen_actor_ids == ["ac_0", "ac_2", "ac_4"]
    assert action.timer["remaining"].tolist() == [2, 0, 2, 0, 2, 2, 2, 3, 3, 3]
