import pandas as pd
import numpy as np

from trumania.core.operations import Operation
from trumania.core.random_generators import SequencialGenerator, ConstantGenerator, ConstantDependentGenerator
from trumania.core.population import Population
from trumania.core.story import Story

from tests.mocks.random_generators import MockTimerGenerator, ConstantsMockGenerator
from tests.mocks.operations import MockDropOp, FakeRecording


def test_empty_story_should_do_nothing_and_not_crash():

    customers = Population(circus=None, size=1000,
                           ids_gen=SequencialGenerator(prefix="a"))
    empty_story = Story(
        name="purchase",
        initiating_population=customers,
        member_id_field="AGENT")

    logs = empty_story.execute()

    # no logs should be produced
    assert logs == {}


def test_all_populations_should_be_inactive_when_timers_are_positive():
    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )

    assert ([], population.ids.tolist()) == story.active_inactive_ids()


def test_active_inactive_ids_should_mark_timer_0_as_active():
    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([0] * 5 + [1] * 5, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )
    assert (population.ids[:5].tolist(), population.ids[5:].tolist()) == story.active_inactive_ids()


def test_active_inactive_ids_should_mark_all_populations_active_when_all_timers_0():
    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([0] * 10, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )
    assert (population.ids.tolist(), []) == story.active_inactive_ids()


def test_get_activity_should_be_default_by_default():

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))
    story = Story(name="tested", initiating_population=population,
                  member_id_field="")

    # by default, each population should be in the default state with activity 1
    assert [1.] * 10 == story.get_param("activity", population.ids).tolist()
    assert story.get_possible_states() == ["default"]


def test_populations_with_zero_activity_should_never_have_positive_timer():

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    story = Story(
        name="tested",
        initiating_population=population,
        # fake generator that assign zero activity to 3 populations
        activity_gen=ConstantsMockGenerator([1, 1, 1, 1, 0, 1, 1, 0, 0, 1]),
        timer_gen=ConstantDependentGenerator(value=10),
        member_id_field="")

    story.reset_timers()

    # all non zero populations should have been through the profiler => timer to 10
    # all others should be locked to -1, to reflect that activity 0 never
    # triggers anything
    expected_timers = [10, 10, 10, 10, -1, 10, 10, -1, -1, 10]

    assert expected_timers == story.timer["remaining"].tolist()


def test_get_activity_should_be_aligned_for_each_state():

    excited_call_activity = ConstantGenerator(value=10)
    back_to_normal_prob = ConstantGenerator(value=.3)

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))
    story = Story(name="tested", initiating_population=population,
                  member_id_field="",
                  states={
                             "excited": {
                                 "activity": excited_call_activity,
                                 "back_to_default_probability":
                                     back_to_normal_prob}
                         })

    # by default, each population should be in the default state with activity 1
    assert [1] * 10 == story.get_param("activity", population.ids).tolist()
    assert [1] * 10 == story.get_param("back_to_default_probability",
                                       population.ids).tolist()
    assert sorted(story.get_possible_states()) == ["default", "excited"]

    story.transit_to_state(["ac_2", "ac_5", "ac_9"],
                           ["excited", "excited", "excited"])

    # activity and probability of getting back to normal should now be updated
    expected_activity = [1, 1, 10, 1, 1, 10, 1, 1, 1, 10]
    assert expected_activity == story.get_param("activity",
                                                population.ids).tolist()

    # also, doing a get_param for some specific population ids should return the
    # correct values (was buggy if we requested sth else than the whole list)
    assert expected_activity[2:7] == story.get_param("activity",
                                                     population.ids[2:7]).tolist()

    assert [1, 10] == story.get_param("activity", population.ids[-2:]).tolist()

    expected_probs = [1, 1, .3, 1, 1, .3, 1, 1, 1, .3]
    assert expected_probs == story.get_param("back_to_default_probability",
                                             population.ids, ).tolist()


def test_scenario_transiting_to_state_with_0_back_to_default_prob_should_remain_there():
    """
    we create an story with a transit_to_state operation and 0 probability
    of going back to normal => after the execution, all triggered populations should
    still be in that starte
    """

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # here we are saying that some story on populations 5 to 9 is triggering a
    # state change on populations 0 to 4
    active_ids_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + population.ids[:5].tolist())

    excited_state_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + ["excited"] * 5)

    excited_call_activity = ConstantGenerator(value=10)

    # forcing to stay excited
    back_to_normal_prob = ConstantGenerator(value=0)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",
        states={
         "excited": {
             "activity": excited_call_activity,
             "back_to_default_probability": back_to_normal_prob}
        },

        # forcing the timer of all populations to be initialized to 0
        timer_gen=ConstantDependentGenerator(value=0)
    )

    story.set_operations(
        # first 5 population are "active"
        active_ids_gens.ops.generate(named_as="active_ids"),
        excited_state_gens.ops.generate(named_as="new_state"),

        # forcing a transition to "excited" state of the 5 populations
        story.ops.transit_to_state(member_id_field="active_ids",
                                   state_field="new_state")
    )

    # before any execution, the state should be default for all
    assert ["default"] * 10 == story.timer["state"].tolist()

    logs = story.execute()

    # no logs are expected as output
    assert logs == {}

    # the first 5 populations should still be in "excited", since
    # "back_to_normal_probability" is 0, the other 5 should not have
    # moved
    expected_state = ["excited"] * 5 + ["default"] * 5
    assert expected_state == story.timer["state"].tolist()


def test_scenario_transiting_to_state_with_1_back_to_default_prob_should_go_back_to_normal():
    """
    similar test to above, though this time we are using
    back_to_normal_prob = 1 => all populations should be back to "normal" state
    at the end of the execution
    """

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # this one is slightly tricky: populations
    active_ids_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + population.ids[:5].tolist())

    excited_state_gens = ConstantsMockGenerator(
        values=[np.nan] * 5 + ["excited"] * 5)

    excited_call_activity = ConstantGenerator(value=10)

    # this time we're forcing to stay in the transited state
    back_to_normal_prob = ConstantGenerator(value=1)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",
        states={
         "excited": {
             "activity": excited_call_activity,
             "back_to_default_probability": back_to_normal_prob}
        },

        # forcing the timer of all populations to be initialized to 0
        timer_gen=ConstantDependentGenerator(value=0)
    )

    story.set_operations(
        # first 5 population are "active"
        active_ids_gens.ops.generate(named_as="active_ids"),
        excited_state_gens.ops.generate(named_as="new_state"),

        # forcing a transition to "excited" state of the 5 populations
        story.ops.transit_to_state(member_id_field="active_ids",
                                   state_field="new_state")
    )

    # before any execution, the state should be default for all
    assert ["default"] * 10 == story.timer["state"].tolist()

    logs = story.execute()

    # no logs are expected as output
    assert logs == {}

    # this time, all populations should have transited back to "normal" at the end
    print(story.timer["state"].tolist())
    assert ["default"] * 10 == story.timer["state"].tolist()


def test_story_autoreset_true_not_dropping_rows_should_reset_all_timers():
    # in case an story is configured to perform an auto-reset, after one
    # execution,
    #  - all executed rows should have a timer back to some positive value
    #  - all non executed rows should have gone down one tick

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )

    # empty operation list as initialization
    story.set_operations(Operation())

    # initial timers should be those provided by the generator
    assert story.timer["remaining"].equals(init_timers)

    # after one execution, no population id has been selected and all counters
    # are decreased by 1
    story.execute()
    assert story.timer["remaining"].equals(init_timers - 1)

    # this time, the last 5 should have executed => go back up to 1. The
    # other 5 should now be at 0, ready to execute at next step
    story.execute()
    expected_timers = pd.Series([0] * 5 + [1] * 5, index=population.ids)
    assert story.timer["remaining"].equals(expected_timers)


def test_story_autoreset_true_and_dropping_rows_should_reset_all_timers():
    # in case an story is configured to perform an auto-reset, but also
    # drops some rows, after one execution,
    #  - all executed rows (dropped or not) should have a timer back to some
    # positive value
    #  - all non executed rows should have gone down one tick

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=True
    )

    # simulating an operation that drop the last 2 rows
    story.set_operations(MockDropOp(0, 2))

    # initial timers should be those provided by the generator
    assert story.timer["remaining"].equals(init_timers)

    # after one execution, no population id has been selected and all counters
    # are decreased by 1
    story.execute()
    assert story.timer["remaining"].equals(init_timers - 1)

    # this time, the last 5 should have executed => and the last 3 of them
    # should have been dropped. Nonetheless, all 5 of them should be back to 1
    story.execute()
    expected_timers = pd.Series([0] * 5 + [1] * 5, index=population.ids)
    assert story.timer["remaining"].equals(expected_timers)


def test_story_autoreset_false_not_dropping_rows_should_reset_all_timers():
    # in case an story is configured not to perform an auto-reset, after one
    # execution:
    #  - all executed rows should now be at -1
    #  - all non executed rows should have gone down one tick

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=False
    )

    # empty operation list as initialization
    story.set_operations(Operation())

    # we have no auto-reset => all timers should intially be at -1
    all_minus_1 = pd.Series([-1] * 10, index=population.ids)
    assert story.timer["remaining"].equals(all_minus_1)

    # executing once => should do nothing, and leave all timers at -1
    story.execute()
    assert story.timer["remaining"].equals(all_minus_1)

    # triggering explicitally the story => timers should have the hard-coded
    # values from the mock generator
    story.reset_timers()
    assert story.timer["remaining"].equals(init_timers)

    # after one execution, no population id has been selected and all counters
    # are decreased by 1
    story.execute()
    assert story.timer["remaining"].equals(init_timers - 1)

    # this time, the last 5 should have executed, but we should not have
    # any timer reste => they should go to -1.
    # The other 5 should now be at 0, ready to execute at next step
    story.execute()
    expected_timers = pd.Series([0] * 5 + [-1] * 5, index=population.ids)
    assert story.timer["remaining"].equals(expected_timers)

    # executing once more: the previously at -1 should still be there, and the
    # just executed at this stage should be there too
    story.execute()
    expected_timers = pd.Series([-1] * 10, index=population.ids)
    assert story.timer["remaining"].equals(expected_timers)


def test_story_autoreset_false_and_dropping_rows_should_reset_all_timers():
    # in case an story is configured not to perform an auto-reset, after one
    # execution:
    #  - all executed rows should now be at -1 (dropped or not)
    #  - all non executed rows should have gone down one tick

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen,
        auto_reset_timer=False
    )

    # empty operation list as initialization
    # simulating an operation that drop the last 2 rows
    story.set_operations(MockDropOp(0, 2))

    # we have no auto-reset => all timers should intially be at -1
    all_minus_1 = pd.Series([-1] * 10, index=population.ids)
    assert story.timer["remaining"].equals(all_minus_1)

    # executing once => should do nothing, and leave all timers at -1
    story.execute()
    assert story.timer["remaining"].equals(all_minus_1)

    # triggering explicitaly the story => timers should have the hard-coded
    # values from the mock generator
    story.reset_timers()
    assert story.timer["remaining"].equals(init_timers)

    # after one execution, no population id has been selected and all counters
    # are decreased by 1
    story.execute()
    assert story.timer["remaining"].equals(init_timers - 1)

    # this time, the last 5 should have executed, but we should not have
    # any timer reste => they should go to -1.
    # The other 5 should now be at 0, ready to execute at next step
    story.execute()
    expected_timers = pd.Series([0] * 5 + [-1] * 5, index=population.ids)
    assert story.timer["remaining"].equals(expected_timers)

    # executing once more: the previously at -1 should still be there, and the
    # just executed at this stage should be there too
    story.execute()
    expected_timers = pd.Series([-1] * 10, index=population.ids)
    assert story.timer["remaining"].equals(expected_timers)


def test_bugfix_collisions_force_act_next():
    # Previously, resetting the timer of reset populations was cancelling the reset.
    #
    # We typically want to reset the timer when we have change the activity
    # state => we want to generate new timer values that reflect the new state.
    #
    # But force_act_next should still have priority on that: if somewhere else
    # we force some populations to act at the next clock step (e.g. to re-try
    # buying an ER or so), the fact that their activity level changed should
    # not cancel the retry.

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [1] * 5, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen
    )

    timer_values = story.timer["remaining"].copy()

    forced = pd.Index(["ac_1", "ac_3", "ac_7", "ac_8", "ac_9"])
    not_forced = pd.Index(["ac_0", "ac_2", "ac_4", "ac_4", "ac_6"])

    # force_act_next should only impact those ids
    story.force_act_next(forced)
    assert story.timer.loc[forced]["remaining"].tolist() == [0, 0, 0, 0, 0]
    assert story.timer.loc[not_forced]["remaining"].equals(
        timer_values[not_forced]
    )

    # resetting the timers should not change the timers of the populations that
    # are being forced
    story.reset_timers()
    assert story.timer.loc[forced]["remaining"].tolist() == [0, 0, 0, 0, 0]

    # Ticking the timers should not change the timers of the populations that
    # are being forced.
    # This is important for population forcing themselves to act at the next
    # clock
    # step (typical scenario for retry) => the fact of thick the clock at the
    # end of the execution should not impact them.
    story.timer_tick(population.ids)
    assert story.timer.loc[forced]["remaining"].tolist() == [0, 0, 0, 0, 0]
    assert story.timer.loc[not_forced]["remaining"].equals(
        timer_values[not_forced] - 1
    )


def test_bugfix_force_populations_should_only_act_once():

    population = Population(circus=None, size=10,
                            ids_gen=SequencialGenerator(prefix="ac_", max_length=1))

    # 5 populations should trigger in 2 ticks, and 5 more
    init_timers = pd.Series([2] * 5 + [5] * 5, index=population.ids)
    timers_gen = MockTimerGenerator(init_timers)

    story = Story(
        name="tested",
        initiating_population=population,
        member_id_field="ac_id",

        # forcing the timer of all populations to be initialized to 0
        timer_gen=timers_gen)

    recording_op = FakeRecording()
    story.set_operations(recording_op)

    forced = pd.Index(["ac_1", "ac_3", "ac_7", "ac_8", "ac_9"])

    # force_act_next should only impact those ids
    story.force_act_next(forced)

    assert story.timer["remaining"].tolist() == [2, 0, 2, 0, 2, 5, 5, 0, 0, 0]

    story.execute()
    assert recording_op.last_seen_population_ids == ["ac_1", "ac_3", "ac_7", "ac_8", "ac_9"]
    print(story.timer["remaining"].tolist())
    assert story.timer["remaining"].tolist() == [1, 2, 1, 2, 1, 4, 4, 5, 5, 5]
    recording_op.reset()

    story.execute()
    assert recording_op.last_seen_population_ids == []
    assert story.timer["remaining"].tolist() == [0, 1, 0, 1, 0, 3, 3, 4, 4, 4]

    story.execute()
    assert recording_op.last_seen_population_ids == ["ac_0", "ac_2", "ac_4"]
    assert story.timer["remaining"].tolist() == [2, 0, 2, 0, 2, 2, 2, 3, 3, 3]
