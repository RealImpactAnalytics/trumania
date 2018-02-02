import pandas as pd
import logging
import numpy as np

from trumania.core.operations import SideEffectOnly, Chain
from trumania.core.random_generators import ConstantGenerator, ConstantDependentGenerator, NumpyRandomGenerator
from trumania.core.util_functions import merge_2_dicts


class Story(object):
    def __init__(self, name,
                 initiating_population, member_id_field,
                 activity_gen=ConstantGenerator(value=1.), states=None,
                 timer_gen=ConstantDependentGenerator(value=-1),
                 auto_reset_timer=True):
        """
        :param name: name of this story

        :param initiating_population: population from which the operations of
        this story are started

        :param member_id_field: when building the story data, a field will be
            automatically inserted containing the member ids, with this name

        :param activity_gen: generator for the default activity levels of the
            population members for this story. Default: same level for
            everybody

        :param states: dictionary of states providing activity level for
            other states of the population + a probability level to transit
            back to the default state after each execution (NOT after each clock
            tick). Default: no supplementary states.

        :param timer_gen: timer generator: this must be a generator able to
            generate new timer values based on the activity level. Default:
            no such generator, in which case the timer never triggers this
            story.

        :param auto_reset_timer: if True, we automatically re-schedule a new
            execution for the same member id after at the end of the previous
            ont, by resetting the timer.
        """

        self.name = name
        self.triggering_population = initiating_population
        self.member_id_field = member_id_field
        self.size = initiating_population.size
        self.time_generator = timer_gen
        self.auto_reset_timer = auto_reset_timer
        self.forced_to_act_next = pd.Series()

        # activity and transition probability parameters, for each state
        self.params = pd.DataFrame({("default", "activity"): 0},
                                   index=initiating_population.ids)

        default_state = {"default": {
            "activity": activity_gen,
            "back_to_default_probability": ConstantGenerator(value=1.),
        }}
        for state, state_gens in merge_2_dicts(default_state, states).items():
            activity_vals = state_gens["activity"].generate(size=self.size)
            probs_vals = state_gens["back_to_default_probability"].generate(
                size=self.size)

            self.params[("activity", state)] = activity_vals
            self.params[("back_to_default_probability", state)] = probs_vals

        # current state and timer value for each population member id
        self.timer = pd.DataFrame({"state": "default", "remaining": -1},
                                  index=self.params.index)
        if self.auto_reset_timer:
            self.reset_timers()

        self.ops = self.StoryOps(self)

        # in case self.operations is not called, at least we have a basic
        # selection
        self.operation_chain = Chain()

    def set_operations(self, *ops):
        """
        :param ops: sequence of operations to be executed at each step
        """

        all_ops = list(ops) + [self._MaybeBackToDefault(self)]
        self.operation_chain = Chain(*all_ops)

    def append_operations(self, *ops):
        self.operation_chain.append(*ops)

    def get_param(self, param_name, ids):
        """
        :param param_name: either "activity" or ""back_to_default_probability""
        :param ids: population member ids
        :return: the activity level of each requested member id, depending its
        current state
        """
        if len(self.get_possible_states()) == 1:
            return self.params.loc[ids][(param_name, "default")]
        else:
            # pairs of (member id, state), to select the desired activity level
            param_idx = zip(ids, self.timer.ix[ids, "state"].tolist())
            param_values = self.params.loc[ids][param_name].stack()[param_idx]
            param_values.index = param_values.index.droplevel(level=1)
            return param_values

    def get_possible_states(self):
        return self.params["activity"].columns.tolist()

    def transit_to_state(self, ids, states):
        """
        :param ids: array of population member id to updates
        :param states: array of states to assign to those member ids
        """
        self.timer.loc[ids, "state"] = states

    def active_inactive_ids(self):
        """
        :return: 2 sets of member ids: the one active at this turn and the
        others
        """

        active_idx = self.timer["remaining"] == 0
        return self.timer.index[active_idx].tolist(), self.timer.index[~active_idx].tolist()

    def timer_tick(self, member_ids):

        if self.forced_to_act_next.shape[0] > 0:
            member_ids = pd.Index(member_ids).difference(self.forced_to_act_next)
        impacted_timers = self.timer.loc[member_ids]

        # not updating members that keep a negative counter: those are "marked
        #  inactive" already
        positive_idx = impacted_timers[impacted_timers["remaining"] >= 0].index
        if len(positive_idx) > 0:
            self.timer.loc[positive_idx, "remaining"] -= 1

    def force_act_next(self, ids):
        if len(ids) > 0:
            self.forced_to_act_next = pd.Index(ids).union(self.forced_to_act_next)
            self.timer.loc[ids, "remaining"] = 0

    def reset_timers(self, ids=None):
        """
        Resets the timers to some random positive number of ticks, related to
        the activity level of each population row.

        We limit to a set of ids and not all the members currently set to
        zero, since we could have zero timers as a side effect of other
        storys, in which case we want to trigger an execution at next clock
        tick instead of resetting the timer.

        :param ids: the subset of population member ids to impact
        """

        if ids is None:
            ids = self.timer.index
        else:
            ids = pd.Index(ids)

        # The reset_timers *operation* is called on a member typically to
        # re-generate some timer values because the activity level has
        # changed (e.g. subscribers get "bursty" => their next story should
        # now be earlier than originally timed).
        # BUT: if for some other reason, some (typically other)
        # population member have been forced to act at the next clock step,
        # we don't want to cancel that by resetting their timers to some
        # positive value.
        ids = ids.difference(self.forced_to_act_next)

        if len(ids) > 0:

            activity = self.get_param("activity", ids)
            new_timer = self.time_generator.generate(observations=activity)

            # replacing any generated timer with -1 for fully inactive members
            new_timer = new_timer.where(cond=activity != 0, other=-1)

            self.timer.loc[ids, "remaining"] = new_timer

    @staticmethod
    def init_story_data(member_id_field_name, active_ids):
        """
        creates the initial story_data dataframe containing just the id of
        the currently active members
        """
        return pd.DataFrame({member_id_field_name: active_ids}, index=active_ids)

    def execute(self):

        # Any previously forced storys will now execute => cancelling the flag.
        # Note that some members might put back the flag to themselves during
        # self.operation_chain(ids_df), which is ok and in which case their
        # timer will not be ticked => they will re-execte at the next clock step
        self.forced_to_act_next = pd.Series()

        logging.info(" executing {} ".format(self.name))
        active_ids, inactive_ids = self.active_inactive_ids()

        if len(active_ids) == 0:
            # skips execution altogether if no member has a timer at 0 right now
            all_logs = {}

        else:
            _, all_logs = self.operation_chain(
                Story.init_story_data(self.member_id_field, active_ids))

            if self.auto_reset_timer:
                # re-scheduling those storys one more time
                self.reset_timers(active_ids)
            else:
                # this should set the timer to -1 => will stay there ad vitam
                self.timer_tick(active_ids)

        self.timer_tick(inactive_ids)
        return all_logs

    class _MaybeBackToDefault(SideEffectOnly):
        """
        This is an internal operation of story, that transits members
        back to default with probability as declared in
        back_to_default_probability
        """

        def __init__(self, story):
            self.judge = NumpyRandomGenerator(method="uniform", seed=1234)
            self.story = story

        def side_effect(self, story_data):
            # only transiting members that have ran during this clock tick
            active_timer = self.story.timer.loc[story_data.index]

            non_default_ids = active_timer[
                active_timer["state"] != "default"].index

            if non_default_ids.shape[0] == 0:
                return

            back_prob = self.story.get_param("back_to_default_probability",
                                             non_default_ids)

            if np.all(back_prob == 0):
                cond = [False] * non_default_ids.shape[0]
            elif np.all(back_prob == 1):
                cond = [True] * non_default_ids.shape[0]
            else:
                baseline = self.judge.generate(back_prob.shape[0])
                cond = back_prob > baseline

            member_ids = back_prob[cond].index
            states = ["default"] * member_ids.shape[0]

            self.story.transit_to_state(ids=member_ids, states=states)

    class StoryOps(object):
        class ForceActNext(SideEffectOnly):
            def __init__(self, story, member_id_field, condition_field):
                self.story = story
                self.active_ids_field = member_id_field
                self.condition_field = condition_field

            def side_effect(self, story_data):
                if story_data.shape[0] > 0:
                    # active_ids_field should contain NA: which are all the
                    # members _NOT_ being forced to trigger

                    if self.condition_field is None:
                        filtered = story_data
                    else:
                        condition = story_data[self.condition_field]
                        filtered = story_data.where(condition)

                    ids = filtered[self.active_ids_field].dropna().values

                    self.story.force_act_next(ids)

        def __init__(self, story):
            self.story = story

        def force_act_next(self, member_id_field, condition_field=None):
            """
            Sets the timer of those members to 0, forcing them to act at the
            next clock tick
            """
            return self.ForceActNext(self.story, member_id_field,
                                     condition_field)

        class ResetTimers(SideEffectOnly):
            def __init__(self, story, member_id_field=None):
                self.story = story
                self.member_id_field = member_id_field

            def side_effect(self, story_data):
                if self.member_id_field is None:
                    # no ids specified => resetting everybody
                    self.story.reset_timers(story_data.index)
                else:
                    ids = story_data[self.member_id_field].dropna().unique()
                    self.story.reset_timers(ids)

        def reset_timers(self, member_id_field=None):
            """
            regenerates some random positive count value for all timers
            """
            return self.ResetTimers(self.story, member_id_field)

        class TransitToState(SideEffectOnly):
            def __init__(self, story, member_id_field, state_field, state,
                         condition_field):
                self.story = story
                self.state_field = state_field
                self.state = state
                self.member_id_field = member_id_field
                self.condition_field = condition_field

            def side_effect(self, story_data):

                if self.condition_field is None:
                    filtered = story_data
                else:
                    filtered = story_data[story_data[self.condition_field]]

                if self.state_field is None:
                    member_ids = filtered[self.member_id_field].dropna()
                    states = [self.state] * member_ids.shape[0]

                else:
                    updated = filtered[[self.member_id_field, self.state_field]].dropna()
                    member_ids = updated[self.member_id_field]
                    states = updated[self.state_field].tolist()

                self.story.transit_to_state(ids=member_ids, states=states)

        def transit_to_state(self, member_id_field, state_field=None,
                             state=None, condition_field=None):
            """
            changes the state of those population member ids
            """

            if not ((state_field is None) ^ (state is None)):
                raise ValueError("must provide exactly one of state_field or "
                                 "state")

            return self.TransitToState(self.story, member_id_field,
                                       state_field, state, condition_field)
