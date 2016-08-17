from datagenerator.clock import *
from datagenerator.random_generators import *
from datagenerator import operations
from datagenerator.util_functions import *


class ActorAction(object):
    def __init__(self, name,
                 triggering_actor, actorid_field,
                 activity=ConstantGenerator(value=1.), states=None,
                 timer_gen=ConstantProfiler(-1)):
        """
        :param name: name of this action

        :param triggering_actor: actors from which the operations of this
            action are started

        :param actorid_field: when building the action data, a field will be
            automatically inserted containing the actor id, with this name

        :param activity: generator for the default activity levels of the
            actors for this action. Default: same level for everybody

        :param states: dictionary of states providing activity level for
            other states of the actors + a probability level to transit back to
            the default state after each execution (NOT after each clock
            tick). Default: no supplementary states.

        :param timer_gen: timer generator: this must be a generator able to
            generate new timer values based on the activity level. Default:
            no such generator, in which case the timer never triggers this
            action.
        """

        self.name = name
        self.triggering_actor = triggering_actor
        self.actorid_field_name = actorid_field
        self.size = triggering_actor.size
        self.time_generator = timer_gen

        # activity and transition probability parameters, for each state
        self.params = pd.DataFrame({("default", "activity"): 0},
                                   index=triggering_actor.ids)

        default_state = {"default": {
            "activity": activity,
            "back_to_default_probability": ConstantGenerator(value=1.),
        }}
        for state, state_gens in merge_2_dicts(default_state, states).items():
            activity_vals = state_gens["activity"].generate(size=self.size)
            probs_vals = state_gens["back_to_default_probability"].generate(
                size=self.size)

            self.params[("activity", state)] = activity_vals
            self.params[("back_to_default_probability", state)] = probs_vals

        # current state and timer value for each actor id
        self.timer = pd.DataFrame({"state": "default"}, index=self.params.index)
        self.reset_timers()

        self.ops = self.ActionOps(self)

        # in case self.operations is not called, at least we have a basic
        # selection
        self.operation_chain = operations.Chain(self._WhoActsNow(self))

    def set_operations(self, *ops):
        """
        :param operations: sequence of operations to be executed at each step
        """
        self.operation_chain = operations.Chain(*(
            # the first operation is always a "who acts now"
            [self._WhoActsNow(self)] +
            list(ops) +
            [self.ops.ResetTimers(self), self._MaybeBackToDefault(self)]
        ))

    def get_param(self, param_name, ids):
        """
        :param param_name: either "activity" or ""back_to_default_probability""
        :param ids: actor ids
        :return: the activity level of each requested actor id, depending its
        current state
        """

        # pairs of (actorid, state), to select the appropriate activity level
        activity_idx = zip(ids, self.timer.ix[ids, "state"].tolist())
        activities = self.params.loc[ids][param_name].stack()[activity_idx]
        activities.index = activities.index.droplevel(level=1)
        return activities

    def get_possible_states(self):
        return self.params["activity"].columns.tolist()

    def transit_to_state(self, ids, states):
        """
        :param ids: array of actor id to updates
        :param states: array of states to assign to those actor ids
        """
        self.timer.loc[ids, "state"] = states

    def who_acts_now(self):
        """
        :return: the set of actor id which should be active at this clock tick
        """
        return self.timer[self.timer["remaining"] == 0].index

    def timer_tick(self):

        positive_idx = self.timer[self.timer["remaining"] >= 0].index
        if len(positive_idx) > 0:
            self.timer.loc[positive_idx, "remaining"] -= 1

    def force_act_next(self, ids):
        # TODO: minor collision bug here: in case an actor id is forced to act
        # next AND has been active during the current clock tick, then
        # the reset_elapsed_timers is going to reset the clock anyhow.
        # THis is hopefully rare enough so that we can care about that later...

        if len(ids) > 0:
            self.timer.loc[ids, "remaining"] = 0

    def reset_timers(self, ids=None):
        """
        Resets the timers to some random positive number of ticks, related to
        the activity level of each actor row.

        We limit to a set of ids and not all the actors currently set to
        zero, since we could have zero timers as a side effect of other
        actions, in which case we want to trigger an execution at next clock
        tick instead of resetting the timer.

        :param ids: the subset of actor ids to impact
        """

        if ids is None:
            ids = self.timer.index

        if len(ids) > 0:
            new_timer = self.time_generator.generate(
                weights=self.get_param("activity", ids))

            self.timer.loc[ids, "remaining"] = new_timer

    # @staticmethod
    # def execute_operation((action_data, prev_logs), operation):
    #     """
    #
    #     executes this operation and merges its logs with the previous one
    #     :param operation: the operation to call
    #     :return: the merged action data and logs
    #     """
    #
    #     output, supp_logs = operation(action_data)
    #     # merging the logs of each operation of this action.
    #     # TODO: I guess just adding pd.concat at the end of this would allow
    #     # multiple operations to contribute to the same log => to be checked...
    #     return output, merge_dicts([prev_logs, supp_logs])

    def execute(self):

        # empty dataframe and logs to start with:
#        init = [(None, {})]

        #_, all_logs = reduce(self.execute_operation, init + self.operations)
        _, all_logs = self.operation_chain([(None, {})])
        self.timer_tick()

        if len(all_logs.keys()) == 0:
            return None

        if len(all_logs.keys()) > 1:
            # TODO: add support for more than one log emitting within the action
            raise NotImplemented("not supported yet: circus can only handle "
                                 "one logger per ActorAction")

        return all_logs

    class _WhoActsNow(Operation):
        """
        Initial operation of an Action: creates a basic Dataframe with the
        ids of the actor that are triggered by the clock now
        """
        # TODO: if we remove this action but do this as first step of
        # execute + allow None to be return, clock ticks with no actor
        # executing might be faster

        def __init__(self, action):
            self.action = action

        def transform(self, ignored_input):
            ids = self.action.who_acts_now()

            df = pd.DataFrame(ids, columns=[self.action.actorid_field_name])

            # makes sure the actor id is also kept as index
            df.set_index(self.action.actorid_field_name,
                         drop=False, inplace=True)
            return df

    class _MaybeBackToDefault(SideEffectOnly):
        """
        This is an internal operation of Action, that transits actors
        back to default with probability as declared in
        back_to_default_probability
        """

        def __init__(self, action):
            self.judge = NumpyRandomGenerator(method="uniform")
            self.action = action

        def side_effect(self, action_data):
            # only transiting actors that have ran during this clock tick
            active_timer = self.action.timer.loc[action_data.index]

            non_default_ids = active_timer[
                active_timer["state"] != "default"].index

            if non_default_ids.shape[0] == 0:
                return

            back_prob = self.action.get_param("back_to_default_probability",
                                              non_default_ids)

            baseline = self.judge.generate(back_prob.shape[0])

            actor_ids = back_prob[back_prob > baseline].index
            states = ["default"] * actor_ids.shape[0]

            self.action.transit_to_state(ids=actor_ids, states=states)

    class ActionOps(object):
        class ForceActNext(SideEffectOnly):
            def __init__(self, action, actor_id_field, condition_field):
                self.action = action
                self.active_ids_field = actor_id_field
                self.condition_field = condition_field

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:
                    # active_ids_field should contain NA: which are all the
                    # actior _NOT_ being forced to trigger

                    if self.condition_field is None:
                        filtered = action_data
                    else:
                        condition = action_data[self.condition_field]
                        filtered = action_data.where(condition)

                    ids = filtered[self.active_ids_field].dropna().values
                    self.action.force_act_next(ids)

        def __init__(self, action):
            self.action = action

        def force_act_next(self, actor_id_field, condition_field=None):
            """
            Sets the timer of those actor to 0, forcing them to act at the
            next clock tick
            """
            return self.ForceActNext(self.action, actor_id_field,
                                     condition_field)

        class ResetTimers(SideEffectOnly):
            def __init__(self, action, actor_id_field=None):
                self.action = action
                self.actor_id_field = actor_id_field

            def side_effect(self, action_data):
                if self.actor_id_field is None:
                    # no ids specified => resetting everybody
                    self.action.reset_timers(action_data.index)
                else:
                    ids = action_data[self.actor_id_field].dropna().unique()
                    self.action.reset_timers(ids)

        def reset_timers(self, actor_id_field=None):
            """
            regenerates some random positive count value for all timers
            """
            return self.ResetTimers(self.action, actor_id_field)

        class TransitToState(SideEffectOnly):
            def __init__(self, action, actor_id_field, state_field, state,
                         condition_field):
                self.action = action
                self.state_field = state_field
                self.state=state
                self.actor_id_field = actor_id_field
                self.condition_field = condition_field

            def side_effect(self, action_data):

                if self.condition_field is None:
                    filtered = action_data
                else:
                    filtered = action_data[action_data[self.condition_field]]

                if self.state_field is None:
                    actor_ids = filtered[self.actor_id_field].dropna()
                    states = [self.state] * actor_ids.shape[0]

                else:
                    updated = filtered[[self.actor_id_field, self.state_field]].dropna()
                    actor_ids = updated[self.actor_id_field]
                    states = updated[self.state_field].tolist()

                self.action.transit_to_state(ids=actor_ids, states=states)

        def transit_to_state(self, actor_id_field, state_field=None,
                             state=None, condition_field=None):
            """
            changes the state of those actor ids
            """

            if not ((state_field is None) ^ (state is None)):
                raise ValueError("must provide exactly one of state_field or "
                                 "state")

            return self.TransitToState(self.action, actor_id_field,
                                       state_field, state, condition_field)

