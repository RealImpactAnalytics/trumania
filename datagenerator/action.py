from datagenerator.clock import *
from datagenerator.random_generators import *
from datagenerator import operations
from datagenerator.util_functions import *


class Action(object):
    def __init__(self, name,
                 triggering_actor, actorid_field,
                 activity=ConstantGenerator(value=1.), states=None,
                 timer_gen=ConstantProfiler(-1),
                 auto_reset_timer=True):
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

        :param auto_reset_timer: if True, we automatically re-schedule a new
            execution for the same actor id after at the end of the previous
            ont, by resetting the timer.
        """

        self.name = name
        self.triggering_actor = triggering_actor
        self.actorid_field_name = actorid_field
        self.size = triggering_actor.size
        self.time_generator = timer_gen
        self.auto_reset_timer = auto_reset_timer

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
        self.timer = pd.DataFrame({"state": "default", "remaining": -1},
                                  index=self.params.index)
        if self.auto_reset_timer:
            self.reset_timers()

        self.ops = self.ActionOps(self)

        # in case self.operations is not called, at least we have a basic
        # selection
        self.operation_chain = operations.Chain()

    def set_operations(self, *ops):
        """
        :param operations: sequence of operations to be executed at each step
        """

        all_ops = list(ops)
        if self.auto_reset_timer:
            all_ops += [self.ops.ResetTimers(self)]
        all_ops += [self._MaybeBackToDefault(self)]

        self.operation_chain = operations.Chain(*all_ops)

    def get_param(self, param_name, ids):
        """
        :param param_name: either "activity" or ""back_to_default_probability""
        :param ids: actor ids
        :return: the activity level of each requested actor id, depending its
        current state
        """
        if len(self.get_possible_states()) == 1:
            return self.params.loc[ids][(param_name, "default")]
        else:
            # pairs of (actorid, state), to select the appropriate activity level
            param_idx = zip(ids, self.timer.ix[ids, "state"].tolist())
            param_values = self.params.loc[ids][param_name].stack()[param_idx]
            param_values.index = param_values.index.droplevel(level=1)
            return param_values

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

        # not updating actors that keep a negative counter: those are "marked
        #  inactive" already
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
            activity = self.get_param("activity", ids)

            new_timer = self.time_generator.generate(weights=activity)

            # replacing any generated timer with -1 for fully inactive actors
            new_timer = new_timer.where(cond=activity, other=-1)

            self.timer.loc[ids, "remaining"] = new_timer

    def execute(self):

        logging.info(" executing {} action ".format(self.name))
        ids = self.who_acts_now()

        if ids.shape[0] == 0:
            all_logs = None

        else:
            ids_df = pd.DataFrame({self.actorid_field_name: ids}, index=ids)
            _, all_logs = self.operation_chain(ids_df)

        self.timer_tick()
        return all_logs

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

            if np.all(back_prob == 0):
                cond = [False] * non_default_ids.shape[0]
            elif np.all(back_prob == 1):
                cond = [True] * non_default_ids.shape[0]
            else:
                baseline = self.judge.generate(back_prob.shape[0])
                cond = back_prob > baseline

            actor_ids = back_prob[cond].index
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

