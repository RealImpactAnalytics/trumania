from datagenerator.clock import *
from datagenerator.random_generators import *
from datagenerator.operations import *
from datagenerator.util_functions import merge_dicts


class ActorAction(object):
    def __init__(self, name,
                 triggering_actor, actorid_field,
                 operations,

                 # otherwise specified, all members of this actor have the
                 # same activity
                 activity_gen=GenericGenerator("1", "constant",  {"a": 1.}),

                 # if no time_gen is provided, then the action clock is
                 # maintained at -1 (i.e. never triggering), unless the clock
                 # is reset by some other means
                 time_gen=ConstantProfiler(-1)):

        self.name = name
        self.triggering_actor = triggering_actor
        self.actorid_field_name = actorid_field

        self.time_generator = time_gen
        activity = activity_gen.generate(size=len(triggering_actor.ids))
        self.clock = pd.DataFrame({"activity": activity, "clock": 0},
                                  index=triggering_actor.ids)
        self.reset_clock()

        # the first operation is always a "who acts now" and ends with a
        # clock reset
        self.operations = [self.WhoActsNow(self)] + operations + [self.ResetClock(self)]
        self.ops = self.ActionOps(self)

    def who_acts_now(self):
        """

        :return:
        """

        active_ids = self.clock[self.clock["clock"] == 0].index
#        print (" {}: who_acts_now clock: {}".format(self.name,
        # len(active_ids)))
        return active_ids

    def clock_tick(self):

        positive_idx = self.clock[self.clock["clock"] > 0].index
        if len(positive_idx) > 0:
            self.clock.loc[positive_idx, "clock"] -= 1

    def force_act_next(self, ids):

        if len(ids) > 0:
            self.clock.loc[ids, "clock"] = 0

    def reset_clock(self, ids=None):

        if ids is None:
            ids = self.clock.index

        if len(ids) > 0:
            new_clock = self.time_generator.generate(
                weights=self.clock.loc[ids, "activity"])

            self.clock.loc[ids, "clock"] = new_clock

    class WhoActsNow(Operation):
        """
        Initial operation of an Action: creates a basic Dataframe with the
        ids of the actor that are triggered by the clock now
        """

        def __init__(self, action):
            self.action = action

        def transform(self, ignored_input):
            ids = self.action.who_acts_now()
            df = pd.DataFrame(ids, columns=[self.action.actorid_field_name])

            # makes sure the actor id is also kept as index
            df.set_index(self.action.actorid_field_name,
                         drop=False,
                         inplace=True)
            return df

    class ResetClock(SideEffectOnly):
        """
        """

        def __init__(self, action):
            self.action = action

        def side_effect(self, data):
            self.action.reset_clock(data.index)

    @staticmethod
    def _one_execution((prev_output, prev_logs), f):
        """

        executes this operation and merges its outcome with the previous one

        :param f: the next operation to call on the Action operations list
        :return:


        """

        output, supp_logs = f(prev_output)
        # merging the logs of each operation of this action.
        # TODO: I guess just adding pd.concat at the end of this would allow
        # multiple operations to contribute to the same log => to be checked...
        return output, merge_dicts([prev_logs, supp_logs])

    def execute(self):

        # empty dataframe and logs to start with:
        init = [(None, {})]

        _, all_logs = reduce(self._one_execution, init + self.operations)
        self.clock_tick()

        if len(all_logs.keys()) == 0:
            return pd.DataFrame(columns=[])

        if len(all_logs.keys()) > 1:
            # TODO: add support for more than one log emitting within the action
            raise NotImplemented("not supported yet: circus can only handle "
                                 "one logger per ActorAction")

        return all_logs

    class ActionOps(object):
        def __init__(self, action):
            self.action = action

        class ForceActNext(SideEffectOnly):
            def __init__(self, action, active_ids_field):
                self.action = action
                self.active_ids_field = active_ids_field

            def side_effect(self, data):
                if data.shape[0] > 0:
                    # active_ids_field should contain NA: which are all the
                    # actior _NOT_ being forced to trigger
                    ids = data[self.active_ids_field].dropna().values
                    self.action.force_act_next(ids)

        def force_act_next(self, active_ids_field):
            return self.ForceActNext(self.action, active_ids_field)










