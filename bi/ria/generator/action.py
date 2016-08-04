from bi.ria.generator.operations import *
from bi.ria.generator.random_generators import *
from bi.ria.generator.clock import *

class Action(object):
    def __init__(self, name, actor, joined_fields):
        self.name = name
        self.actor = actor
        self.joined_fields = [] if joined_fields is None else joined_fields

    def add_joined_field(self, field_values):

        for joined_now in self.joined_fields:
            actor = joined_now["from_actor"]

            for named_as, actor_field in zip(joined_now["as"], joined_now["select"]):
                actor_ids = field_values[joined_now["left_on"]]
                field_values[named_as] = actor.get_attribute_values(actor_field, actor_ids)

        return field_values


class ActorAction_old(Action):
    def __init__(self, name, actor,
                 actorid_field_name, random_relation_fields,
                 time_generator, activity_generator,
                 joined_fields=None):

        Action.__init__(self, name, actor, joined_fields)

        self.clock = pd.DataFrame({"clock": 0, "activity": 1.},
                                  index=actor.ids)
        self.clock["activity"] = activity_generator.generate(size=len(self.clock.index))
        self.clock["clock"] = time_generator.generate(weights=self.clock["activity"])

        self.time_generator = time_generator
        self.items = {}
        self.random_relation_fields = random_relation_fields
        self.value_conditions = {}
        self.feature_conditions = {}
        self.triggers = {}
        self.impacts = {}
        self.actorid_field_name = actorid_field_name

    def who_acts_now(self):
        """

        :return:
        """
        return self.clock[self.clock["clock"] == 0].index

    def update_clock(self, decrease=1):
        """

        :param decrease:
        :return:
        """
        self.clock["clock"] -= 1

    def set_clock(self, ids, values):
        self.clock.loc[ids, "clock"] = values

    def add_item(self, name, item):
        self.items[name] = item

    def add_impact(self, name, attribute, function, parameters):
        """

        :param name:
        :param attribute:
        :param function:
        :param parameters:
        :return:
        """
        if function == "decrease_stock":
            if not parameters.has_key("recharge_action"):
                raise Exception("no recharge action linked to stock decrease")

        self.impacts[name] = (attribute, function, parameters)

    def add_value_condition(self, name, actorfield, attributefield, function, parameters):
        self.value_conditions[name] = (actorfield, attributefield, function, parameters)

    def add_feature_condition(self, name, actorfield, attributefield, item, function, parameters):
        self.feature_conditions[name] = (actorfield, attributefield, item, function, parameters)

    def __pick_field_values(self, actor_ids):
        """
        Constructs values for all fields produced by this actor action,
        selecting randomly from the "other" side of each declared relationship.

        :param actor_ids: the actor ids being "actioned"
        :return: a dataframe with all fields produced by the action
        """

        def add_field(curr_fields, field_params):

            relationship = field_params["picked_from"]
            join_on = field_params["join_on"]
            field_name = field_params["as"]

            new_field = relationship.select_one(from_ids=curr_fields[join_on],
                                                named_as=field_name)

            merged = pd.merge(left=curr_fields, right=new_field,
                              left_on=join_on, right_on="from")

            return merged.drop("from", axis=1)

        init_fields = pd.DataFrame(actor_ids, columns=[self.actorid_field_name])

        return reduce(add_field, [init_fields] + self.random_relation_fields)

    def check_post_conditions(self, fields_values):
        """
        runs all post-condition checks related to this action on those action
        results.

        :param fields_values:
        :return: the index of actor ids for which post-conditions are not
        violated
        """

        valid_ids = fields_values.index

        for actorf, attrf, func, param in self.value_conditions.values():
            current_actors = fields_values.loc[valid_ids, actorf].values
            validated = self.actor.check_attributes(current_actors, attrf, func, param)
            valid_ids = valid_ids[validated]

        for actorf, attrf, item, func, param in self.feature_conditions.values():
            current_actors = fields_values.loc[valid_ids, actorf].values
            attr_val = self.actor.get_attribute_values(current_actors, attrf)
            validated = self.items[item].check_condition(attr_val, func, param)
            valid_ids = valid_ids[validated]

        return valid_ids

    def make_impacts(self, field_values):
        """

        :param field_values:
        :return:
        """
        for impact_name in self.impacts.keys():

            attribute, function, impact_params = self.impacts[impact_name]

            # TODO there is coupling here between the business scenario
            # we need to externalise this to make the design extensible

            if function == "decrease_stock":
                params = {"values": pd.Series(field_values[impact_params["value"]].values,
                                              index=field_values[self.actorid_field_name])}
                ids_for_clock = self.actor.apply_to_attribute(attribute, function, params)

                # TODO: rename this, something like "set clock to zero" or
                # something
                impact_params["recharge_action"].assign_clock_value(pd.Series(data=0,
                                                                       index=ids_for_clock))

            elif function == "transfer_item":
                params_for_remove = {"items": field_values[impact_params["item"]].values,
                                     "ids": field_values[impact_params["seller_key"]].values}

                params_for_add = {"items": field_values[impact_params["item"]].values,
                                  "ids": field_values[impact_params["buyer_key"]].values}

                impact_params["seller"].apply_to_attribute(attribute,
                                                           "remove_item",
                                                           params_for_remove)

                self.actor.apply_to_attribute(attribute,
                                              "add_item",
                                              params_for_add)

    def execute(self):
        act_now = self.who_acts_now()
        field_values = self.__pick_field_values(act_now)

        if len(field_values.index) > 0:
            passed = self.check_post_conditions(field_values)
            field_values["PASS_CONDITIONS"] = 0
            field_values.loc[passed, "PASS_CONDITIONS"] = 1
            count_passed = len(passed)
            if count_passed > 0:
                self.make_impacts(field_values)

        self.set_clock(act_now, self.time_generator.generate(weights=self.clock.loc[act_now, "activity"]) + 1)
        self.update_clock()

        return Action.add_joined_field(self, field_values)


class ActorAction(object):
    def __init__(self, name, triggering_actor, actorid_field,
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
#            print (" {}: clock_tick: {}" .format(self.name, len(positive_idx)))
            self.clock.loc[positive_idx, "clock"] -= 1

    def force_act_next(self, ids):

        if len(ids) > 0:
#            print (" {}: force_act_next clock: {}".format(self.name, len(ids)))
            self.clock.loc[ids, "clock"] = 0

    def reset_clock(self, ids=None):

        if ids is None:
            ids = self.clock.index

        if len(ids) > 0:
#            print (" {}: reseting clock: {}".format(self.name, len(ids)))
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

#        print "  {}...".format(type(f))
        output, supp_logs = f(prev_output)
#        print "   done".format(type(f))

        # this merges the logs, overwriting any duplicate ids
        all_logs = {k: v for d in [prev_logs, supp_logs] for k, v in d.items()}

        return output, all_logs

    def execute(self):

        # empty dataframe and logs to start with:
        init = [(None, {})]

        _, all_logs = reduce(self._one_execution, init + self.operations)
        self.clock_tick()

        if len(all_logs.keys()) == 0:
            return pd.DataFrame(columns=[])

        if len(all_logs.keys()) > 1:
            # TODO
            raise NotImplemented("not supported yet: circus can only handle "
                                 "one logger per ActorAction")

        # TODO: re-create clock for all those that are now at zero !

        return all_logs.values()[0]

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










