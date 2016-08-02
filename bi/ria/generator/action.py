import pandas as pd


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


class ActorAction(Action):
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


class AttributeAction(Action):
    def __init__(self, name, actor, attr_name, actorid_field_name,
                 activity_generator, time_generator,
                 parameters, joined_fields=None):
        Action.__init__(self, name, actor, joined_fields)

        self.attr_name = attr_name
        self.parameters = parameters
        self.time_generator = time_generator
        self.actorid_field_name = actorid_field_name

        self.clock = pd.DataFrame({"clock": 0, "activity": 1.}, index=actor.ids)
        self.clock["activity"] = activity_generator.generate(size=len(self.clock.index))
        self.clock["clock"] = self.time_generator.generate(weights=self.clock["activity"])

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

    def assign_clock_value(self,values):
        self.clock.loc[values.index,"clock"] = values.values

    def execute(self):
        ids, out, values = self.actor.make_attribute_action(self.attr_name,
                                                            self.actorid_field_name,
                                                            self.who_acts_now(),
                                                            self.parameters)

        if len(ids) > 0:
            if values is None:
                self.clock.loc[ids, "clock"] = self.time_generator.generate(weights=self.clock.loc[ids, "activity"])+1
            else:
                self.clock.loc[ids, "clock"] = self.time_generator.generate(weights=values)+1
        self.update_clock()

        return Action.add_joined_field(self, out)
