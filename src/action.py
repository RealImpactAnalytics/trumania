import numpy as np
import pandas as pd


class ActorAction(object):
    def __init__(self, name, actor, time_generator, activity_generator):
        self.name = name
        self.main_actor = actor

        self.clock = pd.DataFrame({"clock": 0, "activity": 1.}, index=actor.get_ids())
        self.clock["activity"] = activity_generator.generate(size=len(self.clock.index))
        self.clock["clock"] = time_generator.generate(weights=self.clock["activity"])

        self.time_generator = time_generator
        self.secondary_actors = {}
        self.relationships = {}
        self.items = {}
        self.base_fields = {}
        self.secondary_fields = {}
        self.value_conditions = {}
        self.feature_conditions = {}
        self.triggers = {}
        self.impacts = {}

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

    def add_secondary_actor(self, name, actor):
        self.secondary_actors[name] = actor

    def add_relationship(self, name, relationship):
        self.relationships[name] = relationship

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

    def add_field(self, name, relationship, params=None):
        """

        :param name:
        :type relationship: str
        :param relationship: name of relationship to use (as named in the "relationship" field of the action)
        :param params:
        :return:
        """
        self.base_fields[name] = (relationship, params)

    def add_secondary_field(self, name, relationship, params=None):
        """

        :param name:
        :type relationship: str
        :param relationship: name of relationship to use (as named in the "relationship" field of the action)
        :param params:
        :return:
        """
        self.secondary_fields[name] = (relationship, params)

    def add_value_condition(self, name, actorfield, attributefield, function, parameters):
        self.value_conditions[name] = (actorfield, attributefield, function, parameters)

    def add_feature_condition(self, name, actorfield, attributefield, item, function, parameters):
        self.feature_conditions[name] = (actorfield, attributefield, item, function, parameters)

    def get_field_data(self, ids):
        """
        Constructs values for all fields produced by this action, selecting
        randomly from the "other" side of each declared relationship.

        :param ids: the actor ids being "actioned"
        :return: a dataframe with all fields produced by the action
        """

        f_data = []
        # TODO there's something weird here: if only 1 field is returned, we would maybe like to have f to be the name of the field
        for f_name, (rel_name, rel_parameters) in self.base_fields.items():
            f_data.append(self.relationships[rel_name].select_one(
                rel_parameters["key"], ids))

        all_fields = pd.concat(f_data, axis=1, join='inner').reset_index()

        for f_name, (rel_name, rel_parameters) in self.secondary_fields.items():
            out = self.relationships[rel_name].select_one(rel_parameters["key_rel"],
                                                          all_fields[rel_parameters["key_table"]].values)
            all_fields = pd.merge(all_fields, out.rename(columns={rel_parameters["out_rel"]: f_name}),
                                  left_on=rel_parameters["key_table"],
                                  right_index=True)

        return all_fields

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
            validated = self.main_actor.check_attributes(current_actors, attrf, func, param)
            valid_ids = valid_ids[validated]

        for actorf, attrf, item, func, param in self.feature_conditions.values():
            current_actors = fields_values.loc[valid_ids, actorf].values
            attr_val = self.main_actor.get_join(current_actors, attrf)
            validated = self.items[item].check_condition(attr_val, func, param)
            valid_ids = valid_ids[validated]

        return valid_ids

    def make_impacts(self, data):
        """

        :param data:
        :return:
        """
        for k in self.impacts.keys():
            if self.impacts[k][1] == "decrease_stock":
                params = {"values": pd.Series(data[self.impacts[k][2]["value"]].values,
                                              index=data[self.impacts[k][2]["key"]].values)}
                ids_for_clock = self.main_actor.apply_to_attribute(self.impacts[k][0], self.impacts[k][1], params)
                self.impacts[k][2]["recharge_action"].assign_clock_value(pd.Series(data=0,index=ids_for_clock))

            if self.impacts[k][1] == "transfer_item":
                params_for_remove = {"items": data[self.impacts[k][2]["item"]].values,
                                     "ids": data[self.impacts[k][2]["seller_key"]].values}

                params_for_add = {"items": data[self.impacts[k][2]["item"]].values,
                                  "ids": data[self.impacts[k][2]["buyer_key"]].values}

                self.secondary_actors[self.impacts[k][2]["seller_table"]].apply_to_attribute(self.impacts[k][0],
                                                                                             "remove_item",
                                                                                             params_for_remove)

                self.main_actor.apply_to_attribute(self.impacts[k][0], "add_item", params_for_add)

    def execute(self):
        act_now = self.who_acts_now()
        field_values = self.get_field_data(act_now.values)

        if len(field_values.index) > 0:
            passed = self.check_post_conditions(field_values)
            field_values["PASS_CONDITIONS"] = 0
            field_values.loc[passed, "PASS_CONDITIONS"] = 1
            count_passed = len(passed)
            if count_passed > 0:
                self.make_impacts(field_values)

        self.set_clock(act_now, self.time_generator.generate(weights=self.clock.loc[act_now, "activity"]) + 1)
        self.update_clock()

        return field_values


class AttributeAction(object):
    def __init__(self, name, actor, field, activity_generator, time_generator, parameters):
        self.name = name
        self.actor = actor
        self.field = field
        self.parameters = parameters
        self.time_generator = time_generator

        self.clock = pd.DataFrame({"clock": 0, "activity": 1.}, index=actor.get_ids())
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
        ids, out, values = self.actor.make_attribute_action(self.field, self.who_acts_now(), self.parameters)

        if len(ids) > 0:
            if values is None:
                self.clock.loc[ids, "clock"] = self.time_generator.generate(weights=self.clock.loc[ids, "activity"])+1
            else:
                self.clock.loc[ids, "clock"] = self.time_generator.generate(weights=values)+1
        self.update_clock()

        return out
