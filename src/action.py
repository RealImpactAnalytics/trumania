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

        :param ids:
        :return: all fields produced by the activity
        """
        f_data = []
        # TODO there's something weird here: if only 1 field is returned, we would maybe like to have f to be the name of the field
        for f in self.base_fields:
            rel_name = self.base_fields[f][0]
            rel_parameters = self.base_fields[f][1]
            f_data.append(self.relationships[rel_name].select_one(rel_parameters["key"], ids))

        all_fields = pd.concat(f_data, axis=1, join='inner').reset_index()

        for f in self.secondary_fields:
            rel_name = self.secondary_fields[f][0]
            rel_parameters = self.secondary_fields[f][1]
            out = self.relationships[rel_name].select_one(rel_parameters["key_rel"],
                                                          all_fields[rel_parameters["key_table"]].values)
            all_fields = pd.merge(all_fields, out.rename(columns={rel_parameters["out_rel"]: f}),
                                  left_on=rel_parameters["key_table"],
                                  right_index=True)

        print ""

        return all_fields

    def check_conditions(self, data):
        valid_ids = data.index

        for c in self.value_conditions.keys():
            actorf, attrf, func, fparam = self.value_conditions[c]
            current_actors = data.loc[valid_ids, actorf].values
            validated = self.main_actor.check_attributes(current_actors, attrf, func, fparam)
            valid_ids = valid_ids[validated]

        for c in self.feature_conditions.keys():
            actorf, attrf, item, func, fparam = self.feature_conditions[c]
            current_actors = data.loc[valid_ids, actorf].values
            attr_val = self.main_actor.get_join(current_actors, attrf)
            validated = self.items[item].check_condition(attr_val, func, fparam)
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
        fields = self.get_field_data(act_now.values)

        if len(fields.index) > 0:
            passed = self.check_conditions(fields)
            fields["PASS_CONDITIONS"] = 0
            fields.loc[passed, "PASS_CONDITIONS"] = 1
            count_passed = len(passed)
            if count_passed > 0:
                self.make_impacts(fields)

        self.set_clock(act_now, self.time_generator.generate(weights=self.clock.loc[act_now, "activity"]) + 1)
        self.update_clock()

        return fields


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
