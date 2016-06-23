import numpy as np
import pandas as pd

class Action(object):
    def __init__(self,name,actor):
        self.name = name
        self.main_actor = actor
        self.secondary_actors = {}
        self.relationships = {}
        self.items = {}
        self.base_fields = {}
        self.value_conditions = {}
        self.feature_conditions = {}
        self.triggers = {}
        self.impacts = {}

    def add_secondary_actor(self,name,actor):
        self.secondary_actors[name] = actor

    def add_relationship(self, name, relationship):
        self.relationships[name] = relationship

    def add_item(self,name,item):
        self.items[name] = item

    def add_field(self,name,relationship,params=None):
        """

        :param name:
        :type relationship: str
        :param relationship: name of relationship to use (as named in the "relationship" field of the action)
        :param params:
        :return:
        """
        self.base_fields[name] = (relationship, params)

    def add_value_condition(self,name,actorfield,attributefield,function,parameters):
        self.value_conditions[name] = (actorfield, attributefield, function, parameters)

    def add_feature_condition(self,name,actorfield,attributefield,item,function,parameters):
        self.feature_conditions[name] = (actorfield,attributefield,item,function,parameters)

    def get_field_data(self,ids):
        """

        :param ids:
        :return: all fields produced by the activity
        """
        f_data = []

        for f in self.base_fields:
            rel_name = self.base_fields[f][0]
            rel_parameters = self.base_fields[f][1]
            f_data.append(self.relationships[rel_name].select_one(rel_parameters["key"], ids))

        all_fields = pd.concat(f_data,axis=1,join='inner')
        all_fields.reset_index(inplace=True)

        return all_fields

    def check_conditions(self,data):
        valid_ids = data.index

        for c in self.value_conditions.keys():
            actorf, attrf, func, fparam = self.value_conditions[c]
            current_actors = data.loc[valid_ids,actorf].values
            validated = self.main_actor.check_attributes(current_actors,attrf,func,fparam)
            valid_ids = valid_ids[validated]

        for c in self.feature_conditions.keys():
            actorf,attrf,item,func,fparam = self.feature_conditions[c]
            current_actors = data.loc[valid_ids,actorf].values
            attr_val = self.main_actor.get_join(current_actors,attrf)
            validated = self.items[item].check_condition(attr_val,func,fparam)
            valid_ids = valid_ids[validated]

        return valid_ids

    def execute(self):
        act_now = self.main_actor.who_acts_now()
        fields = self.get_field_data(act_now.index.values)
        passed = self.check_conditions(fields)
        fields["PASS_CONDITIONS"] = 0
        fields.loc[passed,"PASS_CONDITIONS"] = 1
        return fields