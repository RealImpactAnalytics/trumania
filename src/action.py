import numpy as np
import pandas as pd

class ActorAction(object):
    def __init__(self,name,actor,time_generator):
        self.name = name
        self.main_actor = actor
        self.time_generator = time_generator
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

    def add_impact(self,name,attribute,function,parameters):
        self.impacts[name] = (attribute,function,parameters)

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
        #TODO there's something weird here: if only 1 field is returned, we would maybe like to have f to be the name of the field
        for f in self.base_fields:
            rel_name = self.base_fields[f][0]
            rel_parameters = self.base_fields[f][1]
            f_data.append(self.relationships[rel_name].select_one(rel_parameters["key"], ids))

        all_fields = pd.concat(f_data,axis=1,join='inner')
        all_fields.reset_index(inplace=True)

        return all_fields

    def check_conditions(self, data):
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

    def make_impacts(self, data):
        for k in self.impacts.keys():
            if self.impacts[k][1] == "decrease_stock":
                params = {"values":pd.Series(data[self.impacts[k][2]].values,index=data["A"].values)}
                self.main_actor.apply_to_attribute(self.impacts[k][0],self.impacts[k][1],params)

    def execute(self):
        act_now = self.main_actor.who_acts_now()
        fields = self.get_field_data(act_now.index.values)
        passed = self.check_conditions(fields)
        fields["PASS_CONDITIONS"] = 0
        fields.loc[passed,"PASS_CONDITIONS"] = 1

        self.make_impacts(fields)

        self.main_actor.set_clock(act_now.index, self.time_generator.generate(act_now["activity"])+1)
        self.main_actor.update_clock()

        return fields

class AttributeAction(object):
    def __init__(self,name,actor,field,parameters):
        self.name = name
        self.actor = actor
        self.field = field
        self.parameters = parameters

    def execute(self):
        return self.actor.make_attribute_action(self.field,self.parameters)