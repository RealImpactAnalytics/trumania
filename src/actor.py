import pandas as pd
import numpy as np


class Actor(object):
    """

    """

    def __init__(self, size, id_start=0, prefix="",max_length=10):
        """

        :param size:
        :param id_start:
        :return:
        """
        ids = [prefix + str(x).zfill(max_length) for x in np.arange(id_start, id_start + size)]
        self._table = pd.DataFrame({"clock": 0, "activity":1.},index=ids)
        self._transient_attributes = {}

    def get_ids(self):
        return self._table.index.values

    def add_attribute(self, name, generator,weights=None,weight_field=None):
        """Adds a column named "name" to the inner table of the actor, randomly generated from the generator.

        :param name: string, will be used as name for the column in the table
        :param generator: either a random generator or a timeprofiler. If a timeprofiler, weights or a weight field can be added
        :param weights: Pandas Series
        :param weight_field: string
        :return: none
        """
        if weights is not None:
            self._table[name] = generator.generate(weights)
        elif weight_field is not None:
            self._table[name] = generator.generate(self._table[weight_field])
        else:
            self._table[name] = generator.generate(len(self._table.index))

    def add_transient_attribute(self, name, att_type, generator, time_generator=None, activity=None,params=None):
        """

        :param name:
        :param att_type:
        :param generator:
        :param time_generator:
        :param activity:
        :return: None
        """
        if att_type == "choice":
            transient_attribute = ChoiceAttribute(self._table.index.values)
        elif att_type == "stock":
            transient_attribute = StockAttribute(self._table.index.values,params["trigger_generator"])
        else:
            raise Exception("unknown type: %s" % att_type)

        transient_attribute.update(self._table.index.values, generator.generate(len(self._table.index)))

        if activity is not None:
            transient_attribute.set_activity(self._table.index.values, activity)

        if time_generator is not None:
            transient_attribute.init_clock(time_generator)

        self._transient_attributes[name] = transient_attribute

    def who_acts_now(self):
        """

        :return:
        """
        return self._table[self._table["clock"] == 0]

    def update_clock(self, decrease=1):
        """

        :param decrease:
        :return:
        """
        self._table["clock"] -= 1

    def update_attribute(self, name, generator,weights=None,weight_field=None):
        """

        :param name:
        :param generator: either a random generator or a timeprofiler. If a timeprofiler, weights or a weight field can be added
        :param weights:
        :return:
        """
        if weights is not None:
            self._table[name] = generator.generate(weights)
        elif weight_field is not None:
            self._table[name] = generator.generate(self._table[weight_field])
        else:
            self._table[name] = generator.generate(len(self._table.index))

    def make_actions(self, new_time_generator, action):
        """

        :param new_time_generator:
        :return:
        """
        act_now = self.who_acts_now()

        if len(act_now.index) > 0:
            self._table.loc[act_now.index, "clock"] = new_time_generator.generate(act_now["activity"])+1
        self.update_clock()
        return out

    def make_attribute_action(self,attr_name,params):
        """

        :param attr_name:
        :param params:
        :return:
        """
        if self._transient_attributes.has_key(attr_name):
            return self._transient_attributes[attr_name].make_actions(**params)
        else:
            raise Exception("No transient attribute named %s" % attr_name)

    def apply_to_attribute(self,attr_name,function,params):
        if self._transient_attributes.has_key(attr_name):
            getattr(self._transient_attributes[attr_name], function)(**params)
        else:
            raise Exception("No transient attribute named %s" % attr_name)

    def get_join(self,field,ids):
        """

        :param field:
        :param ids:
        :return:
        """
        if field in self._table.columns.values:
            table_to_join = self._table
            field_name = field
        elif field in self._transient_attributes.keys():
            table_to_join = self._transient_attributes[field]._table
            field_name = "value"
        else:
            raise Exception("No field or attribute named %s" % field)

        left = pd.DataFrame(index=ids)
        right = pd.DataFrame(table_to_join[field_name],index=table_to_join.index)
        res = left.join(right)
        return res[field_name].values

    def check_attributes(self,ids,field,condition,threshold):
        """

        :param ids:
        :param field:
        :param condition:
        :param threshold:
        :return:
        """
        if field in self._table.columns.values:
            table = self._table.loc[ids]
            field_name = field
        elif field in self._transient_attributes.keys():
            table = self._transient_attributes[field]._table.loc[ids]
            field_name = "value"
        else:
            raise Exception("No field or attribute named %s" % field)

        if condition == ">":
            return table[field] > threshold
        if condition ==  ">=":
            return table[field] >= threshold
        if condition == "<":
            return table[field] < threshold
        if condition == "<=":
            return table[field] <= threshold
        if condition == "==":
            return table[field] == threshold
        raise Exception("Unknown condition : %s" % condition)

    def set_clock(self,ids,values):
        self._table.loc[ids, "clock"] = values

    def __repr__(self):
        return self._table


class CallerActor(Actor):
    """
    A CallerActor is an actor that makes calls
    """
    def __init__(self, size, id_start=0):
        """

        :param size:
        :param id_start:
        :return:
        """
        Actor.__init__(self, size, id_start)

    def make_calls(self, new_time_generator, relationship):
        """

        :param relationship:
        :param new_time_generator:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["B"])
        if len(act_now.index) > 0:
            calls = relationship.select_one("A", act_now.index.values)
            if len(calls.index) > 0:
                out = calls[["B"]]
                out.reset_index(inplace=True)

            self._table.loc[act_now.index, "clock"] = new_time_generator.generate(act_now["activity"])+1
        self.update_clock()
        return out


class TransientAttribute(object):
    """

    """
    def __init__(self, ids):
        """

        :param ids:
        :return:
        """
        self._table = pd.DataFrame({ "value": "", "clock": 0, "activity":1.},index=ids)

    def update(self, ids_to_update, values):
        """

        :param values:
        :param ids_to_update:
        :return:
        """
        self._table.loc[ids_to_update, "value"] = values

    def set_activity(self, ids, activity):
        """

        :param ids:
        :param activity:
        :return:
        """
        self._table.loc[ids, "activity"] = activity

    def init_clock(self,new_time_generator):
        """

        :param new_time_generator:
        :return:
        """
        self._table["clock"] = new_time_generator.generate(self._table["activity"])

    def who_acts_now(self):
        """

        :return:
        """
        return self._table[self._table["clock"] == 0]

    def update_clock(self, decrease=1):
        """

        :param decrease:
        :return:
        """
        self._table["clock"] -= decrease


class ChoiceAttribute(TransientAttribute):
    """

    """

    def make_actions(self, new_time_generator, relationship, id1, id2):
        """

        :param new_time_generator:
        :param relationship:
        :param id1:
        :param id2:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["new"])
        if len(act_now.index) > 0:
            out = relationship.select_one(id1,act_now.index.values).rename(columns={id2:"new"})
            if len(out.index) > 0:
                self._table.loc[act_now.index, "value"] = out["new"].values
            self._table.loc[act_now.index, "clock"] = new_time_generator.generate(act_now["activity"])+1
        self.update_clock()
        out.reset_index(inplace=True)
        return out


class StockAttribute(TransientAttribute):
    """

    """

    def __init__(self, ids, trigger_generator):
        """

        :param ids:
        :param trigger_generator: Random Generator that returns 1 or 0 depending on 1 value (stock, and parameters)
        Usually  a check vs a logistic regression
        :return:
        """
        TransientAttribute.__init__(self, ids)
        self._table["value"] = pd.Series(0, index=self._table.index, dtype=int)
        self._trigger = trigger_generator

    def init_clock(self,new_time_generator):
        """

        :param new_time_generator:
        :return:
        """
        self._table["clock"] = new_time_generator.generate(len(self._table.index))

    def decrease_stock(self, values):
        """

        :param values: Pandas Series
        :param new_time_generator:
        :return:
        """
        self._table.loc[values.index,"value"] -= values.values

        triggers = self._trigger.generate(self._table.loc[values.index,"value"])
        small_table = self._table.loc[values.index]
        act_now = small_table[triggers]
        if len(act_now.index)>0:
            self._table.loc[act_now.index, "clock"] = 0

    def make_actions(self,relationship,id1,id2,id3):
        """

        :param relationship: AgentRelationship
        :param id1: id of customer
        :param id2: id of Agent
        :param id3: id of Value
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["new"])
        if len(act_now.index) > 0:
            out = relationship.select_one(id1,act_now.index.values).rename(columns={id2:"AGENT",id3:"VALUE"})
            if len(out.index) > 0:
                self._table.loc[out.index,"value"] += out["VALUE"]

        out.reset_index(inplace=True)
        self.update_clock()
        return out

# TODO LabeledStockAttribute, where it is linked to a relationship table actor-itemID
