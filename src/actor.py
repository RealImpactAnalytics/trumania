from src.attribute import *


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
        #self._table = pd.DataFrame({"clock": 0, "activity":1.},index=ids)
        self._table = pd.DataFrame(index=ids)
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

    def add_transient_attribute(self, name, att_type, generator=None, time_generator=None, activity=None, params=None):
        """

        :param name:
        :param att_type:
        :param generator:
        :param time_generator:
        :param activity:
        :param params:
        :return: None
        """
        if att_type == "choice":
            transient_attribute = ChoiceAttribute(self._table.index.values)
        elif att_type == "stock":
            transient_attribute = StockAttribute(self._table.index.values,params["trigger_generator"])
        elif att_type == "labeled_stock":
            transient_attribute = LabeledStockAttribute(self._table.index.values,params["relationship"])
        else:
            raise Exception("unknown type: %s" % att_type)

        if generator is not None:
            transient_attribute.update(self._table.index.values, generator.generate(len(self._table.index)))

        if activity is not None:
            transient_attribute.set_activity(self._table.index.values, activity)

        if time_generator is not None:
            transient_attribute.init_clock(time_generator)

        self._transient_attributes[name] = transient_attribute

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

    def make_attribute_action(self,attr_name,ids,params):
        """

        :param attr_name:
        :param params:
        :return:
        """
        if self._transient_attributes.has_key(attr_name):
            params["ids"] = ids
            return self._transient_attributes[attr_name].make_actions(**params)
        else:
            raise Exception("No transient attribute named %s" % attr_name)

    def apply_to_attribute(self,attr_name,function,params):
        if self._transient_attributes.has_key(attr_name):
            return getattr(self._transient_attributes[attr_name], function)(**params)
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



