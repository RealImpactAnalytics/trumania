from __future__ import division
import pandas as pd
from abc import ABCMeta, abstractmethod
import numpy as np
from datagenerator import util_functions


class Operation(object):
    """
    An Operation is able to produce transform input into an output +
        produce logs.
    """

    def transform(self, action_data):
        """
        :param action_data: dataframe as produced by the previous operation
        :return: a dataframe that replaces the previous one in the pipeline
        """

        return action_data

    def emit_logs(self, action_data):
        """
        This method is used to produces logs (e.g. CDRs, mobility, topus...)


        :param action_data: output of this operation, as produced by transform()
        :return: emitted logs, as a dictionary of {"log_id": some_data_frame}


        WARNING: logs_ids must be unique in the pipeline (duplicates would
        overwrite each other)
        """

        return {}

    def __call__(self, data):

        output = self.transform(data)
        logs = self.emit_logs(output)

        return output, logs


class Chain(Operation):
    """
    A chain is a list of operation to be executed sequencially
    """

    def __init__(self, *operations):
        self.operations = list(operations)

    @staticmethod
    def _execute_operation((action_data, prev_logs), operation):
        """

        executes this operation and merges its logs with the previous one
        :param operation: the operation to call
        :return: the merged action data and logs
        """

        output, supp_logs = operation(action_data)
        # merging the logs of each operation of this action.
        # TODO: I guess just adding pd.concat at the end of this would allow
        # multiple operations to contribute to the same log => to be checked...
        return output, util_functions.merge_dicts([prev_logs, supp_logs])

    def __call__(self, data):
        init = [(data, {})]
        return reduce(self._execute_operation, init + self.operations)


class FieldLogger(Operation):
    """
    Log creator that simply select a set of columns and create a logged
    dataframe from it
    """

    def __init__(self, log_id, cols=None):
        """
        :param log_id: the id of the logs in the dictionary of logs returned
        by the Circus, at the end of the simulation
        :param cols: sub-sets of fields from the action data that will be
        selected in order to build the logs
        """
        self.log_id = log_id

        if type(cols) == str:
            self.cols = [cols]
        else:
            self.cols = cols

    def emit_logs(self, action_data):

        if self.cols is None:
            return {self.log_id: action_data}
        else:
            return {self.log_id: action_data[self.cols]}


class SideEffectOnly(Operation):
    """
    Operation that does not produce logs nor supplementary columns: just have
    side effect
    """
    __metaclass__ = ABCMeta

    def transform(self, action_data):
        self.side_effect(action_data)
        return action_data

    @abstractmethod
    def side_effect(self, action_data):
        """
        :param action_data:
        :return: nothing
        """
        pass


class AddColumns(Operation):
    """
    Very typical case of an operation that appends (i.e. joins) columns to
    the previous result
    """
    __metaclass__ = ABCMeta

    def __init__(self, join_kind="left"):
        self.join_kind = join_kind

    @abstractmethod
    def build_output(self, action_data):
        """
        Produces a dataframe with one or several columns and an index aligned
        with the one of input. The columns of this will be merge with input.

        :param action_data: current dataframe
        :return: the column(s) to append to it, as a dataframe
        """
        pass

    def transform(self, action_data):
        output = self.build_output(action_data)
        return pd.merge(left=action_data, right=output,
                        left_index=True, right_index=True,
                        how=self.join_kind)


class Apply(AddColumns):
    """
        Custom operation adding one single column computed from a user-provided
        function.

        The length of the source_fields must match the number columns
        in the dataframe expected by the user f function

    """

    def __init__(self, source_fields, named_as, f, f_args="dataframe"):
        """
        :param source_fields: input field from the action data
        :param named_as: name of the resulting field added to the action data
        :param f: tranforming fonction
        :param f_args: "dataframe" or "columns", depending on the signature
            of f:

            - "dataframe": input and output of the function is a dataframe
            containing one single column named "result"

            - "columns" input of f is a list of columns and output is 1
            column (like many numpy built-it function)
        """
        AddColumns.__init__(self)
        if type(source_fields) == str:
            self.source_fields = [source_fields]
        else:
            self.source_fields = source_fields

        self.named_as = named_as
        self.f = f
        if f_args not in ["dataframe", "series"]:
            raise ValueError("unrecognized f input type: {}".format(f_args))

        self.f_input = f_args

    def build_output(self, action_data):
        if self.f_input == "dataframe":
            result = self.f(action_data[self.source_fields])
            return result.rename(columns={"result": self.named_as})
        else:
            cols = [action_data[c] for c in self.source_fields]
            result = pd.DataFrame({self.named_as: self.f(*cols)})
            return result


#####################
# Collection of functions directly usable in Apply

def copy_if(action_data):
    """
    Copies values from the source to the "named_as" if the condition is True,
    otherwise inserts NA

    usage:

        Apply(source_fields=["some_source_field", "some_condition_field"],
              named_as="some_result_field",
              f=copy_if)
    """

    condition_field, source_field = action_data.columns
    copied = action_data.where(action_data[condition_field])[[source_field]]
    return copied.rename(columns={source_field: "result"})


def logistic(a, b):
    """

    Returns a function, usable in an Apply operation, that transforms the
    specified field with a sigmoid with the provided parameters
    _logistic

    usage:

        Apply(source_fields=["some_source_field"],
              named_as="some_result_field",
              f=sigmoid(a=-0.01, b=10.)
    """

    def _logistic(x):
        """
        returns the value of the logistic function 1/(1+e^-(ax+b))
        """
        the_exp = np.minimum(-(a * x + b), 10.)
        return 1 / (1 + np.exp(the_exp))

    return _logistic


def identity(x):
    return x
