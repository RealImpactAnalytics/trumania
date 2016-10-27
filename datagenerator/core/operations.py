from __future__ import division
from scipy import stats
from abc import ABCMeta, abstractmethod

from datagenerator.core.util_functions import *


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
        """

        return {}

    def __call__(self, action_data):

        output = self.transform(action_data)
        logs = self.emit_logs(output)

        return output, logs


class Chain(Operation):
    """
    A chain is a list of operation to be executed sequencially
    """

    def __init__(self, *operations):
        self.operations = list(operations)

    def append(self, *operations):
        """
        adds operations to be executed at the end of this chain
        """
        self.operations += list(operations)

    @staticmethod
    def _execute_operation((action_data, prev_logs), operation):
        """

        executes this operation and merges its logs with the previous one
        :param operation: the operation to call
        :return: the merged action data and logs
        """

        output, supp_logs = operation(action_data)
        # merging the logs of each operation of this action.
        return output, merge_dicts([prev_logs, supp_logs], df_concat)

    def __call__(self, action_data):
        init = [(action_data, {})]
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


class DropRow(Operation):
    """
    Discards any row in the action data where the condition field is false.
    """

    def __init__(self, condition_field):
        self.condition_field = condition_field

    def transform(self, action_data):
        return action_data[~action_data[self.condition_field]]


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
        :param named_as: name of the resulting fields added to the action data
        :param f: transforming function
        :param f_args: "dataframe" or "columns", depending on the signature
            of f:

            - "dataframe": input and output of the function is a dataframe
             as many columns as there are values in "named_as"

            - "columns" input of f is a list of columns and output is 1
             column (like many numpy built-it function). In that case,
             "named_as" can obviously only contain one name
        """

        AddColumns.__init__(self)
        if type(source_fields) == str:
            self.source_fields = [source_fields]
        else:
            self.source_fields = source_fields

        if type(named_as) == str:
            self.named_as = [named_as]
        else:
            self.named_as = named_as

        self.f = f
        if f_args not in ["dataframe", "series"]:
            raise ValueError("unrecognized f input type: {}".format(f_args))

        if f_args == "series":
            assert len(self.named_as) == 1, \
                "'series' functions can only return 1 column"

        self.f_input = f_args

    def build_output(self, action_data):
        if self.f_input == "dataframe":
            result = self.f(action_data[self.source_fields])
            renamed = result.rename(
                columns=dict(zip(result.columns, self.named_as)))

            return renamed
        else:
            cols = [action_data[c] for c in self.source_fields]
            result = pd.DataFrame({self.named_as[0]: self.f(*cols)})
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


def bound_value(lb=None, ub=None):
    """
    builds a function that limits the range of a value
    """

    def _f(value):
        limited = max(lb, value)
        if ub is not None:
            limited = min(ub, limited)
        return limited

    return _f


def scale(factor):
    def _f_vect(value):
        return value * factor

    return _f_vect


def logistic(k, x0=0, L=1):
    """

    Returns a function, usable in an Apply operation, that transforms the
    specified field with a sigmoid with the provided parameters

    :param k: the steepness of the curve
    :param x0: the x-value of the sigmoid's midpoint (default: 0)
    :param L: maximum value of the logistic (default: 1)

    same parameter naming conventions as in:
    https://en.wikipedia.org/wiki/Logistic_function

    usage:
        Apply(source_fields=["some_source_field"],
              named_as="some_result_field",
              f=sigmoid(k=-0.01, x0=1000)
    """

    def _logistic(x):
        the_exp = np.minimum(-k * (x - x0), 10)
        return L / (1 + np.exp(the_exp))

    return _logistic


def bounded_sigmoid(x_min, x_max, shape, incrementing=True):
    """
    Builds a S-shape curve that have y values evolving between 0 and 1 over
    the x domain [x_min, x_max]

    This is preferable to the logistic function for cases where we want to
    make sure that the curve actually reaches 0 and 1 at some point (e.g.
    probability of triggering an "restock" action must be 1 if stock is as
    low as 1).

    :param x_min: lower bound of the x domain
    :param x_max: lower bound of the x domain
    :param incrementing: if True, evolve from 0 to 1, or from 1 to 0 otherwise
    :param shape: strictly positive number controlling the shape of the
                  resulting function
                  * 1 correspond to linear transition
                  * higher values yield a more and more sharper, i.e. more
                    vertical S shape, converging towards a step function
                    transiting at (x_max-x_min)/2 for very large values of S (
                    e.g. 10000)
                  * values in ]0,1[ yield vertically shaped sigmoids, sharply
                    rising/falling at the boundary of the x domain and
                    transiting more smoothly in the middle of it.
    """

    bounded = bound_value(lb=x_min, ub=x_max)

    def f(x):
        # values outside the sigmoid are just the repetition of what's
        # happening at the boundaries
        x_b = bounded(x)

        if incrementing:
            return stats.beta.cdf((x_b - x_min) / (x_max - x_min),
                                   a=shape, b=shape)
        else:
            return stats.beta.sf((x_b - x_min) / (x_max - x_min),
                                  a=shape, b=shape)

    return np.frompyfunc(f, 1, 1)


def identity(x):
    return x
