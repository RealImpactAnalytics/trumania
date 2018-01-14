from scipy import stats
from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
from trumania.core.util_functions import merge_dicts, df_concat
import functools


class Operation(object):
    """
    An Operation is able to produce transform input into an output +
        produce logs.
    """

    def transform(self, story_data):
        """
        :param story_data: dataframe as produced by the previous operation
        :return: a dataframe that replaces the previous one in the pipeline
        """

        return story_data

    def emit_logs(self, story_data):
        """
        This method is used to produces logs (e.g. CDRs, mobility, topus...)

        :param story_data: output of this operation, as produced by transform()
        :return: emitted logs, as a dictionary of {"log_id": some_data_frame}
        """

        return {}

    def __call__(self, story_data):

        output = self.transform(story_data)
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
    def _execute_operation(story_data__prev_logs, operation):
        """

        executes this operation and merges its logs with the previous one
        :param operation: the operation to call
        :return: the merged story data and logs
        """

        (story_data, prev_logs) = story_data__prev_logs

        output, supp_logs = operation(story_data)
        # merging the logs of each operation of this story.
        return output, merge_dicts([prev_logs, supp_logs], df_concat)

    def __call__(self, story_data):
        init = [(story_data, {})]
        return functools.reduce(self._execute_operation, init + self.operations)


class FieldLogger(Operation):
    """
    Log creator that simply select a set of columns and create a logged
    dataframe from it
    """

    def __init__(self, log_id, cols=None, exploded_cols=None):
        """
        :param log_id: the id of the logs in the dictionary of logs returned
        by the Circus, at the end of the simulation
        :param cols: sub-sets of fields from the story data that will be
        selected in order to build the logs
        :param exploded_cols: name of one or several columns containing list of
        values. If provided, we explode the story_data dataframe and log one per value
        in that list (which is more that one line per row in story_data).
        In each row, all lists must have the same length
        """
        self.log_id = log_id

        if type(exploded_cols) == str:
            self.exploded_cols = [exploded_cols]
        else:
            self.exploded_cols = [] if exploded_cols is None else exploded_cols

        if type(cols) == str:
            self.cols = [cols]
        else:
            self.cols = [] if cols is None else cols

        self.cols += self.exploded_cols

    def emit_logs(self, story_data):

        # explode lists, cf constructor documentation
        if self.exploded_cols:

            def explo(df):
                explosion_len = len(df[self.exploded_cols[0]])
                df2 = pd.DataFrame(
                    [df.drop(self.exploded_cols) for _ in range(explosion_len)])
                for col in self.exploded_cols:
                    df2[col] = df[col]

                return df2

            logged_data = pd.concat(explo(row)
                                    for _, row in story_data.iterrows())

        else:
            logged_data = story_data

        if not self.cols:
            return {self.log_id: logged_data}
        else:
            return {self.log_id: logged_data[self.cols]}


class SideEffectOnly(Operation):
    """
    Operation that does not produce logs nor supplementary columns: just have
    side effect
    """
    __metaclass__ = ABCMeta

    def transform(self, story_data):
        self.side_effect(story_data)
        return story_data

    @abstractmethod
    def side_effect(self, story_data):
        """
        :param story_data:
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
    def build_output(self, story_data):
        """
        Produces a dataframe with one or several columns and an index aligned
        with the one of input. The columns of this will be merge with input.

        :param story_data: current dataframe
        :return: the column(s) to append to it, as a dataframe
        """
        pass

    def transform(self, story_data):
        output = self.build_output(story_data)
#        logging.info("  adding column(s) {}".format(output.columns.tolist()))
        return pd.merge(left=story_data, right=output,
                        left_index=True, right_index=True,
                        how=self.join_kind)


class DropRow(Operation):
    """
    Discards any row in the story data where the condition field is false.
    """

    def __init__(self, condition_field):
        self.condition_field = condition_field

    def transform(self, story_data):
        return story_data[~story_data[self.condition_field]]


class Apply(AddColumns):
    """
        Custom operation adding one single column computed from a user-provided
        function.

        The length of the source_fields must match the number columns
        in the dataframe expected by the user f function

    """

    def __init__(self, source_fields, named_as, f, f_args="dataframe"):
        """
        :param source_fields: input field from the story data
        :param named_as: name of the resulting fields added to the story data
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

    def build_output(self, story_data):
        if self.f_input == "dataframe":
            result = self.f(story_data[self.source_fields])
            renamed = result.rename(
                columns=dict(zip(result.columns, self.named_as)))

            return renamed
        else:
            cols = [story_data[c] for c in self.source_fields]
            result = pd.DataFrame({self.named_as[0]: self.f(*cols)})
            return result


#####################
# Collection of functions directly usable in Apply

def copy_if(story_data):
    """
    Copies values from the source to the "named_as" if the condition is True,
    otherwise inserts NA

    usage:

        Apply(source_fields=["some_source_field", "some_condition_field"],
              named_as="some_result_field",
              f=copy_if)
    """

    condition_field, source_field = story_data.columns
    copied = story_data.where(story_data[condition_field])[[source_field]]
    return copied.rename(columns={source_field: "result"})


def bound_value(lb=None, ub=None):
    """
    builds a function that limits the range of a value
    """

    def _f(value):
        limited = value if lb is None else max(lb, value)
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
    probability of triggering an "restock" story must be 1 if stock is as
    low as 1).

    See /tests/notebooks/bounded_sigmoid.ipynb for examples

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
                                  a=shape,
                                  b=shape)
        else:
            return stats.beta.sf((x_b - x_min) / (x_max - x_min),
                                 a=shape,
                                 b=shape)

    return np.frompyfunc(f, 1, 1)


def identity(x):
    return x
