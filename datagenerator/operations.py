import pandas as pd
from abc import ABCMeta, abstractmethod


class Operation(object):
    """
    An Operation is able to produce transform input into an output +
        produce logs.
    """

    def transform(self, data):
        """
        :param data: dataframe as produced by the previous operation
        :return: a dataframe that replaces the previous one in the pipeline
        """

        return data

    def emit_logs(self, data):
        """
        This method is used to produces logs (e.g. CDRs, mobility, topus...)


        :param data: output of this operation, as produced by transform()
        :return: emitted logs, as a dictionary of {"log_id": some_data_frame}


        WARNING: logs_ids must be unique in the pipeline (duplicates would
        overwrite each other)
        """

        return {}

    def __call__(self, data):

        output = self.transform(data)
        logs = self.emit_logs(output)

        return output, logs


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

    def emit_logs(self, data):

        if self.cols is None:
            return {self.log_id: data}
        else:
            return {self.log_id: data[self.cols]}


class SideEffectOnly(Operation):
    """
    Operation that does not produce logs nor supplementary columns: just have
    side effect
    """
    __metaclass__ = ABCMeta

    def transform(self, data):
        self.side_effect(data)
        return data

    @abstractmethod
    def side_effect(self, data):
        """
        :param data:
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
    def build_output(self, data):
        """
        Produces a dataframe with one or several columns and an index aligned
        with the one of input. The columns of this will be merge with input.

        :param data: current dataframe
        :return: the column(s) to append to it, as a dataframe
        """
        pass

    def transform(self, data):
        output = self.build_output(data)
        return pd.merge(left=data, right=output,
                        left_index=True, right_index=True,
                        how=self.join_kind)


class Apply(AddColumns):
    """
        Custom operation adding one single column computed from a user-provided
        function.

        The length of the source_fields must match the number columns
        in the dataframe expected by the user f function

        The f function must return a dataframe with one single column,
        named "result"
    """

    def __init__(self, source_fields, result_field, f):
        super(Apply, self).__init__()
        if type(source_fields) == str:
            self.source_fields = [source_fields]
        else:
            self.source_fields = source_fields

        self.result_field = result_field
        self.f = f

    def build_output(self, data):
        df = self.f(data[self.source_fields])

        return df.rename(columns={"result": self.result_field})
