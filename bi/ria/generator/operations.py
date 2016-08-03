import pandas as pd


class Toolkit(object):
    """
    Just a marker class: any sub class of this in intended at containing
         Operations
    """
    pass


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

    def apply(self, data):

        output = self.transform(data)
        logs = self.emit_logs(output)

        return output, logs


class ColumnLogger(Operation):
    """
    Log creator that simply select a set of columns and create a logged
    dataframe from it
    """

    def __init__(self, log_id, cols):
        self.log_id = log_id

        if type(cols) == str:
            self.cols = [cols]
        else:
            self.cols = cols

    def emit_logs(self, data):
        return {self.log_id: data[self.cols]}


class JoinedOperation(Operation):
    """
    Very typical case of an operation that appends (i.e. joins) columns to
    the previous result
    """

    def __init__(self, join_kind="outer"):
        self.join_kind = join_kind

    def build_output(self, input):
        """
        Produces a dataframe with one or several columns and an index aligned
        with the one of input. The columns of this will be merge with input.

        :param input: current dataframe
        :return: the column(s) to append to it
        """

        raise NotImplemented("BUG: sub-class must implement this")

    def transform(self, data):
        output = self.build_output(data)
        return pd.merge(left=data, right=output,
                        left_index=True, right_index=True,
                        how=self.join_kind)
