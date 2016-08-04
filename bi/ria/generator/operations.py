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

    def __call__(self, data):

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


class SideEffectOnly(Operation):
    """
    Operation that does not produce logs nor supplementary columns: just have
    side effect
    """

    def transform(self, data):
        self.side_effect(data)
        return data

    def side_effect(self, data):
        """
        :param data:
        :return: nothing
        """

        raise NotImplemented("BUG: the sub-class must implement this method")


class AddColumns(Operation):
    """
    Very typical case of an operation that appends (i.e. joins) columns to
    the previous result
    """

    def __init__(self, join_kind="outer"):
        self.join_kind = join_kind

    def build_output(self, data):
        """
        Produces a dataframe with one or several columns and an index aligned
        with the one of input. The columns of this will be merge with input.

        :param data: current dataframe
        :return: the column(s) to append to it, as a dataframe
        """

        raise NotImplemented("BUG: sub-class must implement this")

    def transform(self, data):
        output = self.build_output(data)
        return pd.merge(left=data, right=output,
                        left_index=True, right_index=True,
                        how=self.join_kind)


class Constant(AddColumns):
    """
    Operation that produces one single field having a fixed value
    """

    def __init__(self, value, named_as):
        AddColumns.__init__(self)
        self.value = value
        self.named_as = named_as

    def build_output(self, data):
        return pd.DataFrame({self.named_as: self.value}, index=data.index)


class RandomValues(AddColumns):
    """
    Operation that produces one single column generated randomly.
    """

    def __init__(self, value_generator, named_as, weights_field=None):
        AddColumns.__init__(self)
        self.value_generator = value_generator
        self.named_as = named_as
        self.weights_field = weights_field

    def build_output(self, data):
        if self.weights_field is None:
            weights = None
        else:
            weights = data[self.weights_field]

        values = self.value_generator.generate(data.shape[0], weights)
        return pd.DataFrame({self.named_as: values}, index=data.index)


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
