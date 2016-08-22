import datagenerator.operations as operations
import pandas as pd
import numpy as np


def test_apply_should_delegate_to_dataframe_function_correctly():

    # some function that expect a dataframe as input => must return
    # dataframe with "result" column
    def f(df):
        return pd.DataFrame({"result": df["A"] + df["D"] - df["C"]})

    tested = operations.Apply(source_fields=["A", "C", "D"],
                              named_as="r",
                              f=f, f_args="dataframe")

    action_data = pd.DataFrame(
        np.random.rand(10,5), columns=["A", "B", "C", "D", "E"])

    result = tested.build_output(action_data)

    assert result["r"].equals(action_data["A"] + action_data["D"] - action_data[
        "C"])


def test_apply_should_delegate_to_columns_function_correctly():
    """
        same as the above, but this time f input and output arguments are
        pandas Series
    """

    def f(ca, cc, cd):
        return ca + cd - cc

    tested = operations.Apply(source_fields=["A", "C", "D"],
                              named_as="r",
                              f=f, f_args="series")

    action_data = pd.DataFrame(
        np.random.rand(10,5), columns=["A", "B", "C", "D", "E"])

    result = tested.build_output(action_data)

    assert result["r"].equals(
        action_data["A"] + action_data["D"] - action_data["C"])
