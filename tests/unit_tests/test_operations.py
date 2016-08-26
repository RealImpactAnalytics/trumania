import datagenerator.operations as operations
import tests.mocks.operations as mockops
import pandas as pd
import numpy as np


def test_apply_should_delegate_to_single_col_dataframe_function_correctly():

    # some function that expect a dataframe as input => must return
    # dataframe with "result" column
    def f(df):
        return pd.DataFrame({"result": df["A"] + df["D"] - df["C"]})

    tested = operations.Apply(source_fields=["A", "C", "D"],
                              named_as="r",
                              f=f, f_args="dataframe")

    action_data = pd.DataFrame(
        np.random.rand(10, 5), columns=["A", "B", "C", "D", "E"])

    result = tested.build_output(action_data)

    assert result["r"].equals(action_data["A"] + action_data["D"] - action_data[
        "C"])


def test_apply_should_delegate_to_multi_col_dataframe_function_correctly():

    # now f returns several columns
    def f(df):
        return pd.DataFrame({
            "r1": df["A"] + df["D"] - df["C"],
            "r2": df["A"] + df["C"],
            "r3": df["A"] * df["C"],
        })

    tested = operations.Apply(source_fields=["A", "C", "D"],
                              named_as=["op1", "op2", "op3"],
                              f=f, f_args="dataframe")

    action_data = pd.DataFrame(
        np.random.rand(10, 5), columns=["A", "B", "C", "D", "E"])

    result = tested.transform(action_data)
    assert result.columns.tolist() == ["A", "B", "C", "D", "E", "op1", "op2",
                                       "op3"]

    assert result["op1"].equals(
        action_data["A"] + action_data["D"] - action_data["C"])
    assert result["op2"].equals(
        action_data["A"] + action_data["C"])
    assert result["op3"].equals(
        action_data["A"] * action_data["C"])


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
        np.random.rand(10, 5), columns=["A", "B", "C", "D", "E"])

    result = tested.build_output(action_data)

    assert result["r"].equals(
        action_data["A"] + action_data["D"] - action_data["C"])


def test_one_execution_should_merge_empty_data_correctly():

    # empty previous
    prev_df = pd.DataFrame(columns=[])
    prev_log = {}
    nop = operations.Operation()

    output, logs = operations.Chain._execute_operation((prev_df, prev_log), nop)

    assert logs == {}
    assert output.equals(prev_df)


def test_one_execution_should_merge_one_op_with_nothing_into_one_result():

    # empty previous
    prev = pd.DataFrame(columns=[]), {}

    cdrs = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    input = pd.DataFrame(np.random.rand(10, 2), columns=["C", "D"])
    op = mockops.FakeOp(input, logs={"cdrs": cdrs})

    output, logs = operations.Chain._execute_operation(prev, op)

    assert logs == {"cdrs": cdrs}
    assert input.equals(output)


def test_one_execution_should_merge_2_ops_correctly():

    # previous results
    init = pd.DataFrame(columns=[])
    mobility_logs = pd.DataFrame(np.random.rand(12, 3),
                                 columns=["A", "CELL", "duration"])

    cdrs = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    input = pd.DataFrame(np.random.rand(10, 2), columns=["C", "D"])
    op = mockops.FakeOp(input, {"cdrs" :cdrs})

    output, logs = operations.Chain._execute_operation(
        (init, {"mobility": mobility_logs}), op)

    assert logs == {"cdrs": cdrs, "mobility": mobility_logs}
    assert input.equals(output)


def test_chain_of_3_operation_should_return_merged_logs():

    cdrs1 = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    op1 = mockops.FakeOp(input, {"cdrs1" :cdrs1})

    cdrs2 = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    op2 = mockops.FakeOp(input, {"cdrs2" :cdrs2})

    cdrs3 = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    op3 = mockops.FakeOp(input, {"cdrs3" :cdrs3})

    chain = operations.Chain(op1, op2, op3)

    prev_data = pd.DataFrame(columns=[])
    action_data, all_logs = chain(prev_data)

    assert set(all_logs.keys()) == {"cdrs1", "cdrs2", "cdrs3"}
    assert all_logs["cdrs1"].equals(cdrs1)
    assert all_logs["cdrs2"].equals(cdrs2)
    assert all_logs["cdrs3"].equals(cdrs3)

