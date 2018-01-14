import pandas as pd
import numpy as np

import tests.mocks.operations as mockops
from trumania.core import operations
from trumania.core.util_functions import build_ids


def test_apply_should_delegate_to_single_col_dataframe_function_correctly():

    # some function that expect a dataframe as input => must return
    # dataframe with "result" column
    def f(df):
        return pd.DataFrame({"result": df["A"] + df["D"] - df["C"]})

    tested = operations.Apply(source_fields=["A", "C", "D"],
                              named_as="r",
                              f=f, f_args="dataframe")

    story_data = pd.DataFrame(
        np.random.rand(10, 5), columns=["A", "B", "C", "D", "E"])

    result = tested.build_output(story_data)

    assert result["r"].equals(story_data["A"] + story_data["D"] - story_data[
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

    story_data = pd.DataFrame(
        np.random.rand(10, 5), columns=["A", "B", "C", "D", "E"])

    result = tested.transform(story_data)
    assert result.columns.tolist() == ["A", "B", "C", "D", "E", "op1", "op2",
                                       "op3"]

    assert result["op1"].equals(
        story_data["A"] + story_data["D"] - story_data["C"])
    assert result["op2"].equals(
        story_data["A"] + story_data["C"])
    assert result["op3"].equals(
        story_data["A"] * story_data["C"])


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

    story_data = pd.DataFrame(
        np.random.rand(10, 5), columns=["A", "B", "C", "D", "E"])

    result = tested.build_output(story_data)

    assert result["r"].equals(
        story_data["A"] + story_data["D"] - story_data["C"])


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
    op = mockops.FakeOp(input, {"cdrs": cdrs})

    output, logs = operations.Chain._execute_operation(
        (init, {"mobility": mobility_logs}), op)

    assert logs == {"cdrs": cdrs, "mobility": mobility_logs}
    assert input.equals(output)


def test_chain_of_3_operation_should_return_merged_logs():

    cdrs1 = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    op1 = mockops.FakeOp(input, {"cdrs1": cdrs1})

    cdrs2 = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    op2 = mockops.FakeOp(input, {"cdrs2": cdrs2})

    cdrs3 = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    op3 = mockops.FakeOp(input, {"cdrs3": cdrs3})

    chain = operations.Chain(op1, op2, op3)

    prev_data = pd.DataFrame(columns=[])
    story_data, all_logs = chain(prev_data)

    assert set(all_logs.keys()) == {"cdrs1", "cdrs2", "cdrs3"}
    assert all_logs["cdrs1"].equals(cdrs1)
    assert all_logs["cdrs2"].equals(cdrs2)
    assert all_logs["cdrs3"].equals(cdrs3)


def test_drop_when_condition_is_all_false_should_have_no_impact():

    cdrs = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    cdrs["all_nos"] = False

    rem = operations.DropRow(condition_field="all_nos")
    story_data, all_logs = rem(cdrs)

    # all rows should still be there
    assert story_data.shape == (12, 4)
    assert story_data.columns.tolist() == ["A", "B", "duration", "all_nos"]
    assert story_data["A"].equals(cdrs["A"])
    assert story_data["B"].equals(cdrs["B"])
    assert story_data["duration"].equals(cdrs["duration"])


def test_drop_when_condition_is_all_true_should_remove_everything():

    cdrs = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    cdrs["all_yes"] = True

    rem = operations.DropRow(condition_field="all_yes")
    story_data, all_logs = rem(cdrs)

    # all rows should still be there
    assert story_data.shape == (0, 4)
    assert story_data.columns.tolist() == ["A", "B", "duration", "all_yes"]
    assert story_data["A"].equals(pd.Series())
    assert story_data["B"].equals(pd.Series())
    assert story_data["duration"].equals(pd.Series())


def test_drop_should_remove_the_rows_where_condition_is_true_():
    cdrs = pd.DataFrame(np.random.rand(12, 3), columns=["A", "B", "duration"])
    cdrs.index = build_ids(12, prefix="ix_", max_length=2)
    cdrs["cond"] = ([True] * 3 + [False] * 3) * 2

    rem = operations.DropRow(condition_field="cond")
    story_data, all_logs = rem(cdrs)

    kept_index = ["ix_03", "ix_04", "ix_05", "ix_09", "ix_10", "ix_11"]

    # 6 rows should have been removed
    assert story_data.shape == (6, 4)
    assert story_data.columns.tolist() == ["A", "B", "duration", "cond"]
    assert story_data["A"].equals(cdrs.loc[kept_index]["A"])
    assert story_data["B"].equals(cdrs.loc[kept_index]["B"])
    assert story_data["duration"].equals(cdrs.loc[kept_index]["duration"])


def test_increasing_bounded_sigmoid_must_reach_min_and_max_at_boundaries():

    freud = operations.bounded_sigmoid(x_min=2, x_max=15, shape=5,
                                       incrementing=True)

    # all values before x_min should be 0
    for x in np.linspace(-100, 2, 200):
        assert freud(x) == 0

    # all values after x_max should be 1
    for x in np.linspace(15, 100, 200):
        assert freud(x) == 1

    # all values in between should be in [0,1 ]
    for x in np.linspace(0, 1, 200):
        assert 0 <= freud(x) <= 1


def test_decreasing_bounded_sigmoid_must_reach_min_and_max_at_boundaries():

    freud = operations.bounded_sigmoid(x_min=2, x_max=15, shape=5,
                                       incrementing=False)

    # all values before x_min should be 1
    for x in np.linspace(-100, 2, 200):
        assert freud(x) == 1

    # all values after x_max should be 0
    for x in np.linspace(15, 100, 200):
        assert freud(x) == 0

    # all values in between should be in [0,1 ]
    for x in np.linspace(0, 1, 200):
        assert 0 <= freud(x) <= 1


def test_bounded_sigmoid_should_broadcast_as_a_ufunc():

    freud = operations.bounded_sigmoid(x_min=2, x_max=15, shape=5,
                                       incrementing=True)

    # passing a range of x should yield a range of y's
    for y in freud(np.linspace(-100, 2, 200)):
        assert y == 0

    # all values after x_max should be 1
    for y in freud(np.linspace(15, 100, 200)):
        assert y == 1

    # all values in between should be in [0,1 ]
    for y in freud(np.linspace(0, 1, 200)):
        assert 0 <= y <= 1


def test_bounding_function_should_not_modify_unbounded_values():
    bound_f = operations.bound_value(lb=None, ub=None)

    for x in np.arange(-1000, 2000, 10000):
        assert x == bound_f(x)


def test_bounded_generator_should_limnit_with_lower_bound():

    bound_f = operations.bound_value(lb=15)
    assert bound_f(10) == 15
    assert bound_f(15) == 15
    assert bound_f(20) == 20


def test_bounded_generator_should_limnit_with_upper_bound():

    bound_f = operations.bound_value(ub=15)
    assert bound_f(10) == 10
    assert bound_f(15) == 15
    assert bound_f(20) == 15


def test_bounded_generator_should_limnit_with_both_bound():

    bound_f = operations.bound_value(lb=10, ub=15)
    assert bound_f(5) == 10
    assert bound_f(10) == 10
    assert bound_f(12) == 12
    assert bound_f(15) == 15
    assert bound_f(20) == 15
