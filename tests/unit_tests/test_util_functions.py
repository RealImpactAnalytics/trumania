import pandas as pd
import functools

from trumania.core.util_functions import merge_2_dicts, merge_dicts, is_sequence, make_random_assign, cap_to_total
from trumania.core.util_functions import build_ids, latest_date_before, bipartite, make_random_bipartite_data


def test_merge_two_empty_dict_should_return_empty_dict():
    assert {} == merge_2_dicts({}, {})


def test_merge_two_none_dict_should_return_empty_dict():
    assert {} == merge_2_dicts(None, None)


def test_merging_one_dict_with_none_should_yield_dict():
    d1 = {"a": 1, "b": 2}
    assert d1 == merge_2_dicts(d1, None)


def test_merging_none_with_one_dict_should_yield_dict():
    d2 = {"a": 1, "b": 2}
    assert d2 == merge_2_dicts(None, d2)


def test_merge_empty_with_dict_should_return_itself():

    d1 = {"a": 1, "b": 2}
    assert d1 == merge_2_dicts(d1, {})
    assert d1 == merge_2_dicts({}, d1)


def test_merge_non_overlapping_dict_should_return_all_values():

    d1 = {"a": 1, "b": 2}
    d2 = {"c": 3, "d": 4}
    assert {"a": 1, "b": 2, "c": 3, "d": 4} == merge_2_dicts(d1, d2)


def test_merge_dict_to_itself_should_return_doubled_values():

    d1 = {"a": 1, "b": 2}
    assert {"a": 2, "b": 4} == merge_2_dicts(d1, d1, lambda a, b: a + b)


def test_merging_one_dictionary_should_yield_itself():
    d1 = {"a": 1, "b": 2}
    assert d1 == merge_dicts([d1], lambda a, b: a + b)


def test_merging_an_empty_list_of_dicts_should_yield_empty_dict():
    assert {} == merge_dicts([])


def test_merging_an_empty_gen_of_dicts_should_yield_empty_dict():
    emtpy_gen = ({"a": 1} for _ in [])
    assert {} == merge_dicts(emtpy_gen)


def test_merging_many_dictionary_should_yield_expected_result():
    d1 = {"a": 10, "b": 20}
    d2 = {"a": 100, "c": 30}
    d3 = {}
    d4 = {"b": 200, "z": 1000}
    d5 = {"z": -10}

    merged = merge_dicts([d1, d2, d3, d4, d5], lambda a, b: a + b)

    assert {"a": 110, "b": 220, "c": 30, "z": 990} == merged


def test_merging_many_dictionary_from_gen_should_yield_expected_result():
    ds = [{"a": 10, "b": 20},
          {"a": 100, "c": 30},
          {},
          {"b": 200, "z": 1000},
          {"z": -10}]

    dicts_gens = (d for d in ds)

    merged = merge_dicts(dicts_gens, lambda a, b: a + b)

    assert {"a": 110, "b": 220, "c": 30, "z": 990} == merged


def test_is_sequence():
    assert is_sequence([])
    assert is_sequence([1, 2, 3, 1])
    assert is_sequence({1, 2, 3, 1})
    assert not is_sequence(1)
    assert not is_sequence("hello")


def test_make_random_assign_shoud_assign_each_element_only_once():

    dealers = build_ids(size=10, prefix="DEALER_", max_length=2)
    sims = build_ids(size=1000, prefix="SIM_", max_length=4)

    assignment = make_random_assign(set1=sims, set2=dealers, seed=10)

    # all sims should have been assigned
    assert assignment.shape == (1000, 2)

    # all SIM should have been given
    assert set(assignment["set1"].unique().tolist()) == set(sims)

    # all owners should be part of the dealers
    assert set(assignment["chosen_from_set2"].unique().tolist()) <= set(dealers)


def test_cap_to_total_should_leave_untouched_values_below_target():
    assert [10, 20, 30] == cap_to_total([10, 20, 30], target_total=100)


def test_cap_to_total_should_leave_untouched_equal_to_target():
    assert [50, 40, 20] == cap_to_total([50, 40, 20], target_total=110)


def test_cap_to_total_should_lower_last_correctly():
    assert [50, 40, 5] == cap_to_total([50, 40, 20], target_total=95)


def test_cap_to_total_should_zero_last_correctly():
    assert [50, 40, 0] == cap_to_total([50, 40, 20], target_total=90)


def test_cap_to_total_should_zero_several_correctly():
    assert [38, 0, 0] == cap_to_total([50, 40, 20], target_total=38)


def test_latest_date_before_should_return_input_if_within_range():

    starting_date = pd.Timestamp("6 June 2016")
    upper_bound = pd.Timestamp("8 June 2016")
    time_step = pd.Timedelta("7D")

    result = latest_date_before(starting_date, upper_bound, time_step)

    assert result == starting_date


def test_latest_date_before_should_return_input_if_start_equals_ub():

    starting_date = pd.Timestamp("8 June 2016")
    upper_bound = pd.Timestamp("8 June 2016")
    time_step = pd.Timedelta("7D")

    result = latest_date_before(starting_date, upper_bound, time_step)

    assert result == starting_date


def test_latest_date_before_should_shift_backward_ne_week_input_as_required():

    starting_date = pd.Timestamp("10 June 2016")
    expected_date = pd.Timestamp("3 June 2016")
    upper_bound = pd.Timestamp("8 June 2016")
    time_step = pd.Timedelta("7D")

    result = latest_date_before(starting_date, upper_bound, time_step)

    assert result == expected_date


def test_latest_date_before_should_shift_backward_n_weeks_input_as_required():

    starting_date = pd.Timestamp("10 June 2016")
    expected_date = pd.Timestamp("25 March 2016")
    upper_bound = pd.Timestamp("31 March 2016")
    time_step = pd.Timedelta("7D")

    result = latest_date_before(starting_date, upper_bound, time_step)

    assert result == expected_date


def test_latest_date_before_should_shift_forward_n_weeks_input_as_required():

    starting_date = pd.Timestamp("10 June 2016")
    expected_date = pd.Timestamp("27 January 2017")
    upper_bound = pd.Timestamp("29 January 2017")
    time_step = pd.Timedelta("7D")

    result = latest_date_before(starting_date, upper_bound, time_step)

    assert result == expected_date


def test_latest_date_before_should_shift_forward_until_upper_bound():

    # here the upper bound IS the expected date => makes sure we go up to
    # thsi ons
    starting_date = pd.Timestamp("10 June 2016")
    upper_bound = pd.Timestamp("24 June 2016")
    time_step = pd.Timedelta("7D")

    result = latest_date_before(starting_date, upper_bound, time_step)

    assert result == upper_bound


def test_if_networkx_bipartite_keeps_actual_structure():

    # Currently, Netorkx.bipartite returns bipartite networks where the first node
    # is always in the first group, and the second node is always in the second group
    RB = bipartite.random_graph(5, 10, 0.9, 1234)

    assert functools.reduce(lambda x, y: x & y, [e[0] < 5 for e in RB.edges()])


def test_random_bipartite_network_generation_returns_empty_list_if_first_entry_is_empty():

    assert [] == make_random_bipartite_data([], [1, 2], 1., 1234)


def test_random_bipartite_network_generation_returns_empty_list_if_second_entry_is_empty():

    assert [] == make_random_bipartite_data([1, 2], [], 1., 1234)


def test_random_bipartite_network_generation_returns_empty_list_if_both_entries_are_empty():

    assert [] == make_random_bipartite_data([], [], 1., 1234)


def test_random_bipartite_network_generation_returns_empty_list_if_prob_is_zero():

    assert [] == make_random_bipartite_data([1, 2], [5, 6], 0., 1234)


def test_random_bipartite_network_generation_returns_bipartite_network():

    all_edges = [(1, 5), (1, 6), (2, 5), (2, 6)]
    bp = make_random_bipartite_data([1, 2], [5, 6], 1., 1234)

    assert functools.reduce(lambda x, y: x & y, [e in bp for e in all_edges])
