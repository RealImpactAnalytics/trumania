from datagenerator.util_functions import *


def test_merge_two_empty_dict_should_return_empty_dict():
    assert {} == merge_2_dicts({}, {})


def test_merge_empty_with_dict_should_return_itself():

    d1 = {"a": 1, "b": 2}
    assert d1 == merge_2_dicts(d1, {})
    assert d1 == merge_2_dicts({}, d1)


def test_merge_non_overlapping_dict_should_return_all_values():

    d1 = {"a": 1, "b": 2}
    d2 = {"c": 3, "d": 4}
    assert {"a": 1, "b": 2, "c": 3, "d": 4} == merge_2_dicts(d1, d2)


def test_merge_dict_to_itslef_should_return_doubled_values():

    d1 = {"a": 1, "b": 2}

    assert {"a": 2, "b": 4} == merge_2_dicts(d1, d1, lambda a, b: a+b)


def test_merging_one_dictionary_should_yield_itself():
    d1 = {"a": 1, "b": 2}
    assert d1 == merge_dicts([d1], lambda a, b: a+b)


def test_merging_many_dictionary_should_yield_expected_result():
    d1 ={"a": 10, "b": 20}
    d2 ={"a": 100, "c": 30}
    d3 ={}
    d4 ={"b": 200, "z": 1000}
    d5 = {"z": -10}

    merged = merge_dicts([d1, d2, d3, d4, d5], lambda a, b: a+b)

    assert {"a": 110, "b": 220, "c": 30, "z": 990} == merged
