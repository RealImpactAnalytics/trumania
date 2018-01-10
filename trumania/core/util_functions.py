"""
Collection of utility functions
"""

from numpy.random import RandomState
import pandas as pd
import numpy as np
import os
import functools
from networkx.algorithms import bipartite
import logging


def make_random_bipartite_data(group1, group2, p, seed):
    """

    :type group1: list
    :param group1: Ids of first group
    :type group2: list
    :param group2: Ids of second group
    :type p: float
    :param p: probability of existence of 1 edge
    :type seed: int
    :param seed: seed for random generator
    :rtype: list
    :return: all edges in the graph
    """
    logging.info("  creating a bipartite graph between {} items in group1, {} "
                 "items in group2 and edge probability {}".format(
                    len(group1), len(group2), p))

    if len(group1) == 0 or len(group2) == 0 or p == 0:
        return []

    bp = pd.DataFrame.from_records(list(bipartite.random_graph(len(group1), len(group2), p, seed).edges()),
                                   columns=["from", "to"])
    logging.info("  (bipartite index created, now resolving item values)")

    # as all "to" nodes are from the second group,
    # but numbered by networkx in range(len(group1),len(group1)+len(group2))
    # we need to deduct len(group1) to have proper indexes.
    bp["to"] -= len(group1)

    bp["from"] = bp.apply(lambda x: group1[x["from"]], axis=1)
    bp["to"] = bp.apply(lambda x: group2[x["to"]], axis=1)
    logging.info("  (resolution done, now converting to tuples)")
    out = [tuple(x) for x in bp.to_records(index=False)]
    logging.info("  (exiting bipartite)")
    return out


def assign_random_proportions(name1, name2, group1, group2, seed):

    state = RandomState(seed)
    assignments = state.rand(len(group1), len(group2))
    assignments = assignments / assignments.sum(axis=1, keepdims=True)
    data = pd.DataFrame(assignments, index=group1,
                        columns=group2).stack().reset_index(level=[0, 1])
    data.rename(columns={"level_0": name1,
                         "level_1": name2,
                         0: "weight"},
                inplace=True)
    return data


def make_random_assign(set1, set2, seed):
    """Assign randomly a member of set2 to each member of set1
      :return: a dataframe with as many rows as set1
    """
    chosen_froms = RandomState(seed).choice(set2, size=len(set1))
    return pd.DataFrame({"set1": set1, "chosen_from_set2": chosen_froms})


def merge_2_dicts(dict1, dict2, value_merge_func=None):
    """
    :param dict1: first dictionary to be merged
    :param dict2: first dictionary to be merged
    :param value_merge_func: specifies how to merge 2 values if present in
    both dictionaries
    :type value_merge_func: function (value1, value) => value
    :return:
    """
    if dict1 is None and dict2 is None:
        return {}

    if dict2 is None:
        return dict1

    if dict1 is None:
        return dict2

    def merged_value(key):
        if key not in dict1:
            return dict2[key]
        elif key not in dict2:
            return dict1[key]
        else:
            if value_merge_func is None:
                raise ValueError(
                    "Conflict in merged dictionaries: merge function not "
                    "provided but key {} exists in both dictionaries".format(
                        key))

            return value_merge_func(dict1[key], dict2[key])

    keys = set(dict1.keys()) | set(dict2.keys())

    return {key: merged_value(key) for key in keys}


def df_concat(d1, d2):
    return pd.concat([d1, d2], ignore_index=True, copy=False)


def merge_dicts(dicts, merge_func=None):
    """
    :param dicts: list of dictionnaries to be merged
    :type dicts: list[dict]
    :param merge_func:
    :type merge_func: function
    :return: one single dictionary containing all entries received
    """
    from itertools import tee

    # check if the input list or iterator is empty
    dict_backup, test = tee(iter(dicts))
    try:
        next(test)
    except StopIteration:
        return {}

    return functools.reduce(lambda d1, d2: merge_2_dicts(d1, d2, merge_func), dict_backup)


def setup_logging():
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        level=logging.INFO)


# stolen from http://stackoverflow.com/questions/1835018/
# python-check-if-an-object-is-a-list-or-tuple-but-not-string#answer-1835259
def is_sequence(arg):
    return type(arg) is list or type(arg) is tuple or type(arg) is set


def build_ids(size, id_start=0, prefix="id_", max_length=10):
    """
    builds a sequencial list of string ids of specified size
    """
    return [prefix + str(x).zfill(max_length)
            for x in np.arange(id_start, id_start + size)]


def log_dataframe_sample(msg, df):

    if df.shape[0] == 0:
        logging.info("{}:  [empty]".format(msg))
    else:
        logging.info("{}: \n  {}".format(msg, df.sample(min(df.shape[0], 15))))


def cap_to_total(values, target_total):
    """
    return a copy of values with the largest values possible s.t.:
       - all return values are <= the original ones
       - their sum is == total
       -
    """

    excedent = np.sum(values) - target_total
    if excedent <= 0:
        return values
    elif values[-1] >= excedent:
        return values[:-1] + [values[-1] - excedent]
    else:
        return cap_to_total(values[:-1], target_total) + [0]


def ensure_folder_exists(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)


def ensure_non_existing_dir(folder):
    """
    makes sure the specified directory does not exist, potentially deleting
    any file or folder it contains
    """

    if not os.path.exists(folder):
        return

    if os.path.isfile(folder):
        os.remove(folder)

    else:
        for f in os.listdir(folder):
            full_path = os.path.join(folder, f)
            ensure_non_existing_dir(full_path)
        os.rmdir(folder)


def latest_date_before(starting_date, upper_bound, time_step):
    """
    Looks for the latest result_date s.t

        result_date = starting_date + n * time_step     for any integer n
        result_date <= upper_bound

    :type starting_date: pd.Timestamp
    :type upper_bound: pd.Timestamp
    :type time_step: pd.Timedelta
    :return: pd.Timestamp
    """

    result = starting_date

    while result > upper_bound:
        result -= time_step

    while upper_bound - result >= time_step:
        result += time_step

    return result


def load_all_logs(folder):
    """
    loads all csv file contained in this folder and retun them as one
    dictionary where the key is the filename without the extension
    """

    all_logs = {}

    for file_name in os.listdir(folder):
        full_path = os.path.join(folder, file_name)
        logs = pd.read_csv(full_path, index_col=None)
        log_id = file_name[:-4]

        all_logs[log_id] = logs

    return all_logs
