"""Collection of utility functions

"""

from numpy.random import RandomState
import pandas as pd
import numpy as np
import os
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
    bp_network = bipartite.random_graph(len(group1), len(group2), 0.4, seed)
    i1 = 0
    i2 = 0
    node_index = {}
    node_type = {}
    for n, d in bp_network.nodes(data=True):
        if d["bipartite"] == 0:
            node_index[n] = i1
            i1 += 1
        else:
            node_index[n] = i2
            i2 += 1
        node_type[n] = d["bipartite"]
    edges_for_out = []
    for e in bp_network.edges():
        if node_type[e[0]] == 0:
            edges_for_out.append((group1[node_index[e[0]]], group2[node_index[e[1]]]))
        else:
            edges_for_out.append((group2[node_index[e[1]]], group1[node_index[e[0]]]))
    return edges_for_out


def assign_random_proportions(name1,name2,group1,group2,seed):

    state = RandomState(seed)
    assignments = state.rand(len(group1),len(group2))
    assignments = assignments/assignments.sum(axis=1,keepdims=True)
    data = pd.DataFrame(assignments,index=group1,columns=group2).stack().reset_index(level=[0,1])
    data.rename(columns={"level_0": name1,
                         "level_1": name2,
                         0: "weight"},
                inplace=True)
    return data


def make_random_assign(owned, owners, seed):
    """Assign randomly each member from owner to a member of own
    """
    choices = RandomState(seed).choice(owners, size=len(owned))
    return pd.DataFrame({"from": choices, "to": owned})


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
        test.next()
    except StopIteration:
        return {}

    return reduce(lambda d1, d2: merge_2_dicts(d1, d2, merge_func), dict_backup)


def setup_logging():
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        level=logging.INFO)


# stolen from http://stackoverflow.com/questions/1835018/python-check-if-an-object-is-a-list-or-tuple-but-not-string#answer-1835259
def is_sequence(arg):
    return (not hasattr(arg, "strip") and
            hasattr(arg, "__getitem__") or
            hasattr(arg, "__iter__"))


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


def ensure_non_existing_dir(path):
    """
    makes sure the specified directory does not exist, potentially deleting
    any file or folder it contains
    """

    if not os.path.exists(path):
        return

    if os.path.isfile(path):
        os.remove(path)

    else:
        for f in os.listdir(path):
            full_path = os.path.join(path, f)
            ensure_non_existing_dir(full_path)
        os.rmdir(path)
