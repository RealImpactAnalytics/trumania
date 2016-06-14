"""Collection of utility functions

"""

import pandas as pd
import networkx as nx
from networkx.algorithms import bipartite

def create_ER_social_network(customer_ids,p,seed):
    """

    :type customer_ids: list
    :param customer_ids: list of IDs as defined in the data
    :type p: float
    :param p: probability of existence of 1 edge
    :type seed: int
    :param seed: seed for random generator
    :rtype: Pandas DataFrame, with two columns (A and B)
    :return: all edges in the graph
    """

    ERG = pd.DataFrame.from_records(nx.fast_gnp_random_graph(len(customer_ids), p, seed).edges(),
                                             columns=["A", "B"])

    custs = dict([(i,customer_ids[i]) for i in range(len(customer_ids))])
    ERG.replace({"A":custs,"B":custs})
    return ERG


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
