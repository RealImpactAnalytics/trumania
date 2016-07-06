"""Collection of utility functions

"""

from numpy.random import RandomState
import pandas as pd
import networkx as nx
from networkx.algorithms import bipartite


def create_er_social_network(customer_ids, p, seed):
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

    return pd.DataFrame.from_records([(customer_ids[e[0]],customer_ids[e[1]])
                                     for e in nx.fast_gnp_random_graph(len(customer_ids), p, seed).edges()],
                                    columns=["A", "B"])


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
    data.rename(columns={"level_0":name1,"level_1":name2,0:"weight"},inplace=True)
    return data


def make_random_assign(name1,name2,group1,group2,seed):
    """Assign randomly each member from group1 to a member of group2

    """
    state = RandomState(seed)
    choices = state.choice(group2,size=len(group1))

    return pd.DataFrame({name2:choices,name1:group1})

