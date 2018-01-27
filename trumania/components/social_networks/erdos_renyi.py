from __future__ import division
import logging
import networkx as nx
import trumania.core.random_generators as rg
from trumania.core.circus import Circus

import pandas as pd


class WithErdosRenyi(Circus):
    """
        Circus mix-in that provides method to build ER random graph
    """

    def add_er_social_network_relationship(self, population, relationship_name, average_degree):
        """
        Adds to this population a relationship from and to its members based an ER random graph
        """
        logging.info("Creating the social network ")

        # create a random A to B symmetric relationship
        network_weight_gen = rg.ParetoGenerator(xmin=1., a=1.2, seed=next(self.seeder))

        social_network_values = create_er_social_network(
            customer_ids=population.ids,
            p=average_degree / len(population.ids),
            seed=next(self.seeder))

        social_network = population.create_relationship(relationship_name)
        social_network.add_relations(
            from_ids=social_network_values["A"].values,
            to_ids=social_network_values["B"].values,
            weights=network_weight_gen.generate(social_network_values.shape[0]))

        social_network.add_relations(
            from_ids=social_network_values["B"].values,
            to_ids=social_network_values["A"].values,
            weights=network_weight_gen.generate(social_network_values.shape[0]))


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

    return pd.DataFrame.from_records([(customer_ids[e[0]], customer_ids[e[1]])
                                      for e in nx.fast_gnp_random_graph(len(customer_ids), p, seed).edges()],
                                     columns=["A", "B"])
