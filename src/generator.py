"""Base file for generator

"""

import pandas as pd
import networkx as nx

from random_generators import *
from actor import *
from relationship import *

def main():
    """

    :rtype: None
    """
    # Define parameters
    seed = 123456
    n_customers = 10


    # Define generators
    msisdn_gen = MSISDNGenerator("msisdn-test-1","0032",["472","473","475","476","477","478","479"],6,seed)
    timegen = GenericGenerator("time-generator","choice",{"a":12,"p":[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.05,0.05,0.05,0.05]},seed)
    networkchooser = WeightedChooserAggregator("B","weight",seed)
    networkweightgenerator = GenericGenerator("network-weight","pareto",{"a":1.2,"m":1.},seed)

    # Define Actors, Relationships, ...
    customers = CallerActor(n_customers)
    customers.add_attribute("MSISDN",msisdn_gen)
    customers.update_attribute("clock",timegen)

    social_network = pd.DataFrame.from_records(nx.fast_gnp_random_graph(n_customers,0.4,123456).edges(),columns=["A","B"])

    network = WeightedRelationship("A","B",networkchooser)
    network.add_relation("A",social_network["A"].values,"B",social_network["B"].values,networkweightgenerator.generate(len(social_network.index)))




if __name__ == "__main__":
    main()
