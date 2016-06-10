"""Base file for generator

"""
from datetime import datetime

import pandas as pd
import networkx as nx
from random_generators import *
from clock import *
from actor import *
from relationship import *
from circus import *


def main():
    """

    :rtype: None
    """
    ######################################
    # Define parameters
    ######################################
    seed = 123456
    n_customers = 10
    prof = pd.Series([5.,5.,5.,5.,5.,3.,3.],
                        index=[timedelta(hours=23,minutes=59,seconds=59),
                       timedelta(days=1,hours=23,minutes=59,seconds=59),
                       timedelta(days=2,hours=23,minutes=59,seconds=59),
                       timedelta(days=3,hours=23,minutes=59,seconds=59),
                       timedelta(days=4,hours=23,minutes=59,seconds=59),
                       timedelta(days=5,hours=23,minutes=59,seconds=59),
                       timedelta(days=6,hours=23,minutes=59,seconds=59)])
    time_step = 3600

    ######################################
    # Define clocks
    ######################################
    the_clock = Clock(datetime(year=2016,month=6,day=8),time_step,"%d%m%Y %H:%M:%S",seed)

    ######################################
    # Define generators
    ######################################
    msisdn_gen = MSISDNGenerator("msisdn-test-1", "0032", ["472", "473", "475", "476", "477", "478", "479"], 6, seed)
    activity_gen = GenericGenerator("user-activity","pareto",{"a": 1.2, "m":10.}, seed)
    timegen = TimeProfiler(time_step,prof,seed)
    networkchooser = WeightedChooserAggregator("B", "weight", seed)
    networkweightgenerator = GenericGenerator("network-weight", "pareto", {"a": 1.2, "m": 1.}, seed)

    ######################################
    # Initialise generators
    ######################################
    timegen.initialise(the_clock)

    ######################################
    # Define Actors, Relationships, ...
    ######################################
    customers = CallerActor(n_customers)
    customers.add_attribute("MSISDN", msisdn_gen)
    customers.update_attribute("activity",activity_gen)
    customers.update_attribute("clock", timegen,weight_field="activity")

    social_network = pd.DataFrame.from_records(nx.fast_gnp_random_graph(n_customers, 0.4, 123456).edges(),
                                               columns=["A", "B"])

    network = WeightedRelationship("A", "B", networkchooser)
    network.add_relation("A", social_network["A"].values, "B", social_network["B"].values,
                         networkweightgenerator.generate(len(social_network.index)))

    ######################################
    # Create circus
    ######################################
    flying = Circus(the_clock)
    flying.add_actor("customers",customers)
    flying.add_relationship("A","B",network)
    flying.add_generator("time",timegen)
    flying.add_generator("networkchooser",networkchooser)

    flying.add_action("customers","make_calls",{"new_time_generator":timegen,"relationship":network})
    flying.add_increment(timegen)

    ######################################
    # Run
    ######################################

    all_cdrs = []
    for i in range(100):
        these_cdrs = flying.one_round()[0]
        print len(these_cdrs.index), "CDRs generated"
        all_cdrs.append(these_cdrs)

    return (flying,pd.concat(all_cdrs,ignore_index=True))



def old_main():
    """

    :rtype: None
    """
    ######################################
    # Define parameters
    ######################################
    seed = 123456
    n_customers = 10
    prof = pd.Series([5.,5.,5.,5.,5.,3.,3.],
                        index=[timedelta(hours=23,minutes=59,seconds=59),
                       timedelta(days=1,hours=23,minutes=59,seconds=59),
                       timedelta(days=2,hours=23,minutes=59,seconds=59),
                       timedelta(days=3,hours=23,minutes=59,seconds=59),
                       timedelta(days=4,hours=23,minutes=59,seconds=59),
                       timedelta(days=5,hours=23,minutes=59,seconds=59),
                       timedelta(days=6,hours=23,minutes=59,seconds=59)])
    time_step = 3600

    ######################################
    # Define clocks
    ######################################
    the_clock = Clock(datetime(year=2016,month=6,day=8),time_step,"%d%m%Y %H:%M:%S",seed)

    ######################################
    # Define generators
    ######################################
    msisdn_gen = MSISDNGenerator("msisdn-test-1", "0032", ["472", "473", "475", "476", "477", "478", "479"], 6, seed)
    activity_gen = GenericGenerator("user-activity","pareto",{"a": 1.2, "m":10.}, seed)
    timegen = TimeProfiler(time_step,prof,seed)
    networkchooser = WeightedChooserAggregator("B", "weight", seed)
    networkweightgenerator = GenericGenerator("network-weight", "pareto", {"a": 1.2, "m": 1.}, seed)

    ######################################
    # Initialise generators
    ######################################
    timegen.initialise(the_clock)

    ######################################
    # Define Actors, Relationships, ...
    ######################################
    customers = CallerActor(n_customers)
    customers.add_attribute("MSISDN", msisdn_gen)
    customers.update_attribute("activity",activity_gen)
    customers.update_attribute("clock", timegen,weight_field="activity")

    social_network = pd.DataFrame.from_records(nx.fast_gnp_random_graph(n_customers, 0.4, 123456).edges(),
                                               columns=["A", "B"])

    network = WeightedRelationship("A", "B", networkchooser)
    network.add_relation("A", social_network["A"].values, "B", social_network["B"].values,
                         networkweightgenerator.generate(len(social_network.index)))


    ######################################
    # Run
    ######################################
    def one_round():
        out = customers.make_actions(network,networkchooser,timegen)
        out["datetime"] = the_clock.get_timestamp(len(out.index))

        timegen.increment()
        the_clock.increment()
        return out

    all_cdrs = []
    for i in range(100):
        these_cdrs = one_round()
        print len(these_cdrs.index), "CDRs generated"
        all_cdrs.append(these_cdrs)

    return pd.concat(all_cdrs,ignore_index=True)


if __name__ == "__main__":
    main()
