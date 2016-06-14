"""Base file for generator

"""
from datetime import datetime
import pandas as pd
from random_generators import *
from clock import *
from actor import *
from relationship import *
from circus import *
from util_functions import *

import time


def main():
    """

    :rtype: None
    """
    ######################################
    # Define parameters
    ######################################
    tp = time.clock()
    print "Parameters"

    seed = 123456
    n_customers = 10000
    n_iterations = 10
    n_cells = 100
    average_degree = 20

    prof = pd.Series([5., 5., 5., 5., 5., 3., 3.],
                     index=[timedelta(days=x, hours=23, minutes=59, seconds=59) for x in range(7)])
    time_step = 3600

    mov_prof = pd.Series(
        [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.],
        index=[timedelta(hours=h, minutes=59, seconds=59) for h in range(24)])

    cells = ["CELL_%s" % (str(i).zfill(4)) for i in range(n_cells)]

    print "Done"
    ######################################
    # Define clocks
    ######################################
    tc = time.clock()
    print "Clock"
    the_clock = Clock(datetime(year=2016, month=6, day=8), time_step, "%d%m%Y %H:%M:%S", seed)
    print "Done"
    ######################################
    # Define generators
    ######################################
    tg = time.clock()
    print "Generators"
    msisdn_gen = MSISDNGenerator("msisdn-test-1", "0032", ["472", "473", "475", "476", "477", "478", "479"], 6, seed)
    activity_gen = GenericGenerator("user-activity", "pareto", {"a": 1.2, "m": 10.}, seed)
    timegen = WeekProfiler(time_step, prof, seed)

    mobilitytimegen = DayProfiler(time_step, mov_prof, seed)
    networkchooser = WeightedChooserAggregator("B", "weight", seed)
    networkweightgenerator = GenericGenerator("network-weight", "pareto", {"a": 1.2, "m": 1.}, seed)

    mobilitychooser = WeightedChooserAggregator("CELL", "weight", seed)
    mobilityweightgenerator = GenericGenerator("mobility-weight", "exponential", {"scale": 1.})

    init_mobility_generator = GenericGenerator("init-mobility", "choice", {"a": cells})
    print "Done"
    ######################################
    # Initialise generators
    ######################################
    tig = time.clock()
    print "initialise Time Generators"
    timegen.initialise(the_clock)
    mobilitytimegen.initialise(the_clock)
    print "Done"
    ######################################
    # Define Actors, Relationships, ...
    ######################################
    tcal = time.clock()
    print "Create callers"
    customers = CallerActor(n_customers)
    print "Done"
    tatt = time.clock()
    customers.add_attribute("MSISDN", msisdn_gen)
    customers.update_attribute("activity", activity_gen)
    customers.update_attribute("clock", timegen, weight_field="activity")

    print "Added atributes"
    tsna = time.clock()
    print "Creating social network"
    social_network = create_ER_social_network(customers.get_ids(), float(average_degree)/float(n_customers), seed)
    tsnaatt = time.clock()
    print "Done"
    network = WeightedRelationship("A", "B", networkchooser)
    network.add_relation("A", social_network["A"].values, "B", social_network["B"].values,
                         networkweightgenerator.generate(len(social_network.index)))
    network.add_relation("A", social_network["B"].values, "B", social_network["A"].values,
                         networkweightgenerator.generate(len(social_network.index)))
    print "Done SNA"
    tmo = time.clock()
    print "Mobility"
    mobility_df = pd.DataFrame.from_records(make_random_bipartite_data(customers.get_ids(), cells, 0.4, seed),
        columns=["A", "CELL"])
    print "Network created"
    tmoatt = time.clock()
    mobility = WeightedRelationship("A", "CELL", mobilitychooser)
    mobility.add_relation("A", mobility_df["A"], "CELL", mobility_df["CELL"],
                          mobilityweightgenerator.generate(len(mobility_df.index)))

    customers.add_transient_attribute("CELL", init_mobility_generator, mobilitytimegen)
    print "Done all customers"
    ######################################
    # Create circus
    ######################################
    tci = time.clock()
    print "Creating circus"
    flying = Circus(the_clock)
    flying.add_actor("customers", customers)
    flying.add_relationship("A", "B", network)
    flying.add_generator("time", timegen)
    flying.add_generator("networkchooser", networkchooser)

    flying.add_action("customers",
                      "make_calls",
                      {"new_time_generator": timegen, "relationship": network},
                      {"timestamp": True,
                       "join": [("A", customers, "MSISDN", "A_NUMBER"),
                                ("B", customers, "MSISDN", "B_NUMBER"),
                                ("A", customers, "CELL", "CELL_A"),
                                ("B", customers, "CELL", "CELL_B"),]})
    flying.add_action("customers",
                      "make_attribute_action",
                      {"attr_name": "CELL", "params": {"new_time_generator": mobilitytimegen,
                                                       "relationship": mobility,
                                                       "id1": "A",
                                                       "id2": "CELL"}},
                      {"timestamp": True})

    flying.add_increment(timegen)
    print "Done"
    ######################################
    # Run
    ######################################
    tr = time.clock()
    print "Start run"
    all_cdrs = []
    all_mov = []
    for i in range(n_iterations):
        print "iteration %s on %s" % (i,n_iterations),
        all_data = flying.one_round()
        # print len(these_cdrs.index), "CDRs generated"
        all_cdrs.append(all_data[0])
        all_mov.append(all_data[1])
        print '\r',
    tf = time.clock()

    #all_times = [tp,tc,tg,tig,tcal,tatt,tsna,tsnaatt,tmo, tmoatt,tci,tr,tf]
    all_times = {"parameters":tc-tp,
                 "clocks":tg-tc,
                 "generators":tig-tg,
                 "init generators": tcal-tig,
                 "callers creation (full)":tmo-tcal,
                 "caller creation (solo)":tatt-tcal,
                 "caller attribute creation": tsna-tatt,
                 "caller SNA graph creation":tsnaatt-tsna,
                 "mobility graph creation": tmoatt-tmo,
                 "mobility attribute creation": tci - tmoatt,
                 "circus creation": tr-tci,
                 "runs (all)": tf-tr,
                 "one run (average)": (tf-tr)/float(n_iterations)}

    return (flying, pd.concat(all_cdrs, ignore_index=True),pd.concat(all_mov,ignore_index=True),all_times)


if __name__ == "__main__":
    main()
