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
from action import *
from product import *

import time


def main():
    """

    :rtype: tuple
    """
    ######################################
    # Define parameters
    ######################################
    tp = time.clock()
    print "Parameters"

    seed = 123456
    n_customers = 100
    n_iterations = 10
    n_cells = 100
    n_agents = 100
    average_degree = 20

    prof = pd.Series([5., 5., 5., 5., 5., 3., 3.],
                     index=[timedelta(days=x, hours=23, minutes=59, seconds=59) for x in range(7)])
    time_step = 3600

    mov_prof = pd.Series(
        [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.],
        index=[timedelta(hours=h, minutes=59, seconds=59) for h in range(24)])

    cells = ["CELL_%s" % (str(i).zfill(4)) for i in range(n_cells)]
    agents = ["AGENT_%s" % (str(i).zfill(3)) for i in range(n_agents)]

    products = ["VOICE","SMS"]

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

    agentchooser = WeightedChooserAggregator("AGENT", "weight", seed)
    agentweightgenerator = GenericGenerator("agent-weight", "exponential", {"scale": 1.})

    init_mobility_generator = GenericGenerator("init-mobility", "choice", {"a": cells})

    SMS_price_generator = GenericGenerator("SMS-price","constant",{"a":10.})
    voice_duration_generator = GenericGenerator("voice-duration","exponential",{},seed)
    voice_price_generator = ValueGenerator("voice-price",1)
    productchooser = WeightedChooserAggregator("PRODUCT", "weight", seed)

    recharge_init = GenericGenerator("recharge init","constant",{"a":5})
    recharge_trigger = TriggerGenerator("Topup","logistic",{},seed)
    recharge_time_init = GenericGenerator("recharge_init","constant",{"a":-1})
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
    social_network = create_er_social_network(customers.get_ids(), float(average_degree) / float(n_customers), seed)
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

    customers.add_transient_attribute("CELL", "choice", init_mobility_generator, mobilitytimegen)

    agent_df = pd.DataFrame.from_records(make_random_bipartite_data(customers.get_ids(), agents, 0.3, seed),
        columns=["A", "AGENT"])
    print "Agent relationship created"
    tagatt = time.clock()
    agent_rel = AgentRelationship("A", "AGENT", agentchooser)
    agent_rel.add_relation("A", agent_df["A"], "AGENT", agent_df["AGENT"],
                          agentweightgenerator.generate(len(agent_df.index)))

    customers.add_transient_attribute("MAIN_ACCT","stock",recharge_init,recharge_time_init,None,{"trigger_generator":recharge_trigger})
    print "Done all customers"

    voice = VoiceProduct(voice_duration_generator,voice_price_generator)
    sms = SMSProduct(SMS_price_generator)

    product_df = assign_random_proportions("A","PRODUCT",customers.get_ids(),products,seed)
    product_rel = ProductRelationship("A","PRODUCT",productchooser,{"VOICE":voice,"SMS":sms})
    product_rel.add_relation("A",product_df["A"],"PRODUCT",product_df["PRODUCT"],product_df["weight"])

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

    calls = ActorAction("calls",customers,timegen)
    calls.add_relationship("network",network)
    calls.add_relationship("product",product_rel)
    calls.add_field("B","network",{"key":"A"})
    calls.add_field("PRODUCT","product",{"key":"A"})
    calls.add_impact("value decrease","MAIN_ACCT","decrease_stock","VALUE")

    topup = AttributeAction("topup",customers,"MAIN_ACCT",{"relationship":agent_rel,"id1":"A","id2":"AGENT","id3":"value"})

    mobility = AttributeAction("mobility",customers,"CELL",{"new_time_generator": mobilitytimegen,
                                                       "relationship": mobility,
                                                       "id1": "A",
                                                       "id2": "CELL"})

    flying.add_action(calls,{"timestamp": True,
                       "join": [("A", customers, "MSISDN", "A_NUMBER"),
                                ("B", customers, "MSISDN", "B_NUMBER"),
                                ("A", customers, "CELL", "CELL_A"),
                                ("B", customers, "CELL", "CELL_B"),]})

    flying.add_action(mobility,{"timestamp":True})

    flying.add_action(topup,{"timestamp":True,
                             "join":[("A",customers, "MSISDN", "CUSTOMER_NUMBER"),
                                     ("A",customers, "CELL", "CELL")]})


    flying.add_increment(timegen)
    print "Done"


    ######################################
    # Run
    ######################################
    tr = time.clock()
    print "Start run"
    all_cdrs = []
    all_mov = []
    all_topup = []
    for i in range(n_iterations):
        print "iteration %s on %s" % (i,n_iterations),
        all_data = flying.one_round()
        # print len(these_cdrs.index), "CDRs generated"
        all_cdrs.append(all_data[0])
        all_mov.append(all_data[1])
        all_topup.append(all_data[2])
        print '\r',
    tf = time.clock()

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

    return flying, \
           pd.concat(all_cdrs, ignore_index=True),\
           pd.concat(all_mov,ignore_index=True), \
           pd.concat(all_topup,ignore_index=True),\
           all_times



if __name__ == "__main__":
    main()
