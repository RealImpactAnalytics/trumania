from __future__ import division

import time
from datetime import datetime
import json

from bi.ria.generator.action import *
from bi.ria.generator.clock import *
from bi.ria.generator.circus import *
from bi.ria.generator.product import *
from bi.ria.generator.random_generators import *
from bi.ria.generator.relationship import *
from bi.ria.generator.util_functions import *

from bi.ria.generator.actor import *


# AgentA: has stock of SIMs
# AgentB: has stock of SIMs
# SIMs: has ID
# AgentA buys stock to AgentB

def compose_circus():
    """
        Builds a circus simulating SND activity.
        see test case below
    """
    ######################################
    # Define parameters
    ######################################
    tp = time.clock()
    print "Parameters"

    seed = 123456
    n_agents_a = 1000
    n_agents_b = 100
    average_degree = 20
    n_sims = 500000

    prof = pd.Series([5., 5., 5., 5., 5., 3., 3.],
                     index=[timedelta(days=x, hours=23, minutes=59, seconds=59) for x in range(7)])
    time_step = 60

    agents_a = ["AGENT_%s" % (str(i).zfill(3)) for i in range(n_agents_a)]
    agents_b = ["DEALER_%s" % (str(i).zfill(3)) for i in range(n_agents_b)]
    sims = ["SIM_%s" % (str(i).zfill(6)) for i in range(n_sims)]

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
    activity_gen = GenericGenerator("user-activity", "choice", {"a": range(1,4)}, seed)
    timegen = WeekProfiler(time_step, prof, seed)

    agentchooser = WeightedChooserAggregator("AGENT", "weight", seed)
    agentweightgenerator = GenericGenerator("agent-weight", "exponential", {"scale": 1.})

    sim_agent_chooser = ChooserAggregator(seed)
    sim_dealer_chooser = ChooserAggregator(seed)

    print "Done"


    ######################################
    # Initialise generators
    ######################################
    tig = time.clock()
    print "initialise Time Generators"
    timegen.initialise(the_clock)
    print "Done"


    ######################################
    # Define Actors, Relationships, ...
    ######################################
    # Assign all sims to a dealer to start

    customer_sim_rel = Relationship("AGENT","ITEM",sim_agent_chooser)
    dealer_sim_rel = Relationship("AGENT","ITEM",sim_dealer_chooser)

    tcal = time.clock()
    print "Create callers"
    customers = Actor(n_agents_a,prefix="AGENT_",max_length=3)
    dealers = Actor(n_agents_b,prefix="DEALER_",max_length=3)

    sims_dealer = make_random_assign("SIM","DEALER",sims,dealers.get_ids(),seed)
    dealer_sim_rel.add_relation("AGENT",sims_dealer["DEALER"].values,"ITEM",sims_dealer["SIM"].values)

    print "Done"
    tatt = time.clock()
    #customers.update_attribute("activity", activity_gen)
    #customers.update_attribute("clock", timegen, weight_field="activity")
    customers.add_transient_attribute("SIM","labeled_stock",params={"relationship":customer_sim_rel})
    dealers.add_transient_attribute("SIM","labeled_stock",params={"relationship":dealer_sim_rel})

    print "Added atributes"
    tsna = time.clock()

    tmo = time.clock()
    print "Mobility"
    deg_prob = float(average_degree)/float(n_agents_a*n_agents_b)
    agent_customer_df = pd.DataFrame.from_records(make_random_bipartite_data(customers.get_ids(), dealers.get_ids(), deg_prob, seed),
        columns=["AGENT", "DEALER"])
    print "Network created"
    tmoatt = time.clock()
    agent_customer = WeightedRelationship("AGENT", "DEALER", agentchooser)
    agent_customer.add_relation("AGENT", agent_customer_df["AGENT"], "DEALER", agent_customer_df["DEALER"],
                          agentweightgenerator.generate(len(agent_customer_df.index)))


    print "Done all customers"

    ######################################
    # Create circus
    ######################################
    tci = time.clock()
    print "Creating circus"
    flying = Circus(the_clock)
    flying.add_actor("customers", customers)
    flying.add_actor("dealer",dealers)

    purchase = ActorAction("purchase",customers,timegen,activity_gen)
    purchase.add_secondary_actor("DEALER",dealers)
    purchase.add_relationship("customer_dealer",agent_customer)
    purchase.add_relationship("customer_sim",customer_sim_rel)
    purchase.add_relationship("dealer_sim",dealer_sim_rel)
    purchase.add_field("DEALER","customer_dealer",{"key":"AGENT"})
    purchase.add_secondary_field("SIM","dealer_sim",{"key_table":"DEALER","key_rel":"AGENT","out_rel":"ITEM"})
    purchase.add_impact("transfer sim","SIM","transfer_item",
                        {"item":"SIM","buyer_key":"AGENT","seller_key":"DEALER","seller_table":"DEALER"})

    flying.add_action(purchase)

    flying.add_increment(timegen)
    print "Done"

    tr = time.clock()
    all_times = {"parameters":tc-tp,
             "clocks":tg-tc,
             "generators":tig-tg,
             "init generators": tcal-tig,
             "callers creation (full)":tmo-tcal,
             "caller creation (solo)":tatt-tcal,
             "caller attribute creation": tsna-tatt,
             "mobility graph creation": tmoatt-tmo,
             "mobility attribute creation": tci - tmoatt,
             "circus creation": tr-tci,
             "tr": tr
                 }

    return flying, all_times


def test_cdr_scenario():

    snd_circus, all_times = compose_circus()
    n_iterations = 100

    [all_purchases] = snd_circus.run(n_iterations)
    tf = time.clock()

    all_times["runs (all)"] = tf - all_times["tr"]
    all_times["one run (average)"] = (tf - all_times["tr"]) / n_iterations

    print (json.dumps(all_times, indent=2))

    print ("""
        some purchase events:
          {}

    """.format(all_purchases.head()))

    assert all_purchases.shape[0] > 0
    assert "datetime" in all_purchases.columns

    # TODO: add real post-conditions on all_purchases

