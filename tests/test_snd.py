from __future__ import division

import time
from datetime import datetime
import json

from bi.ria.generator.action import *
from bi.ria.generator.attribute import *
from bi.ria.generator.clock import *
from bi.ria.generator.circus import *
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
    agentweightgenerator = GenericGenerator("agent-weight", "exponential", {"scale": 1.})

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


    tcal = time.clock()
    print "Create callers"
    customers = Actor(size=n_agents_a,
                      prefix="AGENT_",
                      max_length=3)

    dealers = Actor(size=n_agents_b,
                    prefix="DEALER_",
                    max_length=3)

    dealer_sim_rel = Relationship(name="dealer to sim", seed=seed)

    sims_dealer = make_random_assign("SIM","DEALER",
                                     sims,
                                     dealers.ids,
                                     seed)
    dealer_sim_rel.add_relations(from_ids=sims_dealer["DEALER"],
                                 to_ids=sims_dealer["SIM"])

    print "Done"
    tatt = time.clock()

    customer_sim_rel = Relationship(name="agent to sim", seed=seed)

    # TODO: that's again an example of coupling between relationship and
    # transient attribute => review this
    # also, the "init_values=0" is coupled with the fact that the
    # Relationship has not been initialized with any data
    # => move the code of LabeledStockAttribute to attribute (which will all
    # be transient anyhow? )
    customer_sim_attr = LabeledStockAttribute(ids=customers.ids,
                                              init_values=0,
                                              relationship=customer_sim_rel)
    customers.add_attribute(name="SIM", attr=customer_sim_attr)

    dealer_sim_attr = LabeledStockAttribute(ids=dealers.ids,
                                            init_values=0,
                                            relationship=dealer_sim_rel)
    dealers.add_attribute(name="SIM", attr=dealer_sim_attr)

    print "Added atributes"
    tsna = time.clock()

    tmo = time.clock()
    print "Mobility"
    deg_prob = average_degree/n_agents_a*n_agents_b
    agent_customer_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.ids, dealers.ids, deg_prob, seed),
        columns=["AGENT", "DEALER"])
    print "Network created"
    tmoatt = time.clock()
    agent_customer = Relationship(name="agent to dealers",
                                  seed=seed)

    agent_customer.add_relations(from_ids=agent_customer_df["AGENT"],
                                 to_ids=agent_customer_df["DEALER"],
                                 weights=agentweightgenerator.generate(
                                    agent_customer_df.shape[0]))

    print "Done all customers"

    ######################################
    # Create circus
    ######################################
    tci = time.clock()
    print "Creating circus"
    flying = Circus(the_clock)

    purchase = ActorAction(name="purchase",
                           actor=customers,
                           actorid_field_name="AGENT",
                           random_relation_fields=[
                               {"picked_from": agent_customer,
                                "as": "DEALER",
                                "join_on": "AGENT"
                                },
                               {"picked_from": dealer_sim_rel,
                                "as": "SIM",
                                "join_on": "DEALER"
                                },
                           ],

                           time_generator=timegen,
                           activity_generator=activity_gen)

    # TODO: impacts should be closures
    purchase.add_impact(name="transfer sim",
                        attribute="SIM",
                        function="transfer_item",
                        parameters={"item": "SIM",
                                    "buyer_key": "AGENT",
                                    "seller_key": "DEALER",
                                    "seller": dealers
                                    })

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

