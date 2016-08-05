from __future__ import division

from datetime import datetime

import datagenerator.operations as operations
from datagenerator.relationship import *
from datagenerator.action import *
from datagenerator.actor import *
from datagenerator.attribute import *
from datagenerator.circus import *
from datagenerator.clock import *
from datagenerator.random_generators import *
from datagenerator.util_functions import *


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

    ######################################
    # Define clocks
    ######################################
    the_clock = Clock(datetime(year=2016, month=6, day=8), time_step, "%d%m%Y %H:%M:%S", seed)

    ######################################
    # Define generators
    ######################################
    activity_gen = GenericGenerator("user-activity", "choice", {"a": range(1,4)}, seed)
    timegen = WeekProfiler(time_step, prof, seed)
    agentweightgenerator = GenericGenerator("agent-weight", "exponential", {"scale": 1.})


    ######################################
    # Initialise generators
    ######################################
    timegen.initialise(the_clock)

    ######################################
    # Define Actors, Relationships, ...
    ######################################
    # Assign all sims to a dealer to start

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

    customer_sim_rel = Relationship(name="agent to sim", seed=seed)

    customer_sim_attr = LabeledStockAttribute(ids=customers.ids,
                                              init_values=0,
                                              relationship=customer_sim_rel)
    customers.add_attribute(name="SIM", attr=customer_sim_attr)

    dealer_sim_attr = LabeledStockAttribute(ids=dealers.ids,
                                            init_values=0,
                                            relationship=dealer_sim_rel)
    dealers.add_attribute(name="SIM", attr=dealer_sim_attr)

    deg_prob = average_degree/n_agents_a*n_agents_b
    agent_customer_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.ids, dealers.ids, deg_prob, seed),
        columns=["AGENT", "DEALER"])

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
    print "Creating circus"
    flying = Circus(the_clock)

    purchase = ActorAction(
        name="purchase",
        triggering_actor=customers,
        actorid_field="AGENT",

        operations=[
            agent_customer.ops.select_one(from_field="AGENT",
                                          named_as="DEALER"),

            dealer_sim_rel.ops.select_one(from_field="DEALER",
                                          named_as="SIM"),

            customer_sim_attr.ops.add_item(actor_id_field="AGENT",
                                           item_field="SIM"),

            dealer_sim_attr.ops.remove_item(actor_id_field="DEALER",
                                            item_field="SIM"),

            # not specifying the columns => by defaults, log everything
            operations.FieldLogger(log_id="cdr"),

        ],
        time_gen=timegen,
        activity_gen=activity_gen)

    flying.add_action(purchase)

    flying.add_increment(timegen)
    print "Done"

    return flying


def test_cdr_scenario():

    snd_circus = compose_circus()
    n_iterations = 100

    [all_purchases] = snd_circus.run(n_iterations)

    print ("""
        some purchase events:
          {}

    """.format(all_purchases.head()))

    assert all_purchases.shape[0] > 0
    assert "datetime" in all_purchases.columns

    # TODO: add real post-conditions on all_purchases

