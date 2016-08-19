from __future__ import division

from datetime import datetime

import datagenerator.operations as operations
from datagenerator.relationship import *
from datagenerator.action import *
from datagenerator.actor import *
from datagenerator.attribute import *
from datagenerator.circus import *
from datagenerator.clock import *
from datagenerator.util_functions import *


# AgentA: has stock of SIMs
# AgentB: has stock of SIMs
# SIMs: has ID
# AgentA buys stock to AgentB

params = {
    "n_agents": 1000,
    "n_dealers": 100,
    "average_degree": 20,
    "n_sims": 500000
}


def create_agents_with_sims(seeder):
    """
    Create the AGENT actor (i.e. customer) together with its "SIM" labeled
    stock, to keep track of which SIMs are own by which agent
    """
    agents = Actor(size=params["n_agents"], prefix="AGENT_", max_length=3)

    customer_sim_rel = agents.create_relationship("SIM", seed=seeder.next())

    customer_sim_attr = LabeledStockAttribute(ids=agents.ids,
                                              init_values=0,
                                              relationship=customer_sim_rel)
    agents.add_attribute(name="SIM", attr=customer_sim_attr)

    # the relationship is not initialized with any SIM: agents start with
    # no SIM

    return agents


def create_dealers_with_sims(seeder):
    """
    Create the DEALER actor together with its "SIM" labeled stock, to keep
     track of which SIMs are available at which agents
    """

    dealers = Actor(size=params["n_dealers"], prefix="DEALER_", max_length=3)

    sims = ["SIM_%s" % (str(i).zfill(6)) for i in range(params["n_sims"])]

    # relationship from dealer to sim, to keep track of their stock
    dealer_sim_rel = dealers.create_relationship("SIM", seed=seeder.next())

    sims_dealer = make_random_assign("SIM", "DEALER",
                                     sims, dealers.ids,
                                     seed=seeder.next())
    dealer_sim_rel.add_relations(from_ids=sims_dealer["DEALER"],
                                 to_ids=sims_dealer["SIM"])

    # the same stock is also kept as an attribute
    dealer_sim_attr = LabeledStockAttribute(ids=dealers.ids,
                                            init_values=0,
                                            relationship=dealer_sim_rel)
    dealers.add_attribute(name="SIM", attr=dealer_sim_attr)

    return dealers


def connect_agent_to_dealer(agents, dealers, seeder):
    """
    Creates a random relationship from agents to dealers
    """

    deg_prob = params["average_degree"] / params["n_agents"] * params["n_dealers"]

    agent_weight_gen = NumpyRandomGenerator(method="exponential", scale=1.)

    agent_customer_df = pd.DataFrame.from_records(
        make_random_bipartite_data(agents.ids, dealers.ids, deg_prob,
                                   seed=seeder.next()),
        columns=["AGENT", "DEALER"])

    agent_customer_rel = agents.create_relationship(name="DEALERS",
                                                    seed=seeder.next())

    agent_customer_rel.add_relations(
        from_ids=agent_customer_df["AGENT"],
        to_ids=agent_customer_df["DEALER"],
        weights=agent_weight_gen.generate(agent_customer_df.shape[0]))


def add_purchase_actions(circus, agents, dealers, seeder):
    """
    Adds a SIM purchase action from agents to dealer, with impact on stock of
    both actors
    """

    timegen = WeekProfiler(circus.clock,
                           week_profile=[5., 5., 5., 5., 5., 3., 3.],
                           seed=seeder.next())

    purchase_activity_gen = NumpyRandomGenerator(
        method="choice", a=range(1, 4), seed=seeder.next())

    purchase = ActorAction(
        name="purchase",
        triggering_actor=agents,
        actorid_field="AGENT",
        timer_gen=timegen,
        activity=purchase_activity_gen)

    purchase.set_operations(
        circus.clock.ops.timestamp(named_as="DATETIME"),

        agents.get_relationship("DEALERS").ops.select_one(from_field="AGENT",
                                                          named_as="DEALER"),

        dealers.get_relationship("SIM").ops.select_one(from_field="DEALER",
                                                       named_as="SIM",
                                                       one_to_one=True),

        agents.get_attribute("SIM").ops.add_item(actor_id_field="AGENT",
                                                 item_field="SIM"),

        dealers.get_attribute("SIM").ops.remove_item(actor_id_field="DEALER",
                                                     item_field="SIM"),

        # not specifying the columns => by defaults, log everything
        operations.FieldLogger(log_id="purchases"),
    )

    circus.add_action(purchase)


def test_cdr_scenario():

    seeder = seed_provider(master_seed=123456)

    the_clock = Clock(datetime(year=2016, month=6, day=8), step_s=60,
                      format_for_out="%d%m%Y %H:%M:%S", seed=seeder.next())

    flying = Circus(the_clock)

    agents = create_agents_with_sims(seeder)
    dealers = create_dealers_with_sims(seeder)
    connect_agent_to_dealer(agents, dealers, seeder)

    add_purchase_actions(flying, agents, dealers, seeder)

    logs = flying.run(n_iterations=100)

    for logid, lg in logs.iteritems():
        print " - some {}:\n{}\n\n".format(logid, lg.head(15).to_string())

