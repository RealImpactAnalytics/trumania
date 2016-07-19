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


def compose_circus():
    """
        Builds a circus simulating call, mobility and topics.
        See test case below
    """

    ######################################
    # Define parameters
    ######################################
    tp = time.clock()
    print "Parameters"

    seed = 123456
    n_customers = 1000
    n_cells = 100
    n_agents = 100
    average_degree = 20

    prof = pd.Series([5., 5., 5., 5., 5., 3., 3.],
                     index=[timedelta(days=x, hours=23, minutes=59, seconds=59) for x in range(7)])
    time_step = 60

    mov_prof = pd.Series(
        [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.],
        index=[timedelta(hours=h, minutes=59, seconds=59) for h in range(24)])

    cells = ["CELL_%s" % (str(i).zfill(4)) for i in range(n_cells)]
    agents = ["AGENT_%s" % (str(i).zfill(3)) for i in range(n_agents)]

    products = ["VOICE", "SMS"]

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
    msisdn_gen = MSISDNGenerator("msisdn-tests-1", "0032", ["472", "473", "475", "476", "477", "478", "479"], 6, seed)
    activity_gen = GenericGenerator("user-activity", "pareto", {"a": 1.2, "m": 10.}, seed)
    timegen = WeekProfiler(time_step, prof, seed)

    mobilitytimegen = DayProfiler(time_step, mov_prof, seed)
    networkweightgenerator = GenericGenerator("network-weight", "pareto", {"a": 1.2, "m": 1.}, seed)

#    mobilitychooser = WeightedChooserAggregator("CELL", "weight", seed)
    mobilityweightgenerator = GenericGenerator("mobility-weight", "exponential", {"scale": 1.})

#    agentchooser = WeightedChooserAggregator("AGENT", "weight", seed)
    agentweightgenerator = GenericGenerator("agent-weight", "exponential", {"scale": 1.})

    init_mobility_generator = GenericGenerator("init-mobility", "choice", {"a": cells})

    SMS_price_generator = GenericGenerator("SMS-price", "constant", {"a": 10.})
    voice_duration_generator = GenericGenerator("voice-duration", "choice", {"a": range(20, 240)}, seed)
    voice_price_generator = ValueGenerator("voice-price", 1)
#    productchooser = WeightedChooserAggregator("PRODUCT", "weight", seed)

    recharge_init = GenericGenerator("recharge init", "constant", {"a": 1000.})
    recharge_trigger = TriggerGenerator("Topup", "logistic", {}, seed)
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
    customers = Actor(n_customers)
    print "Done"
    customers.gen_attribute(name="MSISDN",
                            generator=msisdn_gen)
    tatt = time.clock()
    # customers.gen_attribute("activity", activity_gen)
    # customers.gen_attribute("clock", timegen, weight_field="activity")

    print "Added atributes"
    tsna = time.clock()
    print "Creating social network"
    social_network = create_er_social_network(customer_ids=customers.get_ids(),
                                              p=average_degree / n_customers,
                                              seed=seed)
    tsnaatt = time.clock()
    print "Done"

    ###
    # social network

    network = WeightedRelationship(name="neighbours",
                                   seed=seed)

    # TODO: make this a add_weighted_relations, passing the arguments to
    # build th
    network.add_relations(from_ids=social_network["A"].values,
                          to_ids=social_network["B"].values,
                          weights=networkweightgenerator.generate(len(
                             social_network.index)))

    network.add_relations(from_ids=social_network["B"].values,
                         to_ids=social_network["A"].values,
                         weights=networkweightgenerator.generate(len(
                             social_network.index)))


    print "Done SNA"
    tmo = time.clock()

    ###
    # People's mobility

    print "Mobility"
    mobility_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.get_ids(), cells, 0.4, seed),
        columns=["A", "CELL"])

    print "Network created"
    tmoatt = time.clock()


    ###
    # MSISDN -> Agent

    agent_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.get_ids(), agents, 0.3, seed),
        columns=["A", "AGENT"])

    print "Agent relationship created"
    tagatt = time.clock()
    agent_rel = AgentRelationship(name="people's agent",
                                  seed=seed)

    agent_rel.add_relations(from_ids=agent_df["A"],
                            to_ids=agent_df["AGENT"],
                            weights=agentweightgenerator.generate(len(
                                agent_df.index)))

    # customers's account

    # TODO: I think all transient attributes and attributes should be
    # initiaized at construction, to have one single placer where the
    # object 's signature is defined.
    # TODO there is a coupling here between the att_type and the parameters
    # of the corresponding AttributeAction
    customers.add_transient_attribute(name="MAIN_ACCT",
                                      att_type="stock",
                                      generator=recharge_init,
                                      params={"trigger_generator":
                                                  recharge_trigger})
    print "Done all customers"

    tci = time.clock()
    print "Creating circus"
    flying = Circus(the_clock)
#    flying.add_actor("customers", customers)



    topup = AttributeAction(name="topup",
                            actor=customers,

                            attr_name="MAIN_ACCT",
                            actorid_field_name="A",
                            activity_generator=GenericGenerator("1",
                                                                "constant",
                                                                {"a": 1.}),
                            time_generator=ConstantProfiler(-1),
                            parameters={"relationship": agent_rel,
#                                        "id1": "A",
                                       "id2": "AGENT",
                                        "id3": "value"
                                        }
                            )

    # TODO: those "join" information should be part of hte attribute action
    # definition, to keep all the definition at the same place
    flying.add_action(topup, {"join": [("A", customers, "MSISDN", "CUSTOMER_NUMBER"),
                                       ("A", customers, "CELL", "CELL")]})

    ####
    # calls and SMS

    voice = VoiceProduct(voice_duration_generator, voice_price_generator)
    sms = SMSProduct(SMS_price_generator)

    product_df = assign_random_proportions("A", "PRODUCT", customers.get_ids(), products, seed)
    product_rel = ProductRelationship(products={"VOICE": voice, "SMS": sms},
                                      name="people's product",
                                      seed=seed)

    # TODO: create a contructor that accept a 2 or 3 column dataframes, with the
    # convention that 2 means from, to and expect a weight generation parameters
    # and 3 means from, to, wieghts
    product_rel.add_relations(from_ids=product_df["A"],
                              to_ids=product_df["PRODUCT"],
                              weights=product_df["weight"])

    calls = ActorAction(name="calls",
                        actor=customers,
                        actorid_field_name="A",
                        time_generator=timegen,
                        activity_generator=activity_gen)

    calls.add_field("B", network)
    calls.add_field("PRODUCT", product_rel)
    calls.add_impact(name="value decrease",
                     attribute="MAIN_ACCT",
                     function="decrease_stock",
                     parameters={
                         # TODO: "account value" would be more explicit here
                         # I think
                        "value": "VALUE",
                        # "key": "A",
                        "recharge_action":topup})

    flying.add_action(calls, {"join": [("A", customers, "MSISDN", "A_NUMBER"),
                                       ("B", customers, "MSISDN", "B_NUMBER"),
                                       ("A", customers, "CELL", "CELL_A"),
                                       ("B", customers, "CELL", "CELL_B"), ]})

    # mobility

    mobility = WeightedRelationship(name="people's cell location",
                                    seed=seed)
    # mobility = WeightedRelationship("A", "CELL", mobilitychooser)
    mobility.add_relations(from_ids=mobility_df["A"],
                           to_ids=mobility_df["CELL"],
                           weights=mobilityweightgenerator.generate(len(
                              mobility_df.index)))

    # Initial mobility value (ie.e cell location)
    # => TODO: there is overlap between concern of "relation" and "transient
    # attibute", they should not be initialized separately
    customers.add_transient_attribute(name="CELL",
                                      att_type="choice",
                                      generator=init_mobility_generator)

    mobility_action = AttributeAction(name="mobility",
                                      actor=customers,
                                      attr_name="CELL",
                                      actorid_field_name="A",
                                      activity_generator=GenericGenerator("1",
                                                                     "constant",
                                                                     {"a":1.}),
                                      time_generator=mobilitytimegen,
                                      parameters={'relationship': mobility,
                                           'new_time_generator': mobilitytimegen,
#                                           'id1': "A",
#                                           'id2': "CELL"
                                                  })

    flying.add_action(mobility_action)

    flying.add_increment(timegen)
    tr = time.clock()

    print "Done"

    all_times = {"parameters": tc - tp,
                 "clocks": tg - tc,
                 "generators": tig - tg,
                 "init generators": tcal - tig,
                 "callers creation (full)": tmo - tcal,
                 "caller creation (solo)": tatt - tcal,
                 "caller attribute creation": tsna - tatt,
                 "caller SNA graph creation": tsnaatt - tsna,
                 "mobility graph creation": tmoatt - tmo,
                 "mobility attribute creation": tci - tmoatt,
                 "circus creation": tr - tci,
                 "tr": tr,
        }

    return flying, all_times


def test_cdr_scenario():

    cdr_circus, all_times = compose_circus()
    n_iterations = 100

    # dataframes of outcomes are returned in the order in which the actions
    # are added to the circus
    all_topup, all_cdrs, all_mov = cdr_circus.run(n_iterations)
    tf = time.clock()

    all_times["runs (all)"] = tf - all_times["tr"]
    all_times["one run (average)"] = (tf - all_times["tr"]) / n_iterations

    print (json.dumps(all_times, indent=2))

    assert all_cdrs.shape[0] > 0
    assert "datetime" in all_cdrs.columns

    assert all_mov.shape[0] > 0
    assert "datetime" in all_mov.columns

    assert all_topup.shape[0] > 0
    assert "datetime" in all_topup.columns

    print ("""
        some cdrs:
          {}

        some mobility events:
          {}

        some topup event:
          {}

    """.format(all_cdrs.head(15).to_string(), all_mov.head().to_string(),
               all_topup.head().to_string()))

    # TODO: add real post-conditions on all_cdrs, all_mov and all_topus


