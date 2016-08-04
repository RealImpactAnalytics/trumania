from __future__ import division

from datetime import datetime

from bi.ria.generator.action import *
from bi.ria.generator.attribute import *
from bi.ria.generator.clock import *
from bi.ria.generator.circus import *
from bi.ria.generator.random_generators import *
from bi.ria.generator.relationship import *
from bi.ria.generator.util_functions import *
import bi.ria.generator.operations as operations

from bi.ria.generator.actor import *
import bi.ria.generator.ext.cdr as cdr


#####
# Circus creation


def compose_circus():
    """
        Builds a circus simulating call, mobility and topics.
        See test case below
    """

    ######################################
    # Define parameters
    ######################################

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

    ######################################
    # Define clocks
    ######################################
    the_clock = Clock(datetime(year=2016, month=6, day=8), time_step, "%d%m%Y %H:%M:%S", seed)

    ######################################
    # Define generators
    ######################################
    msisdn_gen = MSISDNGenerator("msisdn-tests-1", "0032", ["472", "473", "475", "476", "477", "478", "479"], 6, seed)
    activity_gen = GenericGenerator("user-activity", "pareto", {"a": 1.2, "m": 10.}, seed)
    timegen = WeekProfiler(time_step, prof, seed)
    mobilitytimegen = DayProfiler(time_step, mov_prof, seed)

    networkweightgenerator = GenericGenerator("network-weight", "pareto", {"a": 1.2, "m": 1.}, seed)

    mobilityweightgenerator = GenericGenerator("mobility-weight", "exponential", {"scale": 1.})

    agentweightgenerator = GenericGenerator("agent-weight", "exponential", {"scale": 1.})

    ######################################
    # Initialise generators
    ######################################
    timegen.initialise(the_clock)
    mobilitytimegen.initialise(the_clock)

    ######################################
    # Define Actors, Relationships, ...
    ######################################
    customers = Actor(n_customers)

    customers.add_attribute(name="MSISDN",
                            attr=Attribute(ids=customers.ids,
                                           init_values_generator=msisdn_gen))

    # mobility
    mobility = Relationship(name="people's cell location", seed=seed)

    mobility_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.ids, cells, 0.4, seed),
        columns=["USER_ID", "CELL"])

    mobility.add_relations(
        from_ids=mobility_df["USER_ID"],
        to_ids=mobility_df["CELL"],
        weights=mobilityweightgenerator.generate(mobility_df.shape[0]))

    cell_attr = Attribute(relationship=mobility)
    customers.add_attribute(name="CELL", attr=cell_attr)

    ###
    # social network

    social_network_values = create_er_social_network(
        customer_ids=customers.ids,
        p=average_degree / n_customers,
        seed=seed)

    social_network = Relationship(name="neighbours",
                                  seed=seed)

    social_network.add_relations(
        from_ids=social_network_values["A"].values,
        to_ids=social_network_values["B"].values,
        weights=networkweightgenerator.generate(social_network_values.shape[0]))

    social_network.add_relations(
        from_ids=social_network_values["B"].values,
        to_ids=social_network_values["A"].values,
        weights=networkweightgenerator.generate(social_network_values.shape[0]))


    ###
    # MSISDN -> Agent

    agent_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.ids, agents, 0.3, seed),
        columns=["USER_ID", "AGENT"])

    agent_rel = Relationship(name="people's agent", seed=seed)

    agent_rel.add_relations(from_ids=agent_df["USER_ID"],
                            to_ids=agent_df["AGENT"],
                            weights=agentweightgenerator.generate(len(
                                agent_df.index)))

    # customers's account
    recharge_trigger = TriggerGenerator(name="Topup",
                                        gen_type="logistic",
                                        parameters={},
                                        seed=seed)

    recharge_init = GenericGenerator(name="recharge init",
                                     gen_type="constant",
                                     parameters={"a": 1000.},
                                     seed=seed)

    main_account = Attribute(ids=customers.ids,
                             init_values_generator=recharge_init)

    customers.add_attribute(name="MAIN_ACCT", attr=main_account)

    flying = Circus(the_clock)

    mobility_action = ActorAction(
        name="mobility",

        triggering_actor=customers,
        actorid_field="A_ID",

        operations=[
            customers.ops.lookup(actor_id_field="A_ID",
                                 select={"CELL": "PREV_CELL"}),

            # selects a cell
            mobility.ops.select_one(from_field="A_ID", named_as="NEW_CELL"),

            # update the CELL attribute of the actor accordingly
            customers.ops.overwrite(attribute="CELL",
                                    copy_from_field="NEW_CELL"),

            # create mobility logs
            operations.ColumnLogger(log_id="mobility",
                                    cols=["A_ID", "PREV_CELL", "NEW_CELL"]),
        ],

        time_gen=mobilitytimegen,
    )

    topup = ActorAction(
        name="topup",
        triggering_actor=customers,
        actorid_field="A_ID",

        operations=[

            customers.ops.lookup(
                actor_id_field="A_ID",
                select={"MSISDN": "CUSTOMER_NUMBER",
                        "CELL": "CELL",
                        "MAIN_ACCT": "MAIN_ACCT_OLD"}),

            agent_rel.ops.select_one(from_field="A_ID", named_as="AGENT"),

            operations.Constant(value=1000, named_as="VALUE"),

            operations.Apply(source_fields=["VALUE", "MAIN_ACCT_OLD"],
                             result_field="MAIN_ACCT",
                             f=cdr.add_value_to_account),

            customers.ops.overwrite(attribute="MAIN_ACCT",
                                    copy_from_field="MAIN_ACCT"),

            operations.ColumnLogger(log_id="topups",
                                    cols=["CUSTOMER_NUMBER", "AGENT", "VALUE",
                                          "CELL",
                                          "MAIN_ACCT_OLD", "MAIN_ACCT"]),
        ],

        # note that there is timegen specified => the clock is not ticking
        # => the action can only be set externally (cf calls action)

    )

    voice_duration_generator = GenericGenerator(
        name="voice-duration",
        gen_type="choice",
        parameters={"a": range(20, 240)},
        seed=seed)

    calls = ActorAction(
        name="calls",

        triggering_actor=customers,
        actorid_field="A_ID",

        operations=[
            # selects a B party
            social_network.ops.select_one(from_field="A_ID", named_as="B_ID"),

            # some static fields
            customers.ops.lookup(actor_id_field="A_ID",
                                 select={"MSISDN": "A",
                                         "CELL": "CELL_A",
                                         "MAIN_ACCT": "MAIN_ACCT_OLD"}),

            customers.ops.lookup(actor_id_field="B_ID",
                                 select={"MSISDN": "B",
                                         "CELL": "CELL_B"}),

            operations.Constant(value="VOICE", named_as="PRODUCT"),



            # computes the duration, value, new account amount and update
            # attribute accordingly
            operations.RandomValues(value_generator=voice_duration_generator,
                                    named_as="DURATION"),

            operations.Apply(source_fields="DURATION",
                             result_field="VALUE",
                             f=cdr.compute_call_value),

            operations.Apply(source_fields=["VALUE", "MAIN_ACCT_OLD"],
                             result_field="MAIN_ACCT_NEW",
                             f=cdr.substract_value_from_account),

            customers.ops.overwrite(attribute="MAIN_ACCT",
                                    copy_from_field="MAIN_ACCT_NEW"),


            # customer with low account are now more likely to topup
            operations.RandomValues(value_generator=recharge_trigger,
                                    weights_field="MAIN_ACCT_NEW",
                                    named_as="SHOULD_TOP_UP"),

            operations.Apply(source_fields=["A_ID", "SHOULD_TOP_UP"],
                             result_field="TOPPING_UP_A_IDS",
                             f=cdr.copy_id_if_topup),

            topup.ops.force_act_next(active_ids_field="TOPPING_UP_A_IDS"),


            # final CDRs
            operations.ColumnLogger(log_id="cdr",
                                    cols=["A", "B", "DURATION", "VALUE",
                                          "CELL_A", "CELL_B", "PRODUCT"]),
        ],

        time_gen=timegen,
        activity_gen=activity_gen,
    )

    flying.add_increment(timegen)

    flying.add_action(topup)
    flying.add_action(calls)
    flying.add_action(mobility_action)

    print "Done"
    return flying


def test_cdr_scenario():

    cdr_circus = compose_circus()
    n_iterations = 100

    # dataframes of outcomes are returned in the order in which the actions
    # are added to the circus
    topups, voice_cdrs, all_mov = cdr_circus.run(n_iterations)

    print ("""
        some voice cdrs:
          {}

        some mobility events:
          {}

        some topup event:
          {}

    """.format(
           voice_cdrs.tail(15).to_string(),
           all_mov.tail(15).to_string(),
           topups.tail().to_string())
           )

    assert voice_cdrs.shape[0] > 0
    assert "datetime" in voice_cdrs.columns

    assert all_mov.shape[0] > 0
    assert "datetime" in all_mov.columns

    assert topups.shape[0] > 0
    assert "datetime" in topups.columns

    # TODO: add real post-conditions on all_cdrs, all_mov and all_topus


