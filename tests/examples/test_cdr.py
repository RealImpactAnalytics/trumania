from __future__ import division

from datetime import datetime

import datagenerator.operations as operations
from datagenerator.util_functions import *
from datagenerator.action import *
from datagenerator.actor import *
from datagenerator.attribute import *
from datagenerator.circus import *
from datagenerator.clock import *
from datagenerator.random_generators import *
from datagenerator.relationship import *


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
    msisdn_gen = MSISDNGenerator(countrycode="0032",
                                 prefix_list=["472", "473", "475", "476",
                                              "477", "478", "479"],
                                 length=6, seed=seed)

    activity_gen = ScaledParetoGenerator(m=10, a=1.2, seed=seed)

    timegen = WeekProfiler(time_step, prof, seed)
    mobilitytimegen = DayProfiler(time_step, mov_prof, seed)

    networkweightgenerator = ScaledParetoGenerator(m=1., a=1.2, seed=seed)

    mobilityweightgenerator = NumpyRandomGenerator(
        method="exponential", scale=1., seed=seed)

    agentweightgenerator = NumpyRandomGenerator(method="exponential", scale=1.,
                                                seed=seed)

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
        # TODO: use weights_gen to simplify API
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

    #
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
    recharge_trigger = TriggerGenerator(trigger_type="logistic", seed=seed)

    recharge_gen = ConstantGenerator(value=1000.)

    main_account = Attribute(ids=customers.ids,
                             init_values_generator=recharge_gen)

    customers.add_attribute(name="MAIN_ACCT", attr=main_account)

    # Operators
    #recharge_init = ConstantGenerator(name="recharge init", value=1000.)

    # Actions

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
            operations.FieldLogger(log_id="mobility",
                                   cols=["A_ID", "PREV_CELL", "NEW_CELL"]),
        ],

        time_gen=mobilitytimegen,
    )

    def add_value_to_account(data):
        # maybe we should prevent negative accounts here? or not?
        new_value = data["MAIN_ACCT_OLD"] + data["VALUE"]

        # must return a dataframe with a single column named "result"
        return pd.DataFrame(new_value, columns=["result"])

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

            recharge_gen.ops.generate(named_as="VALUE"),

            operations.Apply(source_fields=["VALUE", "MAIN_ACCT_OLD"],
                             result_field="MAIN_ACCT",
                             f=add_value_to_account),

            customers.ops.overwrite(attribute="MAIN_ACCT",
                                    copy_from_field="MAIN_ACCT"),

            operations.FieldLogger(log_id="topups",
                                   cols=["CUSTOMER_NUMBER", "AGENT", "VALUE",
                                         "CELL",
                                         "MAIN_ACCT_OLD", "MAIN_ACCT"]),
        ],

        # note that there is timegen specified => the clock is not ticking
        # => the action can only be set externally (cf calls action)

    )

    voice_duration_generator = NumpyRandomGenerator(
        method="choice", a=range(20, 240), seed=seed)

    def compute_call_value(data):
        price_per_second = 2
        df = data[["DURATION"]] * price_per_second

        # must return a dataframe with a single column named "result"
        return df.rename(columns={"DURATION": "result"})

    # TODO: cf Sipho suggestion: we could have generic "add", "diff"... operations
    def substract_value_from_account(data):
        # maybe we should prevent negative accounts here? or not?
        new_value = data["MAIN_ACCT_OLD"] - data["VALUE"]

        # must return a dataframe with a single column named "result"
        return pd.DataFrame(new_value, columns=["result"])

    def copy_id_if_topup(data):
        copied_ids = data[data["SHOULD_TOP_UP"]][["A_ID"]].reindex(data.index)

        return copied_ids.rename(columns={"A_ID": "result"})

    calls = ActorAction(
        name="calls",

        triggering_actor=customers,
        actorid_field="A_ID",

        operations=[
            # selects a B party
            social_network.ops.select_one(from_field="A_ID",
                                          named_as="B_ID",
                                          one_to_one=True),

            # some static fields
            customers.ops.lookup(actor_id_field="A_ID",
                                 select={"MSISDN": "A",
                                         "CELL": "CELL_A",
                                         "MAIN_ACCT": "MAIN_ACCT_OLD"}),

            customers.ops.lookup(actor_id_field="B_ID",
                                 select={"MSISDN": "B",
                                         "CELL": "CELL_B"}),

            ConstantGenerator(value="VOICE").ops.generate(named_as="PRODUCT"),

            # computes the duration, value, new account amount and update
            # attribute accordingly
            voice_duration_generator.ops.generate(named_as="DURATION"),

            operations.Apply(source_fields="DURATION",
                             result_field="VALUE",
                             f=compute_call_value),

            operations.Apply(source_fields=["VALUE", "MAIN_ACCT_OLD"],
                             result_field="MAIN_ACCT_NEW",
                             f=substract_value_from_account),

            customers.ops.overwrite(attribute="MAIN_ACCT",
                                    copy_from_field="MAIN_ACCT_NEW"),


            # customer with low account are now more likely to topup
            recharge_trigger.ops.generate(
                observations_field="MAIN_ACCT_NEW", named_as="SHOULD_TOP_UP"),

            operations.Apply(source_fields=["A_ID", "SHOULD_TOP_UP"],
                             result_field="TOPPING_UP_A_IDS",
                             f=copy_id_if_topup),

            topup.ops.force_act_next(active_ids_field="TOPPING_UP_A_IDS"),


            # final CDRs
            operations.FieldLogger(log_id="cdr",
                                   cols=["A", "B", "DURATION", "VALUE",
                                          "CELL_A", "CELL_B", "PRODUCT"]),
        ],

        time_gen=timegen,
        activity_gen=activity_gen,
    )


    ## Circus
    flying = Circus(the_clock)

    flying.add_increment(timegen)

    flying.add_action(topup)
    flying.add_action(calls)
    flying.add_action(mobility_action)

    print "Done"
    return flying


def test_cdr_scenario():

    cdr_circus = compose_circus()
    n_iterations = 50

    # dataframes of outcomes are returned in the order in which the actions
    # are added to the circus
    logs = cdr_circus.run(n_iterations)

    print ("""
        some voice cdrs:
          {}

        some mobility events:
          {}

        some topup event:
          {}

    """.format(
        logs["cdr"].tail(15).to_string(),
        logs["topups"].tail(15).to_string(),
        logs["mobility"].tail().to_string())
        )

    assert logs["cdr"].shape[0] > 0
    assert "datetime" in logs["cdr"].columns

    assert logs["topups"].shape[0] > 0
    assert "datetime" in logs["topups"].columns

    assert logs["mobility"].shape[0] > 0
    assert "datetime" in logs["mobility"].columns

    # TODO: add real post-conditions on all_cdrs, all_mov and all_topus


