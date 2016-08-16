from __future__ import division

from datetime import datetime

import datagenerator.operations as operations
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

    seeder = seed_provider(master_seed=123456)
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
    operators = ["OPERATOR_%d" % i for i in range(4)]

    ######################################
    # Define clocks
    ######################################
    the_clock = Clock(datetime(year=2016, month=6, day=8), time_step,
                      "%d%m%Y %H:%M:%S", seed=seeder.next())

    ######################################
    # Define generators
    ######################################
    msisdn_gen = MSISDNGenerator(countrycode="0032",
                                 prefix_list=["472", "473", "475", "476",
                                              "477", "478", "479"],
                                 length=6,
                                 seed=seeder.next())

    timegen = WeekProfiler(time_step, prof, seed=seeder.next())

    networkweightgenerator = ScaledParetoGenerator(m=1., a=1.2,
                                                   seed=seeder.next())

    mobilityweightgenerator = NumpyRandomGenerator(
        method="exponential", scale=1., seed=seeder.next())

    agentweightgenerator = NumpyRandomGenerator(method="exponential", scale=1.,
                                                seed=seeder.next())

    ######################################
    # Initialise generators
    ######################################
    timegen.initialise(the_clock)

    ######################################
    # Define Actors, Relationships, ...
    ######################################
    customers = Actor(n_customers)

    customers.add_attribute(name="MSISDN",
                            attr=Attribute(ids=customers.ids,
                                           init_values_generator=msisdn_gen))

    # mobility
    mobility = Relationship(name="people's cell location", seed=seeder.next())

    mobility_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.ids, cells, 0.4,
                                   seed=seeder.next()),
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
        seed=seeder.next())

    social_network = Relationship(name="neighbours", seed=seeder.next())

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
        make_random_bipartite_data(customers.ids, agents, 0.3,
                                   seed=seeder.next()),
        columns=["USER_ID", "AGENT"])

    agent_rel = Relationship(name="people's agent", seed=seeder.next())

    agent_rel.add_relations(from_ids=agent_df["USER_ID"],
                            to_ids=agent_df["AGENT"],
                            weights=agentweightgenerator.generate(len(
                                agent_df.index)))

    # customers's account
    recharge_trigger = DependentTriggerGenerator(
        value_mapper=logistic(a=-0.01, b=10.), seed=seeder.next())

    recharge_gen = ConstantGenerator(value=1000.)

    main_account = Attribute(ids=customers.ids,
                             init_values_generator=recharge_gen)

    customers.add_attribute(name="MAIN_ACCT", attr=main_account)

    # Operators
    operator_gen = NumpyRandomGenerator(method="choice",
                                        a=operators,
                                        p=[.8, .05, .1, .05],
                                        seed=seeder.next())

    customers.add_attribute(name="OPERATOR",
                            attr=Attribute(ids=customers.ids,
                                           init_values_generator=operator_gen))
    # Actions

    # Mobility

    mobilitytimegen = DayProfiler(time_step, mov_prof, seed=seeder.next())
    mobilitytimegen.initialise(the_clock)

    mobility_action = ActorAction(
        name="mobility",

        triggering_actor=customers,
        actorid_field="A_ID",

        timer_gen=mobilitytimegen,
    )

    mobility_action.set_operations(
        customers.ops.lookup(actor_id_field="A_ID",
                             select={"CELL": "PREV_CELL",
                                     "OPERATOR": "OPERATOR"}),

        # selects a cell
        mobility.ops.select_one(from_field="A_ID", named_as="NEW_CELL"),

        # update the CELL attribute of the actor accordingly
        customers.ops.overwrite(attribute="CELL",
                                copy_from_field="NEW_CELL"),

        the_clock.ops.timestamp(named_as="TIME"),

        # create mobility logs
        operations.FieldLogger(log_id="mobility",
                               cols=["TIME", "A_ID", "OPERATOR",
                                     "PREV_CELL", "NEW_CELL", ]),

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

        # note that there is timegen specified => the clock is not ticking
        # => the action can only be set externally (cf calls action)
    )

    topup.set_operations(
        customers.ops.lookup(
            actor_id_field="A_ID",
            select={"MSISDN": "CUSTOMER_NUMBER",
                    "CELL": "CELL",
                    "OPERATOR": "OPERATOR",
                    "MAIN_ACCT": "MAIN_ACCT_OLD"}),

        agent_rel.ops.select_one(from_field="A_ID", named_as="AGENT"),

        recharge_gen.ops.generate(named_as="VALUE"),

        operations.Apply(source_fields=["VALUE", "MAIN_ACCT_OLD"],
                         named_as="MAIN_ACCT",
                         f=add_value_to_account),

        customers.ops.overwrite(attribute="MAIN_ACCT",
                                copy_from_field="MAIN_ACCT"),

        the_clock.ops.timestamp(named_as="TIME"),

        operations.FieldLogger(log_id="topups",
                               cols=["TIME", "CUSTOMER_NUMBER", "AGENT",
                                     "VALUE", "OPERATOR", "CELL",
                                     "MAIN_ACCT_OLD", "MAIN_ACCT"]),

    )

    # Calls

    voice_duration_generator = NumpyRandomGenerator(
        method="choice", a=range(20, 240), seed=seeder.next())

    def compute_call_value(action_data):
        price_per_second = 2
        df = action_data[["DURATION"]] * price_per_second

        # must return a dataframe with a single column named "result"
        return df.rename(columns={"DURATION": "result"})

    def compute_call_type(action_data):

        def onnet(row):
            return (row["OPERATOR_A"] == "OPERATOR_0") & (row["OPERATOR_B"]
                                                          == "OPERATOR_0")

        result = pd.DataFrame(action_data.apply(onnet, axis=1), columns=["result_b"])

        result["result"] = result["result_b"].map(
            {True: "ONNET", False: "OFFNET"})

        return result[["result"]]

    # TODO: cf Sipho suggestion: we could have generic "add", "diff"... operations
    def substract_value_from_account(action_data):
        # maybe we should prevent negative accounts here? or not?
        new_value = action_data["MAIN_ACCT_OLD"] - action_data["VALUE"]

        # must return a dataframe with a single column named "result"
        return pd.DataFrame(new_value, columns=["result"])

    # call activity level, under normal and "excited" states
    normal_call_activity = ScaledParetoGenerator(m=10, a=1.2,
                                                 seed=seeder.next())
    excited_call_activity = ScaledParetoGenerator(m=1000, a=1.1,
                                                 seed=seeder.next())

    # after a call, probability of getting into "excited" mode (i.e., having
    # a shorted expected delay until next call
    to_excited_prob = ConstantGenerator(value=1)

    back_to_normal_prob = ConstantGenerator(value=0)

    # TODO: that is not correct: we need to generate booleans, not p
    # => generated uniform(0,1) and compare them to those activation levels
    # => maybe this is a trigger attribute? though there is overlap with
    # DependentTriggerGenerator

    # to_excited_prob = NumpyRandomGenerator(method="beta", a=1, b=9,
    #                                        seed=seeder.next())
    #
    # back_to_normal_prob = NumpyRandomGenerator(method="beta", a=9, b=1,
    #                                            seed=seeder.next())

    calls = ActorAction(
        name="calls",

        triggering_actor=customers,
        actorid_field="A_ID",

        timer_gen=timegen,
        activity=normal_call_activity,

        states={
            "excited": {
                "activity": excited_call_activity,
                "back_to_default_probability": back_to_normal_prob}
        }
    )

    calls.set_operations(
        # selects a B party
        social_network.ops.select_one(from_field="A_ID",
                                      named_as="B_ID",
                                      one_to_one=True),

        # some static fields
        customers.ops.lookup(actor_id_field="A_ID",
                             select={"MSISDN": "A",
                                     "CELL": "CELL_A",
                                     "OPERATOR": "OPERATOR_A",
                                     "MAIN_ACCT": "MAIN_ACCT_OLD"}),

        customers.ops.lookup(actor_id_field="B_ID",
                             select={"MSISDN": "B",
                                     "OPERATOR": "OPERATOR_B",
                                     "CELL": "CELL_B"}),

        ConstantGenerator(value="VOICE").ops.generate(named_as="PRODUCT"),

        operations.Apply(source_fields=["OPERATOR_A", "OPERATOR_B"],
                         named_as="TYPE",
                         f=compute_call_type),

        # computes the duration, value, new account amount and update
        # attribute accordingly
        voice_duration_generator.ops.generate(named_as="DURATION"),

        operations.Apply(source_fields="DURATION",
                         named_as="VALUE",
                         f=compute_call_value),

        operations.Apply(source_fields=["VALUE", "MAIN_ACCT_OLD"],
                         named_as="MAIN_ACCT_NEW",
                         f=substract_value_from_account),

        customers.ops.overwrite(attribute="MAIN_ACCT",
                                copy_from_field="MAIN_ACCT_NEW"),


        # customer with low account are now more likely to topup
        recharge_trigger.ops.generate(
            observations_field="MAIN_ACCT_NEW",
            named_as="SHOULD_TOP_UP"),

        topup.ops.force_act_next(actor_id_field="A_ID",
                                 condition_field="SHOULD_TOP_UP"),


        # triggering another call soon
        # excited_trigger.ops.generate(named_as="A_GETTING_BURSTY"),
        # calls.ops.transit_to_state(actor_id_field="A_ID",
        #                            condition_field="A_GETTING_BURSTY",
        #                            state="excited"),

        the_clock.ops.timestamp(named_as="DATETIME"),

        # final CDRs
        operations.FieldLogger(log_id="cdr",
                               cols=["DATETIME",
                                     "A", "B", "DURATION", "VALUE",
                                     "CELL_A", "OPERATOR_A",
                                     "CELL_B", "OPERATOR_B",
                                     "TYPE",   "PRODUCT"]),
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
        logs["cdr"].head(15).to_string(),
        logs["topups"].head(15).to_string(),
        logs["mobility"].tail(15).to_string())
        )

    print "users having highest amount of calls: "
    top_users = logs["cdr"]["A"].value_counts().head(10)
    print top_users


    customers = cdr_circus.get_actor_of(action_name="calls").to_dataframe()
    df = customers[customers["MSISDN"].isin(top_users.index)]
    print df

    assert logs["cdr"].shape[0] > 0
    assert logs["topups"].shape[0] > 0
    assert logs["mobility"].shape[0] > 0

    # TODO: add real post-conditions on all_cdrs, all_mov and all_topus


