from __future__ import division

from datetime import datetime

from datagenerator.action import *
from datagenerator.actor import *
from datagenerator.attribute import *
from datagenerator.circus import *
from datagenerator.clock import *
from datagenerator.random_generators import *
from datagenerator.relationship import *


params = {
    "time_step": 60,
    "n_cells": 100,
    "n_agents": 100,
    "n_customers": 1000,
    "average_degree": 20
}


def add_cells(circus, seeder):
    """
    Creates the CELL actors + the actions to let them randomly break down and
    get back up
    """
    cells = Actor(prefix="CELL_", size=params["n_cells"])

    # the cell "health" is its probability of accepting a call. By default
    # let's says it's one expected failure every 1000 calls
    healthy_level_gen = NumpyRandomGenerator(method="beta", a=999, b=1,
                                             seed=seeder.next())

    # tendency is inversed in case of broken cell: it's probability of
    # accepting a call is much lower
    unhealthy_level_gen = NumpyRandomGenerator(method="beta", a=1, b=999,
                                             seed=seeder.next())

    health = Attribute(ids=cells.ids, init_values_gen=healthy_level_gen)
    cells.add_attribute(name="HEALTH", attr=health)

    # same profiler for breakdown and repair: they are both related to
    # typical human activity
    default_day_profiler = DayProfiler(circus.clock)

    cell_break_down_action = ActorAction(
        name="cell_break_down",

        triggering_actor=cells,
        actorid_field="CELL_ID",

        timer_gen=default_day_profiler,

        # fault activity is very low: most cell tend never to break down (
        # hopefully...)
        activity=ScaledParetoGenerator(m=5, a=1.4, seed=seeder.next())
    )

    cell_repair_action = ActorAction(
        name="cell_break_down",

        triggering_actor=cells,
        actorid_field="CELL_ID",

        timer_gen=default_day_profiler,

        # repair activity is much higher
        activity=ScaledParetoGenerator(m=100, a=1.2, seed=seeder.next()),

        # repair is not re-scheduled at the end of a repair, but only triggered
        # from a "break-down" action
        auto_reset_timer=False
    )

    cell_break_down_action.set_operations(
        unhealthy_level_gen.ops.generate(named_as="NEW_HEALTH_LEVEL"),
        cells.ops.overwrite(attribute="HEALTH",
                            copy_from_field="NEW_HEALTH_LEVEL"),
        cell_repair_action.ops.reset_timers(actor_id_field="CELL_ID"),
        circus.clock.ops.timestamp(named_as="TIME"),

        operations.FieldLogger(log_id="cell_status",
                               cols=["TIME", "CELL_ID", "NEW_HEALTH_LEVEL"]),
    )

    cell_repair_action.set_operations(
        healthy_level_gen.ops.generate(named_as="NEW_HEALTH_LEVEL"),
        cells.ops.overwrite(attribute="HEALTH",
                            copy_from_field="NEW_HEALTH_LEVEL"),
        circus.clock.ops.timestamp(named_as="TIME"),

        # note that both actions are contributing to the same "cell_status" log
        operations.FieldLogger(log_id="cell_status",
                               cols=["TIME", "CELL_ID", "NEW_HEALTH_LEVEL"]),
    )

    circus.add_action(cell_break_down_action)
    circus.add_action(cell_repair_action)

    return cells


def add_mobility(circus, customers, cells, seeder):
    """
    adds a CELL attribute to the customer actor + a mobility action that
    randomly moves customers from CELL to CELL among their used cells.
    """

    # mobility time profile: assign high mobility activities to busy hours
    # of the day
    mov_prof = [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.,
                1., 1., 5., 10., 5., 1., 1., 1., 1.]
    mobility_time_gen = DayProfiler(circus.clock, mov_prof, seed=seeder.next())

    # Mobility network, i.e. choice of cells per user, i.e. these are the
    # weighted "used cells" (as in "most used cells) for each user
    mobility_weight_gen = NumpyRandomGenerator(
        method="exponential", scale=1., seed=seeder.next())

    mobility_rel = customers.create_relationship("POSSIBLE_CELLS",
                                                 seed=seeder.next())

    mobility_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.ids, cells.ids, 0.4,
                                   seed=seeder.next()),
        columns=["USER_ID", "CELL"])

    mobility_rel.add_relations(
        from_ids=mobility_df["USER_ID"],
        to_ids=mobility_df["CELL"],
        weights=mobility_weight_gen.generate(mobility_df.shape[0]))

    # Initialize the mobility by allocating one first random cell to each
    # customer among its network
    cell_attr = Attribute(relationship=mobility_rel)
    customers.add_attribute(name="CELL", attr=cell_attr)

    # Mobility action itself, basically just a random hop from cell to cell,
    # that updates the "CELL" attributes + generates mobility logs
    mobility_action = ActorAction(
        name="mobility",

        triggering_actor=customers,
        actorid_field="A_ID",

        timer_gen=mobility_time_gen,
    )

    mobility_action.set_operations(
        customers.ops.lookup(actor_id_field="A_ID",
                             select={"CELL": "PREV_CELL",
                                     "OPERATOR": "OPERATOR"}),

        # selects a destination cell
        mobility_rel.ops.select_one(from_field="A_ID", named_as="NEW_CELL"),

        # update the CELL attribute of the customers accordingly
        customers.ops.overwrite(attribute="CELL",
                                copy_from_field="NEW_CELL"),

        circus.clock.ops.timestamp(named_as="TIME"),

        # create mobility logs
        operations.FieldLogger(log_id="mobility_logs",
                               cols=["TIME", "A_ID", "OPERATOR",
                                     "PREV_CELL", "NEW_CELL", ]),
    )

    circus.add_action(mobility_action)


def add_social_network(customers, seeder):
    """
    Creates a random relationship from and to customers, to represent the
    social network
    """

    # create a random A to B symmetric relationship
    networkweightgenerator = ScaledParetoGenerator(m=1., a=1.2,
                                                   seed=seeder.next())

    social_network_values = create_er_social_network(
        customer_ids=customers.ids,
        p=params["average_degree"]/ params["n_customers"],
        seed=seeder.next())

    social_network = customers.create_relationship("FRIENDS",
                                                   seed=seeder.next())
    social_network.add_relations(
        from_ids=social_network_values["A"].values,
        to_ids=social_network_values["B"].values,
        weights=networkweightgenerator.generate(social_network_values.shape[0]))

    social_network.add_relations(
        from_ids=social_network_values["B"].values,
        to_ids=social_network_values["A"].values,
        weights=networkweightgenerator.generate(social_network_values.shape[0]))


def add_topups(circus, customers, seeder):
    """
    Adds a MAIN_ACCT attribute to the customer to keep track of their finance
    level +  a topup actions to allow buying recharges.

    The topups are not triggered by a timer_gen and a decrementing timer =>
        by itself this action is permanently inactive. This action is meant
        to be triggered externally (from the "calls" or "sms" actions)
    """

    # agent relationship: set of available agents to each user, weighted by
    # user's preference
    agents = ["AGENT_%s" % (str(i).zfill(3)) for i in range(params["n_agents"])]

    agent_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.ids, agents, 0.3,
                                   seed=seeder.next()),
        columns=["USER_ID", "AGENT"])

    agent_rel = customers.create_relationship("POSSIBLE_SHOPS",
                                              seed=seeder.next())

    agent_weight_gen = NumpyRandomGenerator(method="exponential", scale=1.,
                                            seed=seeder.next())

    agent_rel.add_relations(from_ids=agent_df["USER_ID"],
                            to_ids=agent_df["AGENT"],
                            weights=agent_weight_gen.generate(len(
                                agent_df.index)))

    # Main account of each users + one initial "recharge" to all so customers do
    # not start with no money
    recharge_gen = ConstantGenerator(value=1000.)

    main_account = Attribute(ids=customers.ids,
                             init_values_gen=recharge_gen)

    customers.add_attribute(name="MAIN_ACCT", attr=main_account)

    # topup action itself, basically just a selection of a dealer and subsequent
    # computation of the value
    topup_action = ActorAction(
        name="topups",
        triggering_actor=customers,
        actorid_field="A_ID",

        # note that there is timegen specified => the clock is not ticking
        # => the action can only be set externally (cf calls action)
    )

    topup_action.set_operations(
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
                         f=np.add, f_args="series"),

        customers.ops.overwrite(attribute="MAIN_ACCT",
                                copy_from_field="MAIN_ACCT"),

        circus.clock.ops.timestamp(named_as="TIME"),

        operations.FieldLogger(log_id="topups",
                               cols=["TIME", "CUSTOMER_NUMBER", "AGENT",
                                     "VALUE", "OPERATOR", "CELL",
                                     "MAIN_ACCT_OLD", "MAIN_ACCT"]),

    )

    circus.add_action(topup_action)


def compute_call_value(action_data):
    """
        Computes the value of a call based on duration, onnet/offnet and time
        of the day.

        This is meant to be called in an Apply of the CDR use case
    """
    price_per_second = 2

    # no, I'm lying, we just look at duration, but that's the idea...
    df = action_data[["DURATION"]] * price_per_second

    # must return a dataframe with a single column named "result"
    return df.rename(columns={"DURATION": "result"})


def compute_sms_value(action_data):
    """
        Computes the value of an call based on duration, onnet/offnet and time
        of the day.

        This is meant to be called in an Apply of the CDR use case
    """
    return pd.DataFrame({"result": 10}, index=action_data.index)


def compute_cdr_type(action_data):
    """
        Computes the ONNET/OFFNET value based on the operator ids

        This is meant to be called in an Apply of the CDR use case
    """
    def onnet(row):
        return (row["OPERATOR_A"] == "OPERATOR_0") & (row["OPERATOR_B"]
                                                      == "OPERATOR_0")

    result = pd.DataFrame(action_data.apply(onnet, axis=1),
                          columns=["result_b"])

    result["result"] = result["result_b"].map(
        {True: "ONNET", False: "OFFNET"})

    return result[["result"]]


def compute_call_status(action_data):
    dropped = action_data["CELL_A_ACCEPTS"] & action_data["CELL_B_ACCEPTS"]

    status = dropped.map({True: "OK", False: "DROPPED"})
    return pd.DataFrame({"result": status})


def add_communications(circus, customers, cells, seeder):
    """
    Adds Calls and SMS actions, which in turn may trigger topups actions.
    """

    # generators for topups and call duration
    recharge_trigger = DependentTriggerGenerator(
        value_mapper=logistic(a=-0.01, b=10.), seed=seeder.next())

    voice_duration_generator = NumpyRandomGenerator(
        method="choice", a=range(20, 240), seed=seeder.next())

    # call and sms timer generator, depending on the day of the week
    call_timegen = WeekProfiler(clock=circus.clock,
                                week_profile=[5., 5., 5., 5., 5., 3., 3.],
                                seed=seeder.next())

    # call activity level, under normal and "excited" states
    normal_call_activity = ScaledParetoGenerator(m=10, a=1.2,
                                                 seed=seeder.next())
    excited_call_activity = ScaledParetoGenerator(m=100, a=1.1,
                                                  seed=seeder.next())

    # after a call or SMS, excitability is the probability of getting into
    # "excited" mode (i.e., having a shorted expected delay until next call
    excitability_gen = NumpyRandomGenerator(method="beta", a=7, b=3,
                                            seed=seeder.next())

    excitability = Attribute(ids=customers.ids,
                             init_values_gen=excitability_gen)
    customers.add_attribute(name="EXCITABILITY", attr=excitability)

    # same "basic" trigger, without any value mapper
    flat_trigger = DependentTriggerGenerator(seed=seeder.next())

    back_to_normal_prob = NumpyRandomGenerator(method="beta", a=3, b=7,
                                               seed=seeder.next())

    # Calls and SMS actions themselves
    calls = ActorAction(
        name="calls",

        triggering_actor=customers,
        actorid_field="A_ID",

        timer_gen=call_timegen,
        activity=normal_call_activity,

        states={
            "excited": {
                "activity": excited_call_activity,
                "back_to_default_probability": back_to_normal_prob}
        }
    )

    sms = ActorAction(
        name="sms",

        triggering_actor=customers,
        actorid_field="A_ID",

        timer_gen=call_timegen,
        activity=normal_call_activity,

        states={
            "excited": {
                "activity": excited_call_activity,
                "back_to_default_probability": back_to_normal_prob}
        }
    )

    # common logic between Call and SMS: selecting A and B + their related
    # fields
    compute_ab_fields = Chain(
        circus.clock.ops.timestamp(named_as="DATETIME"),

        # selects a B party
        customers.get_relationship("FRIENDS").ops.select_one(from_field="A_ID",
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
                                     "CELL": "CELL_B",
                                     "EXCITABILITY": "EXCITABILITY_B"
                                     }),

        operations.Apply(source_fields=["OPERATOR_A", "OPERATOR_B"],
                         named_as="TYPE",
                         f=compute_cdr_type),
    )

    # Both CELL_A and CELL_B might drop the call based on their current HEALTH
    compute_cell_status = Chain(
        flat_trigger.ops.generate_from_attr(
            observed_attribute=cells.get_attribute("HEALTH"),
            observed_attribute_actor_id_field="CELL_A",
            named_as="CELL_A_ACCEPTS"),

        flat_trigger.ops.generate_from_attr(
            observed_attribute=cells.get_attribute("HEALTH"),
            observed_attribute_actor_id_field="CELL_B",
            named_as="CELL_B_ACCEPTS"),

        operations.Apply(source_fields=["CELL_A_ACCEPTS", "CELL_B_ACCEPTS"],
                         named_as="STATUS",
                         f=compute_call_status)
    )

    # update the main account based on the value of this CDR
    update_accounts = Chain(
        operations.Apply(source_fields=["MAIN_ACCT_OLD", "VALUE"],
                         named_as="MAIN_ACCT_NEW",
                         f=np.subtract, f_args="series"),

        customers.ops.overwrite(attribute="MAIN_ACCT",
                                copy_from_field="MAIN_ACCT_NEW"),
    )

    # triggers the topup action if the main account is low
    trigger_topups = Chain(
        # customer with low account are now more likely to topup
        recharge_trigger.ops.generate_from_field(
            observed_field="MAIN_ACCT_NEW",
            named_as="SHOULD_TOP_UP"),

        circus.get_action("topups").ops.force_act_next(
            actor_id_field="A_ID",
            condition_field="SHOULD_TOP_UP"),
    )

    # get BOTH sms and Call "bursty" after EITHER a call or an sms
    get_bursty = Chain(
        # Trigger to get into "excited" mode because A gave a call or sent an
        #  SMS
        flat_trigger.ops.generate_from_attr(
            observed_attribute=customers.get_attribute("EXCITABILITY"),
            observed_attribute_actor_id_field="A_ID",
            named_as="A_GETTING_BURSTY"),

        calls.ops.transit_to_state(actor_id_field="A_ID",
                                   condition_field="A_GETTING_BURSTY",
                                   state="excited"),
        sms.ops.transit_to_state(actor_id_field="A_ID",
                                 condition_field="A_GETTING_BURSTY",
                                 state="excited"),

        # Trigger to get into "excited" mode because B received a call
        flat_trigger.ops.generate_from_field(
            observed_field="EXCITABILITY_B",
            named_as="B_GETTING_BURSTY"),

        # transiting to excited mode, according to trigger value
        calls.ops.transit_to_state(actor_id_field="B_ID",
                                   condition_field="B_GETTING_BURSTY",
                                   state="excited"),

        sms.ops.transit_to_state(actor_id_field="B_ID",
                                 condition_field="B_GETTING_BURSTY",
                                 state="excited"),

        # B party need to have their time reset explicitally since they were
        # not active at this round. A party will be reset automatically
        calls.ops.reset_timers(actor_id_field="B_ID"),
        sms.ops.reset_timers(actor_id_field="B_ID"),
    )

    calls.set_operations(

        compute_ab_fields,
        compute_cell_status,

        ConstantGenerator(value="VOICE").ops.generate(named_as="PRODUCT"),
        voice_duration_generator.ops.generate(named_as="DURATION"),
        operations.Apply(source_fields=["DURATION", "DATETIME", "TYPE"],
                         named_as="VALUE",
                         f=compute_call_value),

        update_accounts,
        trigger_topups,
        get_bursty,

        # final CDRs
        operations.FieldLogger(log_id="voice_cdr",
                               cols=["DATETIME", "A", "B", "STATUS",
                                     "DURATION", "VALUE",
                                     "CELL_A", "OPERATOR_A",
                                     "CELL_B", "OPERATOR_B",
                                     "TYPE",   "PRODUCT"]),
    )

    sms.set_operations(

        compute_ab_fields,
        compute_cell_status,

        ConstantGenerator(value="SMS").ops.generate(named_as="PRODUCT"),
        operations.Apply(source_fields=["DATETIME", "TYPE"],
                         named_as="VALUE",
                         f=compute_sms_value),

        update_accounts,
        trigger_topups,
        get_bursty,

        # final CDRs
        operations.FieldLogger(log_id="sms_cdr",
                               cols=["DATETIME", "A", "B", "STATUS",
                                     "VALUE",
                                     "CELL_A", "OPERATOR_A",
                                     "CELL_B", "OPERATOR_B",
                                     "TYPE", "PRODUCT"]),
    )

    circus.add_action(calls)
    circus.add_action(sms)


def test_cdr_scenario():

    # building the circus
    start_time = pd.Timestamp(datetime.now())

    seeder = seed_provider(master_seed=123456)
    the_clock = Clock(datetime(year=2016, month=6, day=8),
                      params["time_step"], "%d%m%Y %H:%M:%S",
                      seed=seeder.next())

    customers = Actor(params["n_customers"])

    msisdn_gen = MSISDNGenerator(countrycode="0032",
                                 prefix_list=["472", "473", "475", "476",
                                              "477", "478", "479"],
                                 length=6,
                                 seed=seeder.next())

    customers.add_attribute(name="MSISDN",
                            attr=Attribute(ids=customers.ids,
                                           init_values_gen=msisdn_gen))

    operator_gen = NumpyRandomGenerator(
        method="choice",
        a=["OPERATOR_%d" % i for i in range(4)],
        p=[.8, .05, .1, .05],
        seed=seeder.next())

    customers.add_attribute(name="OPERATOR",
                            attr=Attribute(ids=customers.ids,
                                           init_values_gen=operator_gen))

    flying = Circus(the_clock)

    cells = add_cells(flying, seeder)
    add_mobility(flying, customers, cells, seeder)
    add_social_network(customers, seeder)
    add_topups(flying, customers, seeder)
    add_communications(flying, customers, cells, seeder)
    built_time = pd.Timestamp(datetime.now())

    # running it
    logs = flying.run(n_iterations=50)
    execution_time = pd.Timestamp(datetime.now())

    for logid, lg in logs.iteritems():
        print " - some {}:\n{}\n\n".format(logid, lg.head(15).to_string())

    print "users having highest amount of calls: "
    voice_cdr = logs["voice_cdr"]
    top_users = voice_cdr["A"].value_counts().head(10)
    print top_users

    print "some dropped calls: "
    dropped_calls = voice_cdr[voice_cdr["STATUS"] == "DROPPED"]

    if dropped_calls.shape[0] > 15:
        print dropped_calls.sample(15).sort_values("DATETIME")
    else:
        print dropped_calls

    customers = flying.get_actor_of(action_name="calls").to_dataframe()
    df = customers[customers["MSISDN"].isin(top_users.index)]
    print df

    all_logs_size = np.sum(df.shape[0] for df in logs.values())
    print "\ntotal number of logs: {}".format(all_logs_size)

    print """\nexecution times: "
     - building the circus: {}
     - running the simulation: {}
    """.format(built_time - start_time, execution_time - built_time)

