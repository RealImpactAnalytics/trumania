from __future__ import division

from datetime import datetime

from datagenerator.action import *
from datagenerator.actor import *
from datagenerator.circus import *
from datagenerator.clock import *
from datagenerator.random_generators import *
from datagenerator.relationship import *
from datagenerator.util_functions import *


def create_subs_and_sims(seeder, params):
    """
    Creates the subs and sims + a relationship between them + an agent
    relationship.

    We have at least one sim per subs: sims.size >= subs.size

    The sims actor contains the "OPERATOR", "MAIN_ACCT" and "MSISDN" attributes.

    The subs actor has a "SIMS" relationship that points to the sims owned by
    each subs.

    The sims actor also has a relationship to the set of agents where this sim
    can be topped up.
    """

    npgen = RandomState(seed=seeder.next())

    # subs are empty here but will receive a "CELLS" and "EXCITABILITY"
    # attributes later on
    subs = Actor(size=params["n_subscribers"], prefix="subs_")
    number_of_operators = npgen.choice(a=range(1, 5), size=subs.size)

    operator_ids = build_ids(size=4, prefix="OPERATOR_", max_length=1)

    def pick_operators(qty):
        """
        randomly choose a set of unique operators of specified size
        """
        return npgen.choice(a=operator_ids,
                            p=[.8, .05, .1, .05],
                            size=qty,
                            replace=False).tolist()

    # set of operator of each sub
    subs_operators_list = map(pick_operators, number_of_operators)

    # Dataframe with 4 columns for the 1rst, 2nd,... operator of each subs.
    # Since subs_operators_list don't all have the size, some entries of this
    # dataframe contains None, which are just discarded by the stack() below
    subs_operators_df = pd.DataFrame(subs_operators_list, index=subs.ids)

    # same thing vertically: the index contains the sub id (with duplicates)
    # and "operator" one of the operators of this subs
    subs_ops_mapping = subs_operators_df.stack()
    subs_ops_mapping.index = subs_ops_mapping.index.droplevel(level=1)
    subs_ops_mapping.columns = ["operator"]

    # SIM actor, each with a OPERATOR and MAIN_ACCT attribute
    # TODO: we could specify communication cost rates as attributes :)
    sims = Actor(size=subs_ops_mapping.size, prefix="SIMS_")
    sims.create_attribute("OPERATOR", init_values=subs_ops_mapping["operator"])
    recharge_gen = ConstantGenerator(value=1000.)
    sims.create_attribute(name="MAIN_ACCT", init_values_gen=recharge_gen)

    # keep ing track of the link between actor and sims as a relationship
    sims_of_subs = subs.create_relationship("SIMS", seed=seeder.next())
    sims_of_subs.add_relations(
        from_ids=subs_ops_mapping.index,
        to_ids=sims.ids)

    msisdn_gen = MSISDNGenerator(countrycode="0032",
                                 prefix_list=["472", "473", "475", "476",
                                              "477", "478", "479"],
                                 length=6,
                                 seed=seeder.next())
    sims.create_attribute(name="MSISDN", init_values_gen=msisdn_gen)

    # Finally, adding one more relationship that defines the set of possible
    # shops where we can topup each SIM.
    # TODO: to make this a bit more realistic, we should probably generate
    # such relationship first from the actor to the shops, and then copy
    # the information to each SIM, maybe with some fluctuation to account
    # for the fact that not shop is providing topups of each operator.
    agents = build_ids(params["n_agents"], prefix="AGENT_", max_length=3)

    agent_df = pd.DataFrame.from_records(
        make_random_bipartite_data(sims.ids, agents, 0.3, seed=seeder.next()),
        columns=["SIM_ID", "AGENT"])

    logging.info(" creating random sim/agent relationship ")
    sims_agents_rel = sims.create_relationship("POSSIBLE_AGENTS",
                                               seed=seeder.next())

    agent_weight_gen = NumpyRandomGenerator(method="exponential", scale=1.,
                                            seed=seeder.next())

    sims_agents_rel.add_relations(from_ids=agent_df["SIM_ID"],
                                  to_ids=agent_df["AGENT"],
                                  weights=agent_weight_gen.generate(
                                    agent_df.shape[0]))

    return subs, sims, recharge_gen


def add_cells(circus, seeder, params):
    """
    Creates the CELL actors + the actions to let them randomly break down and
    get back up
    """
    logging.info("Adding cells ")

    cells = Actor(prefix="CELL_", size=params["n_cells"])

    # the cell "health" is its probability of accepting a call. By default
    # let's says it's one expected failure every 1000 calls
    healthy_level_gen = NumpyRandomGenerator(method="beta", a=999, b=1,
                                             seed=seeder.next())

    # tendency is inversed in case of broken cell: it's probability of
    # accepting a call is much lower
    unhealthy_level_gen = NumpyRandomGenerator(method="beta", a=1, b=999,
                                               seed=seeder.next())

    cells.create_attribute(name="HEALTH", init_values_gen=healthy_level_gen)

    # same profiler for breakdown and repair: they are both related to
    # typical human activity
    default_day_profiler = DayProfiler(circus.clock)

    cell_break_down_action = Action(
        name="cell_break_down",

        initiating_actor=cells,
        actorid_field="CELL_ID",

        timer_gen=default_day_profiler,

        # fault activity is very low: most cell tend never to break down (
        # hopefully...)
        activity_gen=ScaledParetoGenerator(m=5, a=1.4, seed=seeder.next())
    )

    cell_repair_action = Action(
        name="cell_repair_down",

        initiating_actor=cells,
        actorid_field="CELL_ID",

        timer_gen=default_day_profiler,

        # repair activity is much higher
        activity_gen=ScaledParetoGenerator(m=100, a=1.2, seed=seeder.next()),

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

    circus.add_actions(cell_break_down_action, cell_repair_action)

    return cells


def add_mobility(circus, subs, cells, seeder):
    """
    adds a CELL attribute to the customer actor + a mobility action that
    randomly moves customers from CELL to CELL among their used cells.
    """
    logging.info("Adding mobility ")

    # mobility time profile: assign high mobility activities to busy hours
    # of the day
    mov_prof = [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.,
                1., 1., 5., 10., 5., 1., 1., 1., 1.]
    mobility_time_gen = DayProfiler(circus.clock, mov_prof, seed=seeder.next())

    # Mobility network, i.e. choice of cells per user, i.e. these are the
    # weighted "used cells" (as in "most used cells) for each user
    mobility_weight_gen = NumpyRandomGenerator(
        method="exponential", scale=1., seed=seeder.next())

    mobility_rel = subs.create_relationship("POSSIBLE_CELLS",
                                            seed=seeder.next())

    logging.info(" creating bipartite graph ")
    mobility_df = pd.DataFrame.from_records(
        make_random_bipartite_data(subs.ids, cells.ids, 0.4,
                                   seed=seeder.next()),
        columns=["USER_ID", "CELL"])

    logging.info(" adding mobility relationships to customer")
    mobility_rel.add_relations(
        from_ids=mobility_df["USER_ID"],
        to_ids=mobility_df["CELL"],
        weights=mobility_weight_gen.generate(mobility_df.shape[0]))

    logging.info(" creating customer's CELL attribute ")

    # Initialize the mobility by allocating one first random cell to each
    # customer among its network
    subs.create_attribute(name="CELL", init_relationship="POSSIBLE_CELLS")

    # Mobility action itself, basically just a random hop from cell to cell,
    # that updates the "CELL" attributes + generates mobility logs
    logging.info(" creating mobility action")
    mobility_action = Action(
        name="mobility",

        initiating_actor=subs,
        actorid_field="A_ID",

        timer_gen=mobility_time_gen,
    )

    logging.info(" adding operations")
    mobility_action.set_operations(
        subs.ops.lookup(actor_id_field="A_ID", select={"CELL": "PREV_CELL"}),

        # selects a destination cell (or maybe the same as current... ^^)
        mobility_rel.ops.select_one(from_field="A_ID", named_as="NEW_CELL"),

        # update the CELL attribute of the customers accordingly
        subs.ops.overwrite(attribute="CELL", copy_from_field="NEW_CELL"),

        circus.clock.ops.timestamp(named_as="TIME"),

        # create mobility logs
        operations.FieldLogger(log_id="mobility_logs",
                               cols=["TIME", "A_ID", "OPERATOR",
                                     "PREV_CELL", "NEW_CELL", ]),
    )

    circus.add_action(mobility_action)
    logging.info(" done")


def add_social_network(subs, seeder, params):
    """
    Creates a random relationship from and to customers, to represent the
    social network
    """
    logging.info("Creating the social network ")

    # create a random A to B symmetric relationship
    network_weight_gen = ScaledParetoGenerator(m=1., a=1.2, seed=seeder.next())

    social_network_values = create_er_social_network(
        customer_ids=subs.ids,
        p=params["average_degree"] / params["n_subscribers"],
        seed=seeder.next())

    social_network = subs.create_relationship("FRIENDS", seed=seeder.next())
    social_network.add_relations(
        from_ids=social_network_values["A"].values,
        to_ids=social_network_values["B"].values,
        weights=network_weight_gen.generate(social_network_values.shape[0]))

    social_network.add_relations(
        from_ids=social_network_values["B"].values,
        to_ids=social_network_values["A"].values,
        weights=network_weight_gen.generate(social_network_values.shape[0]))


def add_topups(circus, sims, recharge_gen):
    """
    The topups are not triggered by a timer_gen and a decrementing timer =>
        by itself this action is permanently inactive. This action is meant
        to be triggered externally (from the "calls" or "sms" actions)
    """
    logging.info("Adding topups actions")

    # topup action itself, basically just a selection of an agent and subsequent
    # computation of the value
    topup_action = Action(
        name="topups",
        initiating_actor=sims,
        actorid_field="SIM_ID",

        # note that there is timegen specified => the clock is not ticking
        # => the action can only be set externally (cf calls action)
    )

    topup_action.set_operations(
        sims.ops.lookup(
            actor_id_field="SIM_ID",
            select={"MSISDN": "CUSTOMER_NUMBER",
                    "OPERATOR": "OPERATOR",
                    "MAIN_ACCT": "MAIN_ACCT_OLD"}),

        sims.get_relationship("POSSIBLE_AGENTS").ops.select_one(
            from_field="SIM_ID",
            named_as="AGENT"),

        recharge_gen.ops.generate(named_as="VALUE"),

        operations.Apply(source_fields=["VALUE", "MAIN_ACCT_OLD"],
                         named_as="MAIN_ACCT",
                         f=np.add, f_args="series"),

        sims.ops.overwrite(attribute="MAIN_ACCT", copy_from_field="MAIN_ACCT"),

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
    """
    return pd.DataFrame({"result": 10}, index=action_data.index)


def compute_cdr_type(action_data):
    """
        Computes the ONNET/OFFNET value based on the operator ids
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
    is_accepted = action_data["CELL_A_ACCEPTS"] & action_data["CELL_B_ACCEPTS"]
    status = is_accepted.map({True: "OK", False: "DROPPED"})
    return pd.DataFrame({"result": status})


def select_sims(action_data):
    # select a sim in A and B depending on the fact that they share an operator
    # TODO this could be enriched, e.g. taking into account price rates between
    # each operator...
    pass

def add_communications(circus, subs, sims, cells, seeder):
    """
    Adds Calls and SMS actions, which in turn may trigger topups actions.
    """
    logging.info("Adding calls and sms actions ")

    # generators for topups and call duration
    voice_duration_generator = NumpyRandomGenerator(
        method="choice", a=range(20, 240), seed=seeder.next())

    # call and sms timer generator, depending on the day of the week
    call_timegen = WeekProfiler(clock=circus.clock,
                                week_profile=[5., 5., 5., 5., 5., 3., 3.],
                                seed=seeder.next())

    # probability of doing a topup, with high probability when the depended
    # variable (i.e. the main account value, see below) gets close to 0
    recharge_trigger = DependentTriggerGenerator(
        value_mapper=logistic(a=-0.01, b=10.), seed=seeder.next())

    # call activity level, under normal and "excited" states
    normal_call_activity = ScaledParetoGenerator(m=10, a=1.2,
                                                 seed=seeder.next())
    excited_call_activity = ScaledParetoGenerator(m=100, a=1.1,
                                                  seed=seeder.next())

    # after a call or SMS, excitability is the probability of getting into
    # "excited" mode (i.e., having a shorted expected delay until next call
    excitability_gen = NumpyRandomGenerator(method="beta", a=7, b=3,
                                            seed=seeder.next())

    subs.create_attribute(name="EXCITABILITY", init_values_gen=excitability_gen)

    # same "basic" trigger, without any value mapper
    flat_trigger = DependentTriggerGenerator(seed=seeder.next())

    back_to_normal_prob = NumpyRandomGenerator(method="beta", a=3, b=7,
                                               seed=seeder.next())

    # Calls and SMS actions themselves
    calls = Action(
        name="calls",

        initiating_actor=subs,
        actorid_field="A_ID",

        timer_gen=call_timegen,
        activity_gen=normal_call_activity,

        states={
            "excited": {
                "activity": excited_call_activity,
                "back_to_default_probability": back_to_normal_prob}
        }
    )

    # sms = Action(
    #     name="sms",
    #
    #     triggering_actor=subs,
    #     actorid_field="A_ID",
    #
    #     timer_gen=call_timegen,
    #     activity=normal_call_activity,
    #
    #     states={
    #         "excited": {
    #             "activity": excited_call_activity,
    #             "back_to_default_probability": back_to_normal_prob}
    #     }
    # )

    # common logic between Call and SMS: selecting A and B + their related
    # fields
    compute_ab_fields = Chain(
        circus.clock.ops.timestamp(named_as="DATETIME"),

        # selects a B party
        subs.get_relationship("FRIENDS").ops.select_one(from_field="A_ID",
                                                        named_as="B_ID",
                                                        one_to_one=True),

        # fetches information about all SIMs of A and B
        subs.get_relationship("SIMS").ops.select_all(from_field="A_ID",
                                                     named_as="A_SIMS"),
        sims.ops.lookup(actor_id_field="A_SIMS",
                        select={
                            "OPERATOR": "OPERATORS_A",
                            "MSISDN": "MSISDNS_A",
                            "MAIN_ACCT": "MAIN_ACCTS_A"
                        }),

        subs.get_relationship("SIMS").ops.select_all(from_field="B_ID",
                                                     named_as="B_SIMS"),
        sims.ops.lookup(actor_id_field="B_SIMS",
                        select={
                            "OPERATOR": "OPERATORS_B",
                            "MSISDN": "MSISDNS_B",
                        }),


        # selects the sims and related values based on the best match
        # between the sims of A and B
        # TODO: Apply needs to be able to return several values
        operations.Apply(source_fields=["MSISDNS_A", "OPERATORS_A", "A_SIMS",
                                        "MAIN_ACCTS_A",
                                        "MSISDNS_B", "OPERATORS_B", "B_SIMS"],
                         named_as=["MSISDN_A", "OPERATOR_A", "A_SIM",
                                   "MAIN_ACCT_OLD",
                                   "MSISDN_B", "OPERATOR_B", "B_SIM"],
                         f=select_sims),

        # some static fields
        subs.ops.lookup(actor_id_field="A_ID",
                        select={"CELL": "CELL_A",
                                "EXCITABILITY": "EXCITABILITY_A"}),

        subs.ops.lookup(actor_id_field="B_ID",
                        select={"CELL": "CELL_B",
                                "EXCITABILITY": "EXCITABILITY_B"}),

        operations.Apply(source_fields=["OPERATOR_A", "OPERATOR_B"],
                         named_as="TYPE",
                         f=compute_cdr_type),
    )

    # Both CELL_A and CELL_B might drop the call based on their current HEALTH
    compute_cell_status = Chain(
        cells.ops.lookup(actor_id_field="CELL_A",
                         select={"HEALTH": "CELL_A_HEALTH"}),

        cells.ops.lookup(actor_id_field="CELL_B",
                         select={"HEALTH": "CELL_B_HEALTH"}),

        flat_trigger.ops.generate(observed_field="CELL_A_HEALTH",
                                  named_as="CELL_A_ACCEPTS"),

        flat_trigger.ops.generate(observed_field="CELL_B_HEALTH",
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

        # TODO: bug here: we need a actor_id_field again...
        sims.ops.overwrite(attribute="MAIN_ACCT",
                           copy_from_field="MAIN_ACCT_NEW"),
    )

    # triggers the topup action if the main account is low
    trigger_topups = Chain(
        # subscribers with low account are now more likely to topup
        recharge_trigger.ops.generate(
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
        flat_trigger.ops.generate(observed_field="EXCITABILITY_A",
                                  named_as="A_GETTING_BURSTY"),

        calls.ops.transit_to_state(actor_id_field="A_ID",
                                   condition_field="A_GETTING_BURSTY",
                                   state="excited"),
        # sms.ops.transit_to_state(actor_id_field="A_ID",
        #                          condition_field="A_GETTING_BURSTY",
        #                          state="excited"),

        # Trigger to get into "excited" mode because B received a call
        flat_trigger.ops.generate(observed_field="EXCITABILITY_B",
                                  named_as="B_GETTING_BURSTY"),

        # transiting to excited mode, according to trigger value
        calls.ops.transit_to_state(actor_id_field="B_ID",
                                   condition_field="B_GETTING_BURSTY",
                                   state="excited"),

        # sms.ops.transit_to_state(actor_id_field="B_ID",
        #                          condition_field="B_GETTING_BURSTY",
        #                          state="excited"),
        #
        # # B party need to have their time reset explicitally since they were
        # # not active at this round. A party will be reset automatically
        # calls.ops.reset_timers(actor_id_field="B_ID"),
        # sms.ops.reset_timers(actor_id_field="B_ID"),
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
                               cols=["DATETIME", "MSISDN_A", "MSISDN_B",
                                     "STATUS", "DURATION", "VALUE",
                                     "CELL_A", "OPERATOR_A",
                                     "CELL_B", "OPERATOR_B",
                                     "TYPE",   "PRODUCT"]),
    )

    # sms.set_operations(
    #
    #     compute_ab_fields,
    #     compute_cell_status,
    #
    #     ConstantGenerator(value="SMS").ops.generate(named_as="PRODUCT"),
    #     operations.Apply(source_fields=["DATETIME", "TYPE"],
    #                      named_as="VALUE",
    #                      f=compute_sms_value),
    #
    #     update_accounts,
    #     trigger_topups,
    #     get_bursty,
    #
    #     # final CDRs
    #     operations.FieldLogger(log_id="sms_cdr",
    #                            cols=["DATETIME", "A", "B", "STATUS",
    #                                  "VALUE",
    #                                  "CELL_A", "OPERATOR_A",
    #                                  "CELL_B", "OPERATOR_B",
    #                                  "TYPE", "PRODUCT"]),
    # )

    circus.add_actions(calls,
#                       sms
                       )



def build_cdr_scenario(params):

    seeder = seed_provider(master_seed=123456)
    the_clock = Clock(start=datetime(year=2016, month=6, day=8),
                      step_s=params["time_step"],
                      format_for_out="%d%m%Y %H:%M:%S",
                      seed=seeder.next())

    logging.info("building subscriber actors ")

    flying = Circus(the_clock)

    subs, sims, recharge_gen = create_subs_and_sims(seeder, params)
    cells = add_cells(flying, seeder, params)
    add_mobility(flying, subs, cells, seeder)
    add_social_network(subs, seeder, params)
    add_topups(flying, sims, recharge_gen)
    add_communications(flying, subs, sims, cells, seeder)

    return flying


def run_cdr_scenario(params):
    # building the circus
    start_time = pd.Timestamp(datetime.now())

    flying = build_cdr_scenario(params)
    built_time = pd.Timestamp(datetime.now())

    # running it
    logs = flying.run(n_iterations=params["n_iterations"])
    execution_time = pd.Timestamp(datetime.now())

    for logid, lg in logs.iteritems():
        logging.info(" - some {}:\n{}\n\n".format(logid, lg.head(
            15).to_string()))

    logging.info("users having highest amount of calls: ")
    voice_cdr = logs["voice_cdr"]
    top_users = voice_cdr["A"].value_counts().head(10)
    logging.info(top_users)

    logging.info("some dropped calls: ")
    dropped_calls = voice_cdr[voice_cdr["STATUS"] == "DROPPED"]

    if dropped_calls.shape[0] > 15:
        logging.info(dropped_calls.sample(15).sort_values("DATETIME"))
    else:
        logging.info(dropped_calls)

    customers = flying.get_actor_of(action_name="calls").to_dataframe()
    df = customers[customers["MSISDN"].isin(top_users.index)]
    logging.info(df)

    all_logs_size = np.sum(df.shape[0] for df in logs.values())
    logging.info("\ntotal number of logs: {}".format(all_logs_size))

    logging.info("""\nexecution times: "
     - building the circus: {}
     - running the simulation: {}
    """.format(built_time - start_time, execution_time - built_time))


def test_cdr_scenario():

    setup_logging()
    logging.info("test_cdr_scenario")

    params = {
        "time_step": 60,
        "n_cells": 100,
        "n_agents": 100,
        "n_subscribers": 1000,
        "average_degree": 20,
        "n_iterations": 50
    }

    run_cdr_scenario(params)

