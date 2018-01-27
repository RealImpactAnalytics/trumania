from __future__ import division
import pandas as pd
import logging
from datetime import datetime
from numpy.random import RandomState
import numpy as np

from trumania.core.util_functions import setup_logging, load_all_logs, build_ids, make_random_bipartite_data
from trumania.core.clock import CyclicTimerProfile, CyclicTimerGenerator
from trumania.core.random_generators import SequencialGenerator, NumpyRandomGenerator, ConstantGenerator
from trumania.core.random_generators import MSISDNGenerator, ParetoGenerator, DependentTriggerGenerator
from trumania.core.circus import Circus
from trumania.core.operations import Chain
from trumania.components.geographies.uganda import WithUganda
from trumania.components.geographies.random_geo import WithRandomGeo
from trumania.components.social_networks.erdos_renyi import WithErdosRenyi
from trumania.components.time_patterns.profilers import HighWeekDaysTimerGenerator
from trumania.core import operations


# couple of utility methods called in Apply of this scenario
def compute_call_value(story_data):
    """
        Computes the value of a call based on duration, onnet/offnet and time
        of the day.

        This is meant to be called in an Apply of the CDR use case
    """
    price_per_second = 2

    # no, I'm lying, we just look at duration, but that's the idea...
    df = story_data[["DURATION"]] * price_per_second

    # must return a dataframe with a single column named "result"
    return df.rename(columns={"DURATION": "result"})


def compute_sms_value(story_data):
    """
        Computes the value of an call based on duration, onnet/offnet and time
        of the day.
    """
    return pd.DataFrame({"result": 10}, index=story_data.index)


def compute_cdr_type(story_data):
    """
        Computes the ONNET/OFFNET value based on the operator ids
    """

    def onnet(row):
        return (row["OPERATOR_A"] == "OPERATOR_0") & (row["OPERATOR_B"] == "OPERATOR_0")

    result = pd.DataFrame(story_data.apply(onnet, axis=1),
                          columns=["result_b"])

    result["result"] = result["result_b"].map(
        {True: "ONNET", False: "OFFNET"})

    return result[["result"]]


def compute_call_status(story_data):
    is_accepted = story_data["CELL_A_ACCEPTS"] & story_data["CELL_B_ACCEPTS"]
    status = is_accepted.map({True: "OK", False: "DROPPED"})
    return pd.DataFrame({"result": status})


def select_sims(story_data):
    """
    this function expects the following columns in story_data:

    ["MSISDNS_A", "OPERATORS_A", "A_SIMS", "MAIN_ACCTS_A",
     "MSISDNS_B", "OPERATORS_B", "B_SIMS"]

     => it selects the most appropriate OPERATOR for this call and return the
     following values:

     "MSISDN_A", "OPERATOR_A", "SIM_A", "MAIN_ACCT_OLD",
     "MSISDN_B", "OPERATOR_B", "SIM_B"
    """

    def do_select(row):

        common_operators = set(row["OPERATORS_A"]) & set(row["OPERATORS_B"])

        if len(common_operators) > 0:
            # A and B have at least an operator in common => using that
            operator_a = operator_b = list(common_operators)[0]
            a_idx = row["OPERATORS_A"].index(operator_a)
            b_idx = row["OPERATORS_B"].index(operator_b)

        else:
            # otherwise, just use any (we could look at lowest rates here...)
            a_idx = b_idx = 0

        return pd.Series([
            row["MSISDNS_A"][a_idx], row["OPERATORS_A"][a_idx],
            row["A_SIMS"][a_idx], row["MAIN_ACCTS_A"][a_idx],
            row["MSISDNS_B"][b_idx], row["OPERATORS_B"][b_idx],
            row["B_SIMS"][b_idx]])

    return story_data.apply(do_select, axis=1)


class CdrScenario(WithErdosRenyi, WithRandomGeo, WithUganda, Circus):
    """
        Main CDR calls, sms, topus, mobility,... scenario
    """

    def __init__(self, params):
        self.params = params

        logging.info("building subscriber populations ")

        Circus.__init__(self,
                        name="test_cdr_circus",
                        master_seed=123456,
                        start=pd.Timestamp("8 June 2016"),
                        step_duration=pd.Timedelta(params["time_step"]),)

        subs, sims, recharge_gen = self.create_subs_and_sims()
        cells, cities = self.add_uganda_geography(force_build=True)
        self.add_mobility(subs, cells)
        self.add_er_social_network_relationship(
            subs,
            relationship_name="FRIENDS",
            average_degree=params["average_degree"])

        self.add_topups(sims, recharge_gen)
        self.add_communications(subs, sims, cells)

    def create_subs_and_sims(self):
        """
        Creates the subs and sims + a relationship between them + an agent
        relationship.

        We have at least one sim per subs: sims.size >= subs.size

        The sims population contains the "OPERATOR", "MAIN_ACCT" and "MSISDN" attributes.

        The subs population has a "SIMS" relationship that points to the sims owned by
        each subs.

        The sims population also has a relationship to the set of agents where this sim
        can be topped up.
        """

        npgen = RandomState(seed=next(self.seeder))

        # subs are empty here but will receive a "CELLS" and "EXCITABILITY"
        # attributes later on
        subs = self.create_population(name="subs",
                                      size=self.params["n_subscribers"],
                                      ids_gen=SequencialGenerator(prefix="SUBS_"))

        number_of_operators = npgen.choice(a=range(1, 5), size=subs.size)
        operator_ids = build_ids(size=4, prefix="OPERATOR_", max_length=1)

        def pick_operators(qty):
            """
            randomly choose a set of unique operators of specified size
            """
            return npgen.choice(a=operator_ids, p=[.8, .05, .1, .05], size=qty,
                                replace=False).tolist()

        # set of operators of each subs
        subs_operators_list = map(pick_operators, number_of_operators)

        # Dataframe with 4 columns for the 1rst, 2nd,... operator of each subs.
        # Since subs_operators_list don't all have the size, some entries of this
        # dataframe contains None, which are just discarded by the stack() below
        subs_operators_df = pd.DataFrame(data=list(subs_operators_list), index=subs.ids)

        # same info, vertically: the index contains the sub id (with duplicates)
        # and "operator" one of the operators of this subs
        subs_ops_mapping = subs_operators_df.stack()
        subs_ops_mapping.index = subs_ops_mapping.index.droplevel(level=1)

        # SIM population, each with an OPERATOR and MAIN_ACCT attributes
        sims = self.create_population(name="sims",
                                      size=subs_ops_mapping.size,
                                      ids_gen=SequencialGenerator(prefix="SIMS_"))
        sims.create_attribute("OPERATOR", init_values=subs_ops_mapping.values)
        recharge_gen = ConstantGenerator(value=1000.)
        sims.create_attribute(name="MAIN_ACCT", init_gen=recharge_gen)

        # keeping track of the link between population and sims as a relationship
        sims_of_subs = subs.create_relationship("SIMS")
        sims_of_subs.add_relations(
            from_ids=subs_ops_mapping.index,
            to_ids=sims.ids)

        msisdn_gen = MSISDNGenerator(countrycode="0032",
                                     prefix_list=["472", "473", "475", "476",
                                                  "477", "478", "479"],
                                     length=6, seed=next(self.seeder))
        sims.create_attribute(name="MSISDN", init_gen=msisdn_gen)

        # Finally, adding one more relationship that defines the set of possible
        # shops where we can topup each SIM.
        # TODO: to make this a bit more realistic, we should probably generate
        # such relationship first from the subs to their favourite shops, and then
        # copy that info to each SIM, maybe with some fluctuations to account
        # for the fact that not all shops provide topups of all operators.
        agents = build_ids(self.params["n_agents"], prefix="AGENT_", max_length=3)

        agent_df = pd.DataFrame.from_records(
            make_random_bipartite_data(sims.ids, agents, 0.3, seed=next(self.seeder)),
            columns=["SIM_ID", "AGENT"])

        logging.info(" creating random sim/agent relationship ")
        sims_agents_rel = sims.create_relationship("POSSIBLE_AGENTS")

        agent_weight_gen = NumpyRandomGenerator(
            method="exponential", scale=1., seed=next(self.seeder))

        sims_agents_rel.add_relations(
            from_ids=agent_df["SIM_ID"],
            to_ids=agent_df["AGENT"],
            weights=agent_weight_gen.generate(agent_df.shape[0]))

        return subs, sims, recharge_gen

    def add_mobility(self, subs, cells):
        """
        adds a CELL attribute to the customer population + a mobility story that
        randomly moves customers from CELL to CELL among their used cells.
        """
        logging.info("Adding mobility ")

        # mobility time profile: assign high mobility activities to busy hours
        # of the day
        mov_prof = [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.,
                    1., 1., 5., 10., 5., 1., 1., 1., 1.]
        mobility_time_gen = CyclicTimerGenerator(
            clock=self.clock,
            seed=next(self.seeder),
            config=CyclicTimerProfile(
                profile=mov_prof,
                profile_time_steps="1H",
                start_date=pd.Timestamp("12 September 2016 00:00.00")
            )
        )

        # Mobility network, i.e. choice of cells per user, i.e. these are the
        # weighted "used cells" (as in "most used cells) for each user
        mobility_weight_gen = NumpyRandomGenerator(
            method="exponential", scale=1., seed=next(self.seeder))

        mobility_rel = subs.create_relationship("POSSIBLE_CELLS")

        logging.info(" creating bipartite graph ")
        mobility_df = pd.DataFrame.from_records(
            make_random_bipartite_data(subs.ids, cells.ids, 0.4,
                                       seed=next(self.seeder)),
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

        # Mobility story itself, basically just a random hop from cell to cell,
        # that updates the "CELL" attributes + generates mobility logs
        logging.info(" creating mobility story")
        mobility_story = self.create_story(
            name="mobility",

            initiating_population=subs,
            member_id_field="A_ID",

            timer_gen=mobility_time_gen,
        )

        logging.info(" adding operations")
        mobility_story.set_operations(
            subs.ops.lookup(id_field="A_ID", select={"CELL": "PREV_CELL"}),

            # selects a destination cell (or maybe the same as current... ^^)
            mobility_rel.ops.select_one(from_field="A_ID", named_as="NEW_CELL"),

            # update the CELL attribute of the customers accordingly
            subs.get_attribute("CELL").ops.update(
                member_id_field="A_ID",
                copy_from_field="NEW_CELL"),

            self.clock.ops.timestamp(named_as="TIME"),

            # create mobility logs
            operations.FieldLogger(log_id="mobility_logs",
                                   cols=["TIME", "A_ID", "PREV_CELL", "NEW_CELL"]),
        )

        logging.info(" done")

    def add_topups(self, sims, recharge_gen):
        """
        The topups are not triggered by a timer_gen and a decrementing timer =>
            by itself this story is permanently inactive. This story is meant
            to be triggered externally (from the "calls" or "sms" stories)
        """
        logging.info("Adding topups stories")

        # topup story itself, basically just a selection of an agent and subsequent
        # computation of the value
        topup_story = self.create_story(
            name="topups",
            initiating_population=sims,
            member_id_field="SIM_ID",

            # note that there is timegen specified => the clock is not ticking
            # => the story can only be set externally (cf calls story)
        )

        topup_story.set_operations(
            sims.ops.lookup(
                id_field="SIM_ID",
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

            sims.get_attribute("MAIN_ACCT").ops.update(
                member_id_field="SIM_ID",
                copy_from_field="MAIN_ACCT"),

            self.clock.ops.timestamp(named_as="TIME"),

            operations.FieldLogger(log_id="topups",
                                   cols=["TIME", "CUSTOMER_NUMBER", "AGENT",
                                         "VALUE", "OPERATOR",
                                         "MAIN_ACCT_OLD", "MAIN_ACCT"]),
        )

    def add_communications(self, subs, sims, cells):
        """
        Adds Calls and SMS story, which in turn may trigger topups story.
        """
        logging.info("Adding calls and sms story ")

        # generators for topups and call duration
        voice_duration_generator = NumpyRandomGenerator(
            method="choice", a=range(20, 240), seed=next(self.seeder))

        # call and sms timer generator, depending on the day of the week
        call_timegen = HighWeekDaysTimerGenerator(clock=self.clock,
                                                  seed=next(self.seeder))

        # probability of doing a topup, with high probability when the depended
        # variable (i.e. the main account value, see below) gets close to 0
        recharge_trigger = DependentTriggerGenerator(
            value_to_proba_mapper=operations.logistic(k=-0.01, x0=1000),
            seed=next(self.seeder))

        # call activity level, under normal and "excited" states
        normal_call_activity = ParetoGenerator(xmin=10, a=1.2,
                                               seed=next(self.seeder))
        excited_call_activity = ParetoGenerator(xmin=100, a=1.1,
                                                seed=next(self.seeder))

        # after a call or SMS, excitability is the probability of getting into
        # "excited" mode (i.e., having a shorted expected delay until next call
        excitability_gen = NumpyRandomGenerator(method="beta", a=7, b=3,
                                                seed=next(self.seeder))

        subs.create_attribute(name="EXCITABILITY", init_gen=excitability_gen)

        # same "basic" trigger, without any value mapper
        flat_trigger = DependentTriggerGenerator(seed=next(self.seeder))

        back_to_normal_prob = NumpyRandomGenerator(method="beta", a=3, b=7,
                                                   seed=next(self.seeder))

        # Calls and SMS stories themselves
        calls = self.create_story(
            name="calls",

            initiating_population=subs,
            member_id_field="A_ID",

            timer_gen=call_timegen,
            activity_gen=normal_call_activity,

            states={
                "excited": {
                    "activity": excited_call_activity,
                    "back_to_default_probability": back_to_normal_prob}
            }
        )

        sms = self.create_story(
            name="sms",

            initiating_population=subs,
            member_id_field="A_ID",

            timer_gen=call_timegen,
            activity_gen=normal_call_activity,

            states={
                "excited": {
                    "activity": excited_call_activity,
                    "back_to_default_probability": back_to_normal_prob}
            }
        )

        # common logic between Call and SMS: selecting A and B + their related
        # fields
        compute_ab_fields = Chain(
            self.clock.ops.timestamp(named_as="DATETIME"),

            # selects a B party
            subs.get_relationship("FRIENDS").ops.select_one(from_field="A_ID",
                                                            named_as="B_ID",
                                                            one_to_one=True),

            # fetches information about all SIMs of A and B
            subs.get_relationship("SIMS").ops.select_all(from_field="A_ID",
                                                                    named_as="A_SIMS"),
            sims.ops.lookup(id_field="A_SIMS",
                            select={"OPERATOR": "OPERATORS_A",
                                    "MSISDN": "MSISDNS_A",
                                    "MAIN_ACCT": "MAIN_ACCTS_A"}),

            subs.get_relationship("SIMS").ops.select_all(from_field="B_ID",
                                                                    named_as="B_SIMS"),
            sims.ops.lookup(id_field="B_SIMS",
                            select={"OPERATOR": "OPERATORS_B",
                                    "MSISDN": "MSISDNS_B"}),

            # A selects the sims and related values based on the best match
            # between the sims of A and B
            operations.Apply(source_fields=["MSISDNS_A", "OPERATORS_A", "A_SIMS",
                                            "MAIN_ACCTS_A",
                                            "MSISDNS_B", "OPERATORS_B", "B_SIMS"],
                             named_as=["MSISDN_A", "OPERATOR_A", "SIM_A",
                                       "MAIN_ACCT_OLD",
                                       "MSISDN_B", "OPERATOR_B", "SIM_B"],
                             f=select_sims),

            operations.Apply(source_fields=["OPERATOR_A", "OPERATOR_B"],
                             named_as="TYPE",
                             f=compute_cdr_type),
        )

        # Both CELL_A and CELL_B might drop the call based on their current HEALTH
        compute_cell_status = Chain(
            # some static fields
            subs.ops.lookup(id_field="A_ID",
                            select={"CELL": "CELL_A",
                                    "EXCITABILITY": "EXCITABILITY_A"}),

            subs.ops.lookup(id_field="B_ID",
                            select={"CELL": "CELL_B",
                                    "EXCITABILITY": "EXCITABILITY_B"}),

            cells.ops.lookup(id_field="CELL_A",
                             select={"HEALTH": "CELL_A_HEALTH"}),

            cells.ops.lookup(id_field="CELL_B",
                             select={"HEALTH": "CELL_B_HEALTH"}),

            flat_trigger.ops.generate(observed_field="CELL_A_HEALTH",
                                      named_as="CELL_A_ACCEPTS"),

            flat_trigger.ops.generate(observed_field="CELL_B_HEALTH",
                                      named_as="CELL_B_ACCEPTS"),

            operations.Apply(source_fields=["CELL_A_ACCEPTS", "CELL_B_ACCEPTS"],
                             named_as="STATUS", f=compute_call_status)
        )

        # update the main account based on the value of this CDR
        update_accounts = Chain(
            operations.Apply(source_fields=["MAIN_ACCT_OLD", "VALUE"],
                             named_as="MAIN_ACCT_NEW",
                             f=np.subtract, f_args="series"),

            sims.get_attribute("MAIN_ACCT").ops.update(
                member_id_field="SIM_A",
                copy_from_field="MAIN_ACCT_NEW"),
        )

        # triggers the topup story if the main account is low
        trigger_topups = Chain(
            # A subscribers with low account are now more likely to topup the
            # SIM they just used to make a call
            recharge_trigger.ops.generate(
                observed_field="MAIN_ACCT_NEW",
                named_as="SHOULD_TOP_UP"),

            self.get_story("topups").ops.force_act_next(
                member_id_field="SIM_A",
                condition_field="SHOULD_TOP_UP"),
        )

        # get BOTH sms and Call "bursty" after EITHER a call or an sms
        get_bursty = Chain(
            # Trigger to get into "excited" mode because A gave a call or sent an
            #  SMS
            flat_trigger.ops.generate(observed_field="EXCITABILITY_A",
                                      named_as="A_GETTING_BURSTY"),

            calls.ops.transit_to_state(member_id_field="A_ID",
                                       condition_field="A_GETTING_BURSTY",
                                       state="excited"),
            sms.ops.transit_to_state(member_id_field="A_ID",
                                     condition_field="A_GETTING_BURSTY",
                                     state="excited"),

            # Trigger to get into "excited" mode because B received a call
            flat_trigger.ops.generate(observed_field="EXCITABILITY_B",
                                      named_as="B_GETTING_BURSTY"),

            # transiting to excited mode, according to trigger value
            calls.ops.transit_to_state(member_id_field="B_ID",
                                       condition_field="B_GETTING_BURSTY",
                                       state="excited"),

            sms.ops.transit_to_state(member_id_field="B_ID",
                                     condition_field="B_GETTING_BURSTY",
                                     state="excited"),
            #
            # B party need to have their time reset explicitally since they were
            # not active at this round. A party will be reset automatically
            calls.ops.reset_timers(member_id_field="B_ID"),
            sms.ops.reset_timers(member_id_field="B_ID"),
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
                                         "TYPE", "PRODUCT"]),
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
                                   cols=["DATETIME", "MSISDN_A", "MSISDN_B",
                                         "STATUS", "VALUE",
                                         "CELL_A", "OPERATOR_A",
                                         "CELL_B", "OPERATOR_B",
                                         "TYPE", "PRODUCT"]),
        )


def run_cdr_scenario(params):
    setup_logging()
    logging.info("test_cdr_scenario")

    # building the circus
    start_time = pd.Timestamp(datetime.now())

    scenario = CdrScenario(params)
    built_time = pd.Timestamp(datetime.now())

    # running it
    scenario.run(duration=pd.Timedelta(params["simulation_duration"]),
                 delete_existing_logs=True,
                 log_output_folder=params["output_folder"])
    logs = load_all_logs(params["output_folder"])

    execution_time = pd.Timestamp(datetime.now())

    for logid, lg in logs.items():
        logging.info(" - some {}:\n{}\n\n".format(logid, lg.head(
            15).to_string()))

    logging.info("MSISDNs having highest amount of calls: ")
    voice_cdr = logs["voice_cdr"]
    top_users = voice_cdr["MSISDN_A"].value_counts().head(10)
    logging.info(top_users)

    logging.info("some dropped calls: ")
    dropped_calls = voice_cdr[voice_cdr["STATUS"] == "DROPPED"]

    if dropped_calls.shape[0] > 15:
        logging.info(dropped_calls.sample(15).sort_values("DATETIME"))
    else:
        logging.info(dropped_calls)

    all_logs_size = np.sum(df.shape[0] for df in logs.values())
    logging.info("\ntotal number of logs: {}".format(all_logs_size))

    for logid, lg in logs.items():
        logging.info(" {} {} logs".format(len(lg), logid))

    logging.info("""\nexecution times: "
     - building the circus: {}
     - running the simulation: {}
    """.format(built_time - start_time, execution_time - built_time))


# having this method called "test_" makes it interpreted as a unit test
def test_cdr_scenario():

    params = {
        "time_step": "60s",
        "n_cells": 100,
        "n_agents": 100,
        "n_subscribers": 1000,
        "average_degree": 20,
        "simulation_duration": "1h",
        "output_folder": "cdr_output_logs"
    }

    run_cdr_scenario(params)


if __name__ == "__main__":
    test_cdr_scenario()
