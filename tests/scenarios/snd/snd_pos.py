from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
from numpy.random import *

import snd_constants


def _create_attractiveness(circus, pos):

    logging.info("generating pos attractiveness values and evolutions")

    # "base" attractiveness, in [-50, 50]
    attractiveness_base_gen = NumpyRandomGenerator(
        method="choice",
        a=range(-50, 50),
        seed=circus.seeder.next())
    pos.create_attribute("ATTRACT_BASE", init_gen=attractiveness_base_gen)

    # attractiveness scaled into [0,1]. This is the one influencing the choice
    # of customers
    attractiveness_smoother = operations.logistic(k=.15)
    ac = attractiveness_smoother(pos.get_attribute_values("ATTRACT_BASE"))
    pos.create_attribute("ATTRACTIVENESS", init_values=ac)

    # evolution steps of the base attractiveness
    attractiveness_delta_gen = NumpyRandomGenerator(
        method="choice",
        a=[-2, -1, 0, 1, 2],
        p=[.1, .25, .3, .25, .1],
        seed=circus.seeder.next())

    pos.create_attribute("ATTRACT_DELTA", init_gen=attractiveness_delta_gen)

    # once per day the attractiveness of each POS evolves according to the delta
    attractiveness_evolution = circus.create_action(
        name="attractiveness_evolution",
        initiating_actor=pos,
        actorid_field="POS_ID",

        # exactly one attractiveness evolution per day
        timer_gen=ConstantDependentGenerator(
            value=circus.clock.n_iterations(pd.Timedelta("1 day")))
    )

    def update_attract_base(df):
        base = df.apply(lambda row: row["ATTRACT_BASE"] + row["ATTRACT_DELTA"],
                        axis=1)
        base = base.mask(base > 50, 50).mask(base < -50, -50)
        return pd.DataFrame(base, columns=["result"])

    attractiveness_evolution.set_operations(
        pos.ops.lookup(
            actor_id_field="POS_ID",
            select={
                "ATTRACT_BASE": "ATTRACT_BASE",
                "ATTRACT_DELTA": "ATTRACT_DELTA",
            }
        ),

        operations.Apply(
            source_fields=["ATTRACT_BASE", "ATTRACT_DELTA"],
            named_as="NEW_ATTRACT_BASE",
            f=update_attract_base,
        ),

        pos.get_attribute("ATTRACT_BASE").ops.update(
            actor_id_field="POS_ID",
            copy_from_field="NEW_ATTRACT_BASE"
        ),

        operations.Apply(
            source_fields=["ATTRACT_BASE"],
            named_as="NEW_ATTRACTIVENESS",
            f=attractiveness_smoother, f_args="series"
        ),

        pos.get_attribute("ATTRACTIVENESS").ops.update(
            actor_id_field="POS_ID",
            copy_from_field="NEW_ATTRACTIVENESS"
        ),

        # TODO: remove this (currently there just for debugs)
        circus.clock.ops.timestamp(named_as="TIME"),
        FieldLogger(log_id="att_updates")
    )

    delta_updater = NumpyRandomGenerator(method="choice", a=[-1, 0, 1],
                                         seed=circus.seeder.next())

    # random walk around of the attractiveness delta, once per week
    attractiveness_delta_evolution = circus.create_action(
        name="attractiveness_delta_evolution",
        initiating_actor=pos,
        actorid_field="POS_ID",

        timer_gen=ConstantDependentGenerator(
            value=circus.clock.n_iterations(pd.Timedelta("7 days")))
    )

    attractiveness_delta_evolution.set_operations(
        pos.ops.lookup(
            actor_id_field="POS_ID",
            select={"ATTRACT_DELTA": "ATTRACT_DELTA"}
        ),

        delta_updater.ops.generate(named_as="DELTA_UPDATE"),

        operations.Apply(
            source_fields=["ATTRACT_DELTA", "DELTA_UPDATE"],
            named_as="NEW_ATTRACT_DELTA",
            f=np.add, f_args="series"
        ),

        pos.get_attribute("ATTRACT_DELTA").ops.update(
            actor_id_field="POS_ID",
            copy_from_field="NEW_ATTRACT_DELTA"
        ),

        # TODO: remove this (currently there just for debugs)
        circus.clock.ops.timestamp(named_as="TIME"),
        FieldLogger(log_id="att_delta_updates")

    )


def create_pos(circus, params, sim_id_gen):

    logging.info("creating POS")
    pos = Actor(size=params["n_pos"],
                ids_gen=SequencialGenerator(prefix="POS_"))

    _create_attractiveness(circus, pos)

    logging.info("assigning a site to each POS")
    state = RandomState(circus.seeder.next())
    pos_sites = state.choice(a=circus.sites.ids,
                             size=pos.size,
                             replace=True)
    pos.create_attribute("SITE", init_values=pos_sites)

    # TODO: Add POS coordinates based on SITE coordinates
    pos.create_attribute("LATITUDE", init_gen=ConstantGenerator(0.0))
    pos.create_attribute("LONGITUDE", init_gen=ConstantGenerator(0.0))

    name_gen = NumpyRandomGenerator(method="choice",
                                    seed=circus.seeder.next(),
                                    a=snd_constants.POS_NAMES)

    pos.create_attribute("NAME", init_gen=name_gen)

    logging.info("recording the list POS per site in site relationship")
    pos_rel = circus.sites.create_relationship("POS",
                                               seed=circus.seeder.next())
    pos_rel.add_relations(
        from_ids=pos.get_attribute_values("SITE"),
        to_ids=pos.ids)

    logging.info("generating POS initial SIM stock")
    pos_sims = pos.create_relationship("SIMS", seed=circus.seeder.next())
    sim_ids = sim_id_gen.generate(size=params["n_init_sim_per_pos"] * pos.size)

    sims_dealer = make_random_assign(
        set1=sim_ids,
        set2=pos.ids,
        seed=circus.seeder.next())

    pos_sims.add_relations(
        from_ids=sims_dealer["from"],
        to_ids=sims_dealer["to"])

    logging.info("assigning a dealer to each POS")
    dealer_of_pos = make_random_assign(
        set1=circus.dealers.ids,
        set2=pos.ids,
        seed=circus.seeder.next())

    dealer_rel = pos.create_relationship("DEALER", seed=circus.seeder.next())
    dealer_rel.add_relations(
        from_ids=dealer_of_pos["from"],
        to_ids=dealer_of_pos["to"])

    logging.info("generating size of sim bulk purchase for each pos")
    sim_bulk_gen = ParetoGenerator(xmin=500, a=1.5, force_int=True,
                                   seed=circus.seeder.next())

    pos.create_attribute("SIM_BULK_BUY_SIZE", init_gen=sim_bulk_gen)

    return pos


def add_sim_bulk_purchase_action(circus):

    logging.info("creating POS SIM bulk purchase action")
    build_purchases = circus.create_action(
        name="pos_to_dealer_sim_bulk_purchases",
        initiating_actor=circus.pos,
        actorid_field="POS_ID"

        # no timer_gen nor activity_gen: this action is not triggered by the
        # clock
    )

    build_purchases.set_operations(
        circus.clock.ops.timestamp(named_as="TIME"),

        circus.pos.get_relationship("DEALER").ops.select_one(
            from_field="POS_ID",
            named_as="DEALER"),

        circus.pos.ops.lookup(
            actor_id_field="POS_ID",
            select={"SIM_BULK_BUY_SIZE": "SIM_BULK_BUY_SIZE"}),

        # selecting and removing Sims from dealers
        circus.dealers.get_relationship("SIMS").ops.select_many(
            from_field="DEALER",
            named_as="BOUGHT_SIM_BULK",
            quantity_field="SIM_BULK_BUY_SIZE",

            # if a SIM is selected, it is removed from the dealer's stock
            pop=True,

            # TODO: put this back to True and log the failed purchases
            discard_missing=True
        ),

        circus.pos.get_relationship("SIMS").ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="OLD_POS_STOCK"),

        # and adding them to the POS
        circus.pos.get_relationship("SIMS").ops.add_grouped(
            from_field="POS_ID",
            grouped_items_field="BOUGHT_SIM_BULK"),

        circus.pos.get_relationship("SIMS").ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="NEW_POS_STOCK"),

        # just logging the number of sims instead of the sims themselves...
        operations.Apply(source_fields="BOUGHT_SIM_BULK",
                         named_as="NUMBER_OF_SIMS",
                         f=lambda s: s.map(len), f_args="series"),

        operations.FieldLogger(log_id="pos_to_dealer_sim_bulk_purchases",
                               cols=["TIME",  "POS_ID", "DEALER",
                                     "OLD_POS_STOCK", "NEW_POS_STOCK",
                                     "NUMBER_OF_SIMS"]
                               ),
    )
