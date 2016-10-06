from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
from numpy.random import *
from datagenerator.components import db
import patterns


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
        # caveat: all at the same time for now
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
            f=np.add, f_args="series"),

        pos.get_attribute("ATTRACT_DELTA").ops.update(
            actor_id_field="POS_ID",
            copy_from_field="NEW_ATTRACT_DELTA"
        ),

        # TODO: remove this (currently there just for debugs)
        circus.clock.ops.timestamp(named_as="TIME"),
        FieldLogger(log_id="att_delta_updates")
    )


def build_ers_pos_stock_size_gen(circus, params):
    """
    return: the generator for intial pos ERS stock
    """

    gen_namespace, gen_id = params["pos_init_er_stock_distro"].split("/")

    return db.load_empirical_discrete_generator(
        namespace=gen_namespace,
        gen_id=gen_id,
        seed=circus.seeder.next())


def create_pos(circus, params, sim_id_gen, recharge_id_gen):

    logging.info("creating {} POS".format(params["n_pos"]))
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

    logging.info("generating POS initial ERS stock")
    ers_stock_gen = build_ers_pos_stock_size_gen(circus, params).flatmap(
        DependentBulkGenerator(element_generator=recharge_id_gen))
    pos.create_stock_relationship_grp(name="ERS",
                                      stock_bulk_gen=ers_stock_gen,
                                      seed=circus.seeder.next())

    logging.info("generating POS initial SIMS stock")
    sim_stock_gen = build_ers_pos_stock_size_gen(circus, params) \
        .map(f_vect=scale(factor=1/params["ers_to_sim_ratio"]))\
        .map(f=bound_value(lb=10))\
        .flatmap(DependentBulkGenerator(element_generator=sim_id_gen))
    pos.create_stock_relationship_grp(name="SIMS",
                                      stock_bulk_gen=sim_stock_gen,
                                      seed=circus.seeder.next())

    # Recording the stock level of every pos every day, for degugging
    build_purchases = circus.create_action(
        name="pos_stock_log",
        initiating_actor=pos,
        actorid_field="POS_ID",

        timer_gen=ConstantDependentGenerator(
            value=circus.clock.n_iterations(duration=pd.Timedelta("24h")) - 1
        ))

    build_purchases.set_operations(
        pos.get_relationship("SIMS").ops.get_neighbourhood_size(
                from_field="POS_ID",
                named_as="SIM_STOCK_LEVEL"),

        pos.get_relationship("ERS").ops.get_neighbourhood_size(
                from_field="POS_ID",
                named_as="ERS_STOCK_LEVEL"),

        circus.clock.ops.timestamp(named_as="TIME", random=False,
                                   log_format="%Y-%m-%d"),

        operations.FieldLogger(log_id="pos_stock_log")
    )

    return pos


def connect_pos_to_dealers(circus):

    logging.info("assigning a dealer to each POS")
    dealer_of_pos = make_random_assign(
        set1=circus.pos.ids,
        set2=circus.dealers_l2.ids,
        seed=circus.seeder.next())

    dealer_rel = circus.pos.create_relationship("DEALERS_L2",
                                                seed=circus.seeder.next())
    dealer_rel.add_relations(
        from_ids=dealer_of_pos["set1"],
        to_ids=dealer_of_pos["chosen_from_set2"])


def build_pos_sims_build_size_gen(circus, params):
    return NumpyRandomGenerator(
        method="choice",
        a=params["pos_sim_bulk_purchase_sizes"],
        p=params["pos_sim_bulk_purchase_sizes_dist"],
        seed=circus.seeder.next())


def add_sim_bulk_purchase_action(circus, params):

    logging.info("creating POS SIM bulk purchase action")
    bulk_size_gen = build_pos_sims_build_size_gen(circus, params)

    low_stock_dealer_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=1,
            x_max=params["max_pos_sim_stock_triggering_restock"] * 10, # TODO
            shape=params["pos_sim_restock_shape"],
            incrementing=False))

    _add_bulk_purchase_action(
        circus,
        action_name="pos_to_dealer_sim_bulk_purchases",
        bulk_size_gen=bulk_size_gen,
        dealers_relationship="SIMS",
        pos_relationship="SIMS",
        max_stock=params["pos_sim_max_stock"],
        upperlevel_bulk_purchase_trigger=low_stock_dealer_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="dealer_l2_buys_sims_from_dealer_l1"
    )


def build_pos_ers_build_size_gen(circus, params):
    return NumpyRandomGenerator(
        method="choice",
        a=params["pos_ers_bulk_purchase_sizes"],
        p=params["pos_ers_bulk_purchase_sizes_dist"],
        seed=circus.seeder.next())


def add_ers_bulk_purchase_action(circus, params):

    logging.info("creating POS ERS bulk purchase action")
    bulk_size_gen = build_pos_ers_build_size_gen(circus, params)

    low_stock_dealer_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=1,
            x_max=params["max_pos_er_stock_triggering_restock"] * 10, # TODO
            shape=params["pos_er_restock_shape"],
            incrementing=False))

    _add_bulk_purchase_action(
        circus,
        action_name="pos_to_dealer_ers_bulk_purchases",
        bulk_size_gen=bulk_size_gen,
        dealers_relationship="ERS",
        pos_relationship="ERS",
        max_stock=params["pos_ers_max_stock"],
        upperlevel_bulk_purchase_trigger=low_stock_dealer_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="dealer_l2_buys_ers_from_dealer_l1"
    )


def _bound_diff_to_max(max_stock):
    """
    Builds a function that adapts an array of deltas s.t. when added to the
    specified array of stock levels, the result does not exceed max_stock.
    """
    def _bound(action_data):
        stocks, deltas = action_data.iloc[:, 0], action_data.iloc[:, 1]

        ceiling = np.min(
            np.array([stocks + deltas, [max_stock] * stocks.shape[0]]),
            axis=0)

        return pd.DataFrame(ceiling - stocks)

    return _bound


def _add_bulk_purchase_action(circus, action_name, bulk_size_gen,
                              dealers_relationship, pos_relationship,
                              max_stock,
                              upperlevel_bulk_purchase_trigger=None,
                              upperlevel_bulk_purchase_action_name=None):
    """
    Generic utility method to create a bulk purchase action from pos to dealers
    """

    logging.info("creating POS bulk purchase action")
    build_purchases = circus.create_action(
        name=action_name,
        initiating_actor=circus.pos,
        actorid_field="POS_ID",

        # no timer_gen nor activity gens for the POS bulk actions: these are
        # triggered from low stock conditions after the purchases
    )

    if upperlevel_bulk_purchase_trigger is None:
        upper_level_bulk_purchases = operations.Operation()

    else:
        upper_level_bulk_purchases = patterns.trigger_action_if_low_stock(
            circus,
            stock_relationship=circus.dealers_l2.get_relationship(dealers_relationship),
            actor_id_field="DEALER_L2",
            restock_trigger=upperlevel_bulk_purchase_trigger,
            triggered_action_name=upperlevel_bulk_purchase_action_name
        )

    build_purchases.set_operations(
        circus.clock.ops.timestamp(named_as="TIME"),

        circus.pos.get_relationship("DEALERS_L2").ops.select_one(
            from_field="POS_ID",
            named_as="DEALER_L2"),

        bulk_size_gen.ops.generate(named_as="DESIRED_BULK_SIZE"),

        circus.pos.get_relationship(pos_relationship).ops \
            .get_neighbourhood_size(
                from_field="POS_ID",
                named_as="OLD_POS_STOCK"),

        operations.Apply(source_fields=["OLD_POS_STOCK", "DESIRED_BULK_SIZE"],
                         named_as="REQUESTED_BULK_SIZE",
                         f=_bound_diff_to_max(max_stock)),

        # selecting and removing Sims from dealers
        circus.dealers_l2.get_relationship(dealers_relationship).ops.select_many(
            from_field="DEALER_L2",
            named_as="ITEMS_BULK",
            quantity_field="REQUESTED_BULK_SIZE",

            # if an item is selected, it is removed from the dealer's stock
            pop=True,

            # TODO: put this back to False and log the failed purchases
            discard_missing=True),

        # and adding them to the POS
        circus.pos.get_relationship(pos_relationship).ops.add_grouped(
            from_field="POS_ID",
            grouped_items_field="ITEMS_BULK"),

        # We do not track the old and new stock of the dealer since the result
        # is misleading: since all purchases are performed in parallel,
        # if a dealer is selected several times, its stock level after the
        # select_many() is the level _after_ all purchases are done, which is
        # typically not what we want to include in the log.
        circus.pos.get_relationship(pos_relationship).ops\
            .get_neighbourhood_size(
                from_field="POS_ID",
                named_as="NEW_POS_STOCK"),

        # actual number of bought items might be different due to out of stock
        operations.Apply(source_fields="ITEMS_BULK",
                         named_as="BULK_SIZE",
                         f=lambda s: s.map(len), f_args="series"),

        operations.FieldLogger(log_id=action_name,
                               cols=["TIME",  "POS_ID", "DEALER_L2",
                                     "OLD_POS_STOCK", "NEW_POS_STOCK",
                                     "BULK_SIZE"]),

        upper_level_bulk_purchases
    )
