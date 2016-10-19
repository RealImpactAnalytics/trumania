from __future__ import division
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
from datagenerator.components import db
import patterns


import snd_constants


def _attractiveness_sigmoid():
    """
    :return: return the sigmoid used to transform attractiveness_base levels
     into attractiveness in [0, 1], which are used to influence the choice
     of customers
    """
    return operations.logistic(k=.15)


def _create_attractiveness_attributes(circus, pos):

    logging.info("generating pos attractiveness values and evolutions")

    # "base" attractiveness, in [-50, 50]
    attractiveness_base_gen = NumpyRandomGenerator(
        method="choice",
        a=range(-50, 50),
        seed=circus.seeder.next())
    pos.create_attribute("ATTRACT_BASE", init_gen=attractiveness_base_gen)

    ac = _attractiveness_sigmoid()(pos.get_attribute_values("ATTRACT_BASE"))
    pos.create_attribute("ATTRACTIVENESS", init_values=ac)

    # evolution steps of the base attractiveness
    attractiveness_delta_gen = NumpyRandomGenerator(
        method="choice",
        a=[-2, -1, 0, 1, 2],
        p=[.1, .25, .3, .25, .1],
        seed=circus.seeder.next())

    pos.create_attribute("ATTRACT_DELTA", init_gen=attractiveness_delta_gen)


def add_attractiveness_evolution_action(circus):

    pos = circus.actors["pos"]

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
            f=_attractiveness_sigmoid(), f_args="series"
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


def add_pos(circus, params):

    logging.info("creating {} POS".format(params["n_pos"]))
    pos = circus.create_actor(
        name="pos", size=params["n_pos"],
        ids_gen=SequencialGenerator(prefix="POS_"))

    _create_attractiveness_attributes(circus, pos)

    logging.info("assigning a site to each POS")
    site_gen = NumpyRandomGenerator(method="choice",
                                    seed=circus.seeder.next(),
                                    a=circus.actors["sites"].ids)
    pos.create_attribute("SITE", init_gen=site_gen)

    # TODO: Add POS coordinates based on SITE coordinates
    pos.create_attribute("LATITUDE", init_gen=ConstantGenerator(0.0))
    pos.create_attribute("LONGITUDE", init_gen=ConstantGenerator(0.0))

    name_gen = NumpyRandomGenerator(method="choice",
                                    seed=circus.seeder.next(),
                                    a=snd_constants.POS_NAMES)

    pos.create_attribute("NAME", init_gen=name_gen)

    logging.info("recording the list POS per site in site relationship")
    pos_rel = circus.actors["sites"].create_relationship(
        "POS", seed=circus.seeder.next())
    pos_rel.add_relations(
        from_ids=pos.get_attribute_values("SITE"),
        to_ids=pos.ids)

    for product, description in params["products"].items():
        _init_pos_product(circus, product, description)


def _init_pos_product(circus, product, description):
    """
    Initialize the required stock and generators for this
    """

    logging.info("Building a generator of {} POS bulk purchase size".format(product))
    bulk_size_gen = NumpyRandomGenerator(
        method="choice",
        a=description["pos_bulk_purchase_sizes"],
        p=description["pos_bulk_purchase_sizes_dist"],
        seed=circus.seeder.next())
    circus.attach_generator("pos_{}_bulk_size_gen".format(product), bulk_size_gen)

    logging.info("Building a generators of {} POS initial stock size".format(product))
    if "pos_init_distro" in description:
        logging.info(" using pre-defined initial distribution")
        gen_namespace, gen_id = description["pos_init_distro"].split("/")

        # TODO: with the new save/load, this is now a mere numpyGenerator
        init_stock_size_gen = db.load_empirical_discrete_generator(
            namespace=gen_namespace,
            gen_id=gen_id,
            seed=circus.seeder.next())
    else:
        logging.info(" using bulk size distribution")
        init_stock_size_gen = bulk_size_gen
    circus.attach_generator("pos_{}_init_stock_size_gen".format(product), init_stock_size_gen)

    logging.info("Building a generator of {} ids".format(product))
    product_id_gen = SequencialGenerator(prefix="{}_".format(product))
    circus.attach_generator("{}_id_gen".format(product), product_id_gen)

    logging.info("Initializing POS {} stock".format(product))
    stock_gen = init_stock_size_gen.flatmap(DependentBulkGenerator(element_generator=product_id_gen))
    circus.actors["pos"].create_stock_relationship_grp(
        name=product,
        stock_bulk_gen=stock_gen,
        seed=circus.seeder.next())


def add_pos_stock_log_action(circus):
    """
    Adds am action recording the stock level of every pos every day,
    for debugging
    """

    pos = circus.actors["pos"]

    stock_levels_logs = circus.create_action(
        name="pos_stock_log",
        initiating_actor=pos,
        actorid_field="POS_ID",

        timer_gen=ConstantDependentGenerator(
            value=circus.clock.n_iterations(duration=pd.Timedelta("24h")) - 1
        ))

    stock_levels_logs.set_operations(
        circus.clock.ops.timestamp(named_as="TIME", random=False,
                                   log_format="%Y-%m-%d"),

        pos.get_relationship("SIM").ops.get_neighbourhood_size(
                from_field="POS_ID",
                named_as="SIM_STOCK_LEVEL"),

        pos.get_relationship("ER").ops.get_neighbourhood_size(
                from_field="POS_ID",
                named_as="ERS_STOCK_LEVEL"),

        operations.FieldLogger(log_id="pos_stock_log")
    )

    return pos


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


