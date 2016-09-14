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
        timer_gen=ConstantDependentGenerator(value=circus.clock.ticks_per_day)
        #timer_gen=ConstantDependentGenerator(value=1)
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

        circus.clock.ops.timestamp(named_as="TIME"),

        # TODO: remove this (just there for debugs)
        FieldLogger(log_id="att_updates")
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
        to_ids=sim_ids,
        from_ids=pos.ids,
        seed=circus.seeder.next())

    pos_sims.add_relations(
        from_ids=sims_dealer["from"],
        to_ids=sims_dealer["to"])

    return pos
