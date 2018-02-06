"""
This is just an illustration of how to persist various scenario components
"""
import logging
import pandas as pd

from trumania.core import operations
from trumania.components import db
from trumania.core.circus import Circus
from trumania.core.util_functions import make_random_assign, setup_logging
from trumania.core.random_generators import NumpyRandomGenerator, ParetoGenerator, seed_provider, SequencialGenerator
from trumania.core.random_generators import FakerGenerator
from trumania.core.clock import CyclicTimerGenerator, CyclicTimerProfile


def build_unhealthy_level_gen(seed):
    return NumpyRandomGenerator(method="beta", a=1, b=999, seed=seed)


def build_healthy_level_gen(seed):
    return NumpyRandomGenerator(method="beta", a=1, b=999, seed=seed)


class WithUganda(Circus):

    def add_uganda_geography(self, force_build=False):
        """
        Loads the cells definition from Uganda + adds 2 stories to control
        """
        logging.info(" adding Uganda Geography")
        seeder = seed_provider(12345)

        if force_build:
            uganda_cells, uganda_cities, timer_config = build_uganda_populations(
                self)

        else:
            uganda_cells = db.load_population(namespace="uganda", population_id="cells")
            uganda_cities = db.load_population(namespace="uganda", population_id="cities")
            timer_config = db.load_timer_gen_config("uganda",
                                                    "cell_repair_timer_profile")

        repair_n_fix_timer = CyclicTimerGenerator(
            clock=self.clock,
            seed=next(self.seeder),
            config=timer_config)

        unhealthy_level_gen = build_unhealthy_level_gen(next(seeder))
        healthy_level_gen = build_healthy_level_gen(next(seeder))

        # tendency is inversed in case of broken cell: it's probability of
        # accepting a call is much lower

        # same profiler for breakdown and repair: they are both related to
        # typical human activity

        logging.info(" adding Uganda Geography6")
        cell_break_down_story = self.create_story(
            name="cell_break_down",

            initiating_population=uganda_cells,
            member_id_field="CELL_ID",

            timer_gen=repair_n_fix_timer,

            # fault activity is very low: most cell tend never to break down (
            # hopefully...)
            activity_gen=ParetoGenerator(xmin=5, a=1.4, seed=next(self.seeder))
        )

        cell_repair_story = self.create_story(
            name="cell_repair_down",

            initiating_population=uganda_cells,
            member_id_field="CELL_ID",

            timer_gen=repair_n_fix_timer,

            # repair activity is much higher
            activity_gen=ParetoGenerator(xmin=100, a=1.2,
                                         seed=next(self.seeder)),

            # repair is not re-scheduled at the end of a repair, but only triggered
            # from a "break-down" story
            auto_reset_timer=False
        )

        cell_break_down_story.set_operations(
            unhealthy_level_gen.ops.generate(named_as="NEW_HEALTH_LEVEL"),

            uganda_cells.get_attribute("HEALTH").ops.update(
                member_id_field="CELL_ID",
                copy_from_field="NEW_HEALTH_LEVEL"),

            cell_repair_story.ops.reset_timers(member_id_field="CELL_ID"),
            self.clock.ops.timestamp(named_as="TIME"),

            operations.FieldLogger(log_id="cell_status",
                                   cols=["TIME", "CELL_ID",
                                         "NEW_HEALTH_LEVEL"]),
        )

        cell_repair_story.set_operations(
            healthy_level_gen.ops.generate(named_as="NEW_HEALTH_LEVEL"),

            uganda_cells.get_attribute("HEALTH").ops.update(
                member_id_field="CELL_ID",
                copy_from_field="NEW_HEALTH_LEVEL"),

            self.clock.ops.timestamp(named_as="TIME"),

            # note that both stories are contributing to the same
            # "cell_status" log
            operations.FieldLogger(log_id="cell_status",
                                   cols=["TIME", "CELL_ID",
                                         "NEW_HEALTH_LEVEL"]),
        )

        return uganda_cells, uganda_cities


def build_uganda_populations(circus):

    seeder = seed_provider(12345)

    cells = circus.create_population(name="cells",
                                     ids_gen=SequencialGenerator(prefix="CELL_"),
                                     size=200)
    latitude_generator = FakerGenerator(method="latitude",
                                        seed=next(seeder))
    cells.create_attribute("latitude", init_gen=latitude_generator)

    longitude_generator = FakerGenerator(method="longitude",
                                         seed=next(seeder))
    cells.create_attribute("longitude", init_gen=longitude_generator)

    # the cell "health" is its probability of accepting a call. By default
    # let's says it's one expected failure every 1000 calls
    healthy_level_gen = build_healthy_level_gen(next(seeder))

    cells.create_attribute(name="HEALTH", init_gen=healthy_level_gen)

    city_gen = FakerGenerator(method="city", seed=next(seeder))
    cities_values = pd.unique(city_gen.generate(500))[:200]
    cities = circus.create_population(name="cities", ids=cities_values)

    cell_city_rel = cities.create_relationship("CELLS")

    cell_city_df = make_random_assign(cells.ids, cities.ids, next(seeder))
    cell_city_rel.add_relations(
        from_ids=cell_city_df["chosen_from_set2"],
        to_ids=cell_city_df["set1"])

    pop_gen = ParetoGenerator(xmin=10000, a=1.4, seed=next(seeder))
    cities.create_attribute("population", init_gen=pop_gen)

    timer_config = CyclicTimerProfile(
        profile=[1, .5, .2, .15, .2, .4, 3.8,
                 7.2, 8.4, 9.1, 9.0, 8.3, 8.1,
                 7.7, 7.4, 7.8, 8.0, 7.9, 9.7,
                 10.4, 10.5, 8.8, 5.7, 2.8],
        profile_time_steps="1h",
        start_date=pd.Timestamp("6 June 2016 00:00:00"))

    return cells, cities, timer_config


if __name__ == "__main__":
    # This is meant to be executed only once, to create the data on disk.

    # Note: using generators and persisting the result could make sense
    # if such generation is costly or for facilitating reproduceability,
    # though a more common use cas might be to build such Populations  and
    # relationship from empirical exploration of a dataset.

    # Note2: only the "static" properties of an environment are saved here,
    # whereas the "dynamic parts" (e.g. stories) are stored "in code", i.e.
    # in the withXYZ() class above that then need to be mixed in a Circus.

    setup_logging()

    cells, cities, timer_config = build_uganda_populations()

    db.remove_namespace("uganda")
    db.save_population(population=cells, namespace="uganda", population_id="cells")
    db.save_population(population=cities, namespace="uganda", population_id="cities")

    db.save_timer_gen(timer_gen=timer_config, namespace="uganda",
                      timer_gen_id="cell_repair_timer_profile")
