"""
This is just an illustration of how to define a persistent model component
"""
from datagenerator.core.actor import *
from datagenerator.core import operations
from datagenerator.core.random_generators import *
from datagenerator.components import db
from datagenerator.core.circus import Circus
from datagenerator.components.time_patterns.profilers import *


def build_unhealthy_level_gen(seed):
    return NumpyRandomGenerator(method="beta", a=1, b=999, seed=seed)


def build_healthy_level_gen(seed):
    return NumpyRandomGenerator(method="beta", a=1, b=999, seed=seed)


class WithUganda(Circus):

    def add_uganda_geography(self):
        """
        Loads the cells definition from Uganda + adds 2 actions to control
        """
        seeder = seed_provider(12345)
        uganda_cells = db.load_actor(namespace="uganda", actor_id="cells")

        unhealthy_level_gen = build_unhealthy_level_gen(seeder.next())
        healthy_level_gen = build_healthy_level_gen(seeder.next())

        # tendency is inversed in case of broken cell: it's probability of
        # accepting a call is much lower

        # same profiler for breakdown and repair: they are both related to
        # typical human activity
        default_day_profiler = DayProfiler(self.clock)

        cell_break_down_action = self.create_action(
            name="cell_break_down",

            initiating_actor=uganda_cells,
            actorid_field="CELL_ID",

            timer_gen=default_day_profiler,

            # fault activity is very low: most cell tend never to break down (
            # hopefully...)
            activity_gen=ParetoGenerator(xmin=5, a=1.4, seed=self.seeder.next())
        )

        cell_repair_action = self.create_action(
            name="cell_repair_down",

            initiating_actor=uganda_cells,
            actorid_field="CELL_ID",

            timer_gen=default_day_profiler,

            # repair activity is much higher
            activity_gen=ParetoGenerator(xmin=100, a=1.2,
                                         seed=self.seeder.next()),

            # repair is not re-scheduled at the end of a repair, but only triggered
            # from a "break-down" action
            auto_reset_timer=False
        )

        cell_break_down_action.set_operations(
            unhealthy_level_gen.ops.generate(named_as="NEW_HEALTH_LEVEL"),

            uganda_cells.get_attribute("HEALTH").ops.update(
                actor_id_field="CELL_ID",
                copy_from_field="NEW_HEALTH_LEVEL"),

            cell_repair_action.ops.reset_timers(actor_id_field="CELL_ID"),
            self.clock.ops.timestamp(named_as="TIME"),

            operations.FieldLogger(log_id="cell_status",
                                   cols=["TIME", "CELL_ID",
                                         "NEW_HEALTH_LEVEL"]),
        )

        cell_repair_action.set_operations(
            healthy_level_gen.ops.generate(named_as="NEW_HEALTH_LEVEL"),

            uganda_cells.get_attribute("HEALTH").ops.update(
                actor_id_field="CELL_ID",
                copy_from_field="NEW_HEALTH_LEVEL"),

            self.clock.ops.timestamp(named_as="TIME"),

            # note that both actions are contributing to the same "cell_status" log
            operations.FieldLogger(log_id="cell_status",
                                   cols=["TIME", "CELL_ID",
                                         "NEW_HEALTH_LEVEL"]),
        )

        return uganda_cells


if __name__ == "__main__":
    # This is meant to be executed only once, to create the data on disk.

    # Note: using generators and persisting the result could make sense
    # if such generation is costly or for facilitating reproduceability,
    # though a more common use cas might be to build such Actors and
    # relationship from empirical exploration of a dataset.

    # Note2: only the "static" properties of an environment are saved here,
    # whereas the "dynamic parts" (e.g. actions) are stored "in code", i.e.
    # in the withXYZ() class above that then need to be mixed in a Circus.

    seeder = seed_provider(12345)

    cells = Actor(prefix="CELL_", size=200)
    latitude_generator = FakerGenerator(method="latitude",
                                        seed=seeder.next())
    cells.create_attribute("latitude", init_gen=latitude_generator)

    longitude_generator = FakerGenerator(method="longitude",
                                         seed=seeder.next())
    cells.create_attribute("longitude", init_gen=longitude_generator)

    # the cell "health" is its probability of accepting a call. By default
    # let's says it's one expected failure every 1000 calls
    healthy_level_gen = build_healthy_level_gen(seeder.next())

    cells.create_attribute(name="HEALTH", init_gen=healthy_level_gen)

    # city_gen = FakerGenerator(method="city", seed=seeder.next())
    # cities = Actor(prefix="CITY_", size=200)
    #
    #
    db.remove_namespace("uganda")
    db.save_actor(actor=cells, namespace="uganda", actor_id="cells")




