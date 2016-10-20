import path
from datagenerator.components.time_patterns.profilers import *

from datagenerator.core.actor import Actor
import datagenerator.core.operations as operations
import datagenerator.core.util_functions as util_functions
from datagenerator.core.circus import Circus

util_functions.setup_logging()


def run_test_scenario_1(clock_step, simulation_duration,
                        n_actions, per,
                        log_folder):


    circus = Circus(name="tested_circus", master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_duration=pd.Timedelta(clock_step))

    actor = circus.create_actor(name="a",
        size=1000,
        ids_gen=SequencialGenerator(max_length=3, prefix="id_"))

    daily_profile = CyclicTimerGenerator(
        clock=circus.clock,
        config=CyclicTimerProfile(
            profile=[1] * 24,
            profile_time_steps="1h",
            start_date=pd.Timestamp("8 June 2016")
        ),
        seed=1234)

    # each of the 500 actors have a constant 12 logs per day rate
    activity_gen = ConstantGenerator(
        value=daily_profile.activity(
            n_actions=n_actions, per=per
        ))

    # just a dummy operation to produce some logs
    action = circus.create_action(
        name="test_action",
        initiating_actor=actor,
        actorid_field="some_id",
        timer_gen=daily_profile,
        activity_gen=activity_gen)

    action.set_operations(
        circus.clock.ops.timestamp(named_as="TIME"),
        operations.FieldLogger(log_id="the_logs")
    )

    circus.run(duration=pd.Timedelta(simulation_duration), log_output_folder=log_folder)


def test_1000_actors_with_activity_12perday_should_yield_24k_logs_in_2days():
    """
    this is a "high frequency test", where the number of actions per cycle (
    i.e. per day here) is largely below 1 => the cyclic generator should
    typically generate timers smaller than the length of the cycle
    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        run_test_scenario_1(clock_step="15 min",
                            simulation_duration="2 days",
                            n_actions=12,
                            per=pd.Timedelta("1day"),
                            log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        # 2 days of simulation should produce 1000 * 12 * 2 == 24k logs
        logging.info("number of produced logs: {} logs".format(logs.shape[0]))
        assert 22e3 <= logs.shape[0] <= 26e3


def test_1000_actors_with_activity_12perday_should_yield_60k_logs_in_5days():
    """
    same test as above, with bigger clock step => typically more "rounding
    errors", and longer total simulation duration
    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        # note that we cannot have clock_step > 2h since that
        run_test_scenario_1(clock_step="1h",
                            simulation_duration="5 days",
                            n_actions=12,
                            per=pd.Timedelta("1day"),
                            log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        logging.info("number of produced logs: {} logs".format(logs.shape[0]))

        # 5 days of simulation should produce 1000 * 12 * 5 == 60k logs
        assert 55e3 <= logs.shape[0] <= 65e3


def test_1000_actors_with_low_activity():
    """

    This is a low activity test, where the actors have less than one activity
    per cycle

    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        run_test_scenario_1(clock_step="1 h",
                            simulation_duration="20days",
                            n_actions=1,
                            per=pd.Timedelta("5 days"),
                            log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        logging.info("number of produced logs: {} logs".format(logs.shape[0]))

        # 20 days of simulation should produce 1000 * .2 * 20 == 4000 logs
        assert 3500 <= logs.shape[0] <= 4500


def test_1000_actors_with_low_activity2():
    """

    This is a low activity test, where the actors have less than one activity
    per cycle

    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        run_test_scenario_1(clock_step="3 h",
                            simulation_duration="15days",
                            n_actions=1,
                            per=pd.Timedelta("5 days"),
                            log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        # 2 days of simulation should produce 1000 * 15 * 1/5 == 3000 logs
        assert 2600 <= logs.shape[0] <= 3400


def test_1000_actors_with_activity_one_per_cycle():
    """
    This is a border case between low and high activity, where the desired
    amount of logs per cycle is close to 1 (i.e. close to 1 per day with our
    timer) => we still need to have generated timers a bit above or below one
    day, and achieve the expected total amount of logs
    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        run_test_scenario_1(clock_step="15 min",
                            simulation_duration="10days",
                            n_actions=1,
                            per=pd.Timedelta("1 day"),
                            log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        logging.info("number of produced logs: {} logs".format(logs.shape[0]))

        # 10 days of simulation should produce 1000 * 1 * 10 == 10000 logs
        assert 9500 <= logs.shape[0] <= 10500


def test_actors_during_default_daily():

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        circus = Circus(name="tested_circus",
                        master_seed=1,
                        start=pd.Timestamp("8 June 2016"),
                        step_duration=pd.Timedelta("1h"))

        field_agents = circus.create_actor(name="fa",
            size=100,
            ids_gen=SequencialGenerator(max_length=3, prefix="id_"))

        mobility_time_gen = DefaultDailyTimerGenerator(
            clock=circus.clock, seed=circus.seeder.next())

        gaussian_activity = NumpyRandomGenerator(
            method="normal", loc=5,
            scale=.5, seed=1)
        mobility_activity_gen = gaussian_activity.map(bound_value(lb=1))

        # just a dummy operation to produce some logs
        action = circus.create_action(
            name="test_action",
            initiating_actor=field_agents,
            actorid_field="some_id",
            timer_gen=mobility_time_gen,
            activity_gen=mobility_activity_gen)

        action.set_operations(
            circus.clock.ops.timestamp(named_as="TIME"),
            operations.FieldLogger(log_id="the_logs")
        )

        circus.run(duration=pd.Timedelta("30 days"), log_output_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        logging.info("number of produced logs: {} logs".format(logs.shape[0]))

        # 30 days of simulation should produce 100 * 5 * 30 == 15k logs
        assert 14e3 <= logs.shape[0] <= 16.5e3


def test_actors_during_working_hours():

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        circus = Circus(name="tested_circus",
                        master_seed=1,
                        start=pd.Timestamp("8 June 2016"),
                        step_duration=pd.Timedelta("1h"))

        field_agents = circus.create_actor(name="fa",
            size=100,
            ids_gen=SequencialGenerator(max_length=3, prefix="id_"))

        mobility_time_gen = WorkHoursTimerGenerator(
            clock=circus.clock, seed=circus.seeder.next())

        five_per_day = mobility_time_gen.activity(
            n_actions=5, per=pd.Timedelta("1day"))

        std_per_day = mobility_time_gen.activity(
            n_actions=.5, per=pd.Timedelta("1day"))

        gaussian_activity = NumpyRandomGenerator(
            method="normal", loc=five_per_day,
            scale=std_per_day, seed=1)
        mobility_activity_gen = gaussian_activity.map(bound_value(lb=1))

        # just a dummy operation to produce some logs
        action = circus.create_action(
            name="test_action",
            initiating_actor=field_agents,
            actorid_field="some_id",
            timer_gen=mobility_time_gen,
            activity_gen=mobility_activity_gen)

        action.set_operations(
            circus.clock.ops.timestamp(named_as="TIME"),
            operations.FieldLogger(log_id="the_logs")
        )

        circus.run(duration=pd.Timedelta("30 days"), log_output_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        logging.info("number of produced logs: {} logs".format(logs.shape[0]))

        # 30 days of simulation should produce 100 * 5 * 30 == 15k logs
        assert 14e3 <= logs.shape[0] <= 16e3
