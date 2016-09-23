import path
from datagenerator.components.time_patterns.profilers import *

from datagenerator.core.actor import Actor
import datagenerator.core.operations as operations
import datagenerator.core.util_functions as util_functions
from datagenerator.core.circus import Circus

util_functions.setup_logging()


def _run_2_days_scenario(clock_step, simulation_duration,
                         n_actions, per,
                         log_folder):

    actor = Actor(
        size=1000,
        ids_gen=SequencialGenerator(max_length=3, prefix="id_"))

    circus = Circus(master_seed=1,
                    output_folder=log_folder,
                    start=pd.Timestamp("8 June 2016"),
                    step_duration=pd.Timedelta(clock_step))

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

    circus.run(duration=pd.Timedelta(simulation_duration))


def test_1000_actors_with_activity_12perday_should_yield_24k_logs_in_2days():
    """
    this is a "high frequency test", where the number of actions per cycle (
    i.e. per day here) is largely below 1 => the cyclic generator should
    typically generate timers smaller than the length of the cycle
    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        _run_2_days_scenario(clock_step="15 min",
                             simulation_duration="2 days",
                             n_actions=12,
                             per=pd.Timedelta("1day"),
                             log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        # 2 days of simulation should produce 1000 * 12 * 2 == 24k logs
        logging.info("test 1, clock step 15min: {} logs".format(logs.shape[0]))
        assert 22e3 <= logs.shape[0] <= 26e3


def test_1000_actors_with_activity_12perday_should_yield_24k_logs_in_2days_bis():
    """
    same test as above, with bigger clock step => typically more "rounding
    errors", and longer total simulation duration
    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        # note that we cannot have clock_step > 2h since that
        _run_2_days_scenario(clock_step="1h",
                             simulation_duration="5 days",
                             n_actions=12,
                             per=pd.Timedelta("1day"),
                             log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        logging.info("test 2, clock step 1h: {} logs".format(logs.shape[0]))

        # 2 days of simulation should produce 1000 * 12 * 5 == 60k logs
        assert 55e3 <= logs.shape[0] <= 65e3


def test_1000_actors_with_low_activity():
    """

    This is a low activity test, where the actors have less than one activity
    per cycle

    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        _run_2_days_scenario(clock_step="1 h",
                             simulation_duration="20days",
                             n_actions=1,
                             per=pd.Timedelta("5 days"),
                             log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        logging.info("test 3, low activity, clock step 1h: {} logs".format(
            logs.shape[0]))

        # 2 days of simulation should produce 1000 * .2 * 20 == 4000 logs
        assert 3500 <= logs.shape[0] <= 4500


def test_1000_actors_with_activity_one_per_cycle():
    """
    This is a border case between low and high activity, where the desired
    amount of logs per cycle is close to 1 (i.e. close to 1 per day with our
    timer) => we still need to have generated timers a bit above or below one
    day, and achieve the expected total amount of logs
    """

    with path.tempdir() as log_parent_folder:
        log_folder = os.path.join(log_parent_folder, "logs")

        _run_2_days_scenario(clock_step="15 min",
                             simulation_duration="10days",
                             n_actions=1,
                             per=pd.Timedelta("1 day"),
                             log_folder=log_folder)

        logging.info("loading produced logs")
        logs = util_functions.load_all_logs(log_folder)["the_logs"]

        logging.info("test 3, low activity, clock step 1h: {} logs".format(
            logs.shape[0]))

        # 2 days of simulation should produce 1000 * 1 * 10 == 10000 logs
        assert 9500 <= logs.shape[0] <= 10500
