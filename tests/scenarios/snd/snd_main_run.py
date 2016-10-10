from datagenerator.core import circus
from datagenerator.core import util_functions


if __name__ == "__main__":

    util_functions.setup_logging()

    snd = circus.Circus.load_from_db(circus_name="snd_v1")

    log_output_folder = "snd_output_logs/scenario_0"
