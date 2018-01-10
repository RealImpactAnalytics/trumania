import logging

from test_cdr import run_cdr_scenario
from trumania.core.util_functions import setup_logging

# better run this outside of PyCharm for consistent measures...
#
# python tests/scenarios/long_cdr.py

if __name__ == "__main__":
    setup_logging()
    logging.info("starting a long CDR test ")
    params = {
        "time_step": 60,
        "n_cells": 200,
        "n_agents": 500,
        "n_subscribers": 25000,
        "average_degree": 20,
        "n_iterations": 200
    }

    run_cdr_scenario(params)

    """
    result on Svends's laptop:

     total number of logs: 392086
     execution times: "
     - building the circus: 0 days 00:01:32.156013
     - running the simulation: 0 days 00:14:07.355875

     Note: that world is a bit irrealistic:
      * 200 clock steps is about 3 hours
      * 400k logs for 25k users is 16 actions per persons

       => a bit more than 5 actions per user per hour


New result (25 Aug) after adding multi-sim

       total number of logs: 380270
2016-08-25 15:11:47,849  71736 topups logs
2016-08-25 15:11:47,849  86 cell_status logs
2016-08-25 15:11:47,849  154358 voice_cdr logs
2016-08-25 15:11:47,850  152642 sms_cdr logs
2016-08-25 15:11:47,850  1448 mobility_logs logs
2016-08-25 15:11:47,852
execution times: "
     - building the circus: 0 days 00:02:43.163336
     - running the simulation: 0 days 00:30:58.923819
    """
