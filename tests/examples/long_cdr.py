from test_cdr import run_cdr_scenario
from datagenerator.util_functions import *

z# better run this outside of PyCharm for consistent measures...
#
# python tests/examples/long_cdr.py
# (or within docker... TODO...)
if __name__ == "__main__":
    setup_logging()
    logging.info("starting a long CDR test ")
    params = {
        "time_step": 60,
        "n_cells": 200,
        "n_agents": 500,
        "n_customers": 25000,
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
       
    """


