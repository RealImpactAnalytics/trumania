import logging
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import pandas as pd
from numpy.random import *


def create_sites(circus, params):
    logging.info(" adding sites")
    sites = Actor(ids_gen=SequencialGenerator(prefix="SITE_"),
                  size=params["n_sites"])
    return sites
