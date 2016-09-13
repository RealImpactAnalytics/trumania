
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import pandas as pd

from snd_customers import *
from snd_sites import *
from snd_pos import *

import logging

params = {
    "n_sites": 500,
    "n_customers": 5000,
    "n_pos": 1000,
}


class SND(Circus):

    def __init__(self):
        Circus.__init__(
            self,
            master_seed=12345,
            start=pd.Timestamp("13 Sept 2016 12:00"),
            step_s=300)

        self.sites = create_sites(self, params)
        self.customers = create_customers(self, params)
        add_mobility(self)
        self.pos = create_pos(self, params)


if __name__ == "__main__":

    setup_logging()
    circus = SND()

    logs = circus.run(n_iterations=10)

    for logid, log_df in logs.iteritems():
        logging.info(" - some {}:\n{}\n\n".format(
            logid,
            log_df.head(15).to_string()))

