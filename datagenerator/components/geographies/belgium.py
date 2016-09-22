"""
This is just an illustration of how to persist various scenario components
"""
from datagenerator.components import db
from datagenerator.core.circus import Circus
import logging


class WithBelgium(Circus):

    def add_belgium_geography(self):
        """
        Loads the sites definition from Belgium
        """
        logging.info(" adding Belgium Geography")

        return db.load_actor(namespace="belgium", actor_id="sites")


