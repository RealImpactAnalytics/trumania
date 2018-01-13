"""
This Belgium geography adds real sites from a Belgian operator to your circus
The loaded Actor has a LATITUDE and a LONGITUDE attribute

See https://realimpactanalytics.atlassian.net/wiki/x/J4A6Bg to read more on how
the dataset was generated.
"""
from trumania.components import db
from trumania.core.circus import Circus
import logging


class WithBelgium(Circus):

    @staticmethod
    def add_belgium_geography(self):
        """
        Loads the sites definition from Belgium
        """
        logging.info(" adding Belgium Geography")

        return db.load_population(namespace="belgium", population_id="sites")
