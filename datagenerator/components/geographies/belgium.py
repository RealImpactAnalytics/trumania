"""
This is just an illustration of how to persist various scenario components
"""
from datagenerator.components import db
from datagenerator.core.circus import Circus
import logging

"""
This Belgium geography adds real sites from a Belgian operator to your circus
The loaded Actor has a LATITUDE and a LONGITUDE attribute

See https://realimpactanalytics.atlassian.net/wiki/x/J4A6Bg to read more on how
the dataset was generated.
"""
class WithBelgium(Circus):

    def add_belgium_geography(self):
        """
        Loads the sites definition from Belgium
        """
        logging.info(" adding Belgium Geography")

        return db.load_actor(namespace="belgium", actor_id="sites")


