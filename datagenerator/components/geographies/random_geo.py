from datagenerator.core.circus import Circus
from datagenerator.core.actor import Actor
from datagenerator.core.random_generators import *


class WithRandomGeo(Circus):
    """
    Circus mix-in that adds the creation of random cells
    """

    def create_random_cells(self, n_cells):
        """
        Creation of a basic actor for cells, with latitude and longitude
        """

        cells = Actor(prefix="CELL_", size=n_cells)

        latitude_generator = FakerGenerator(method="latitude", seed=self.seeder.next())
        longitude_generator = FakerGenerator(method="longitude", seed=self.seeder.next())

        cells.create_attribute("latitude", init_gen=latitude_generator)
        cells.create_attribute("longitude", init_gen=longitude_generator)

        return cells



