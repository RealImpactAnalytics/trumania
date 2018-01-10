from trumania.core.circus import Circus
from trumania.core.actor import Actor
from trumania.core.random_generators import FakerGenerator


class WithRandomGeo(Circus):
    """
    Circus mix-in that adds the creation of random cells
    """

    def create_random_cells(self, n_cells):
        """
        Creation of a basic actor for cells, with latitude and longitude
        """

        cells = Actor(size=n_cells)

        latitude_generator = FakerGenerator(method="latitude", seed=next(self.seeder))
        longitude_generator = FakerGenerator(method="longitude", seed=next(self.seeder))

        cells.create_attribute("latitude", init_gen=latitude_generator)
        cells.create_attribute("longitude", init_gen=longitude_generator)

        return cells
