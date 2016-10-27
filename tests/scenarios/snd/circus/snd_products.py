from datagenerator.core import random_generators
import snd_constants


def create_products(circus, params):
    """
    :param circus:
    :param params:
    :return:
    """

    for product_name, description in params["products"].items():
        product = circus.create_actor(
            name=product_name,
            ids_gen=random_generators.SequencialGenerator(
                prefix=description["prefix"]),
            size=description["product_types_num"])

        product.create_attribute(
            name="product_description",
            init_gen=random_generators.FakerGenerator(
                method="text",
                seed=circus.seeder.next())
        )

    sims = circus.actors["sim"]

    sims.create_attribute(
        "type",
        init_gen=snd_constants.gen("SIM_TYPES", circus.seeder.next()))

    sims.create_attribute(
        "ean",
        init_gen=random_generators.FakerGenerator(
                method="ean", seed=circus.seeder.next()))

    handsets = circus.actors["handset"]

    handsets.create_attribute(
        "tac_id",
        init_gen=random_generators.FakerGenerator(
                method="ean", seed=circus.seeder.next()))

    handsets.create_attribute(
        "category",
        init_gen=snd_constants.gen("HANDSET_CATEGORY", circus.seeder.next()))

    handsets.create_attribute(
        "internet_technology",
        init_gen=snd_constants.gen("SIM_CAP", circus.seeder.next()))

    handsets.create_attribute(
        "brand",
        init_gen=snd_constants.gen("HANDSET_BRANDS", circus.seeder.next()))

    handsets.create_attribute(
        "ean",
        init_gen=random_generators.FakerGenerator(
            method="ean", seed=circus.seeder.next()))

