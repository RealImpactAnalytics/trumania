from trumania.core import random_generators
import snd_constants


def create_products(circus, params):
    """
    :param circus:
    :param params:
    :return:
    """

    for product_name, description in params["products"].items():
        product = circus.create_population(
            name=product_name,
            ids_gen=random_generators.SequencialGenerator(
                prefix=description["prefix"]),
            size=description["product_types_num"])

        product.create_attribute(
            name="product_description",
            init_gen=random_generators.FakerGenerator(
                method="text",
                seed=next(circus.seeder))
        )

    sims = circus.actors["sim"]

    sims.create_attribute(
        "type",
        init_gen=snd_constants.gen("SIM_TYPES", next(circus.seeder)))

    sims.create_attribute(
        "ean",
        init_gen=random_generators.FakerGenerator(
                method="ean", seed=next(circus.seeder)))

    handsets = circus.actors["handset"]

    handsets.create_attribute(
        "tac_id",
        init_gen=random_generators.FakerGenerator(
                method="ean", seed=next(circus.seeder)))

    handsets.create_attribute(
        "category",
        init_gen=snd_constants.gen("HANDSET_CATEGORY", next(circus.seeder)))

    handsets.create_attribute(
        "internet_technology",
        init_gen=snd_constants.gen("SIM_CAP", next(circus.seeder)))

    handsets.create_attribute(
        "brand",
        init_gen=snd_constants.gen("HANDSET_BRANDS", next(circus.seeder)))

    handsets.create_attribute(
        "ean",
        init_gen=random_generators.FakerGenerator(
            method="ean", seed=next(circus.seeder)))
