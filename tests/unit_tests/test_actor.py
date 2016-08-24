from datagenerator.actor import Actor


def test_resulting_size_should_be_as_expected():
    tested = Actor(size=100)
    assert tested.size == 100


def test_transforming_actor_to_dataframe_should_provide_all_data():
    size = 10
    tested = Actor(size=size)

    ages = [10, 20, 40, 10, 100, 98, 12, 39, 76, 23]
    tested.create_attribute("age", init_values=ages)

    city = ["a", "b", "b", "a", "d", "e", "r", "a", "z", "c"]
    tested.create_attribute("city", init_values=city)

    df = tested.to_dataframe()

    # order of the columns in the resulting dataframe is currently not
    # deterministic
    assert sorted(df.columns) == ["age", "city"]

    assert df["age"].values.tolist() == ages
    assert df["city"].values.tolist() == city
