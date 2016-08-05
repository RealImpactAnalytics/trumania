from datagenerator.actor import Actor


def test_resulting_size_should_be_as_expected():
    tested = Actor(size=100)
    assert tested.size() == 100
