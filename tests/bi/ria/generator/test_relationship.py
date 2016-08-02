from bi.ria.generator.relationship import Relationship


# bug fix: this was simply crashing previously
def test_select_one_from_empty_relationship_should_return_void():
    tested = Relationship(name="tested", seed = 1)

    assert tested.select_one([]).shape[0] == 0


# bug fix: this was simply crashing previously
def test_select_one_from_non_existing_ids_should_return_void():
    tested = Relationship(name="tested", seed = 1)
    tested.add_relations(from_ids=["a", "b", "b", "c"],
                         to_ids=["b", "c", "a", "b"]
                         )
    assert tested.select_one(["non_existing_id", "neither"]).shape[0] == 0
