from ap_picker.datasets.moma.data_reader.relational_db_reader import RelationalDbReader


def test_relational_db_reader():

    reader = RelationalDbReader("mathe")
    schema = reader.get_schema()
    assert len(
        schema) > 0, "Expected non-empty schema from relational database reader"
