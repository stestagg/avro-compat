from io import BytesIO
import avro_compat.fastavro as fastavro

import pytest


def roundtrip(schema, records, new_schema):
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    reader = fastavro.reader(new_file, new_schema)
    new_records = list(reader)
    return new_records


def test_aliases_not_present():
    schema = {
        "type": "record",
        "name": "test_aliases_not_present",
        "fields": [{"name": "test", "type": "double"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_aliases_not_present",
        "fields": [
            {"name": "newtest", "type": "double", "aliases": ["testX"]},
        ],
    }

    records = [{"test": 1.2}]

    with pytest.raises(fastavro.read.SchemaResolutionError):
        roundtrip(schema, records, new_schema)


def test_incompatible_aliases():
    schema = {
        "type": "record",
        "name": "test_incompatible_aliases",
        "fields": [{"name": "test", "type": "double"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_incompatible_aliases",
        "fields": [
            {"name": "newtest", "type": "int", "aliases": ["test"]},
        ],
    }

    records = [{"test": 1.2}]

    with pytest.raises(fastavro.read.SchemaResolutionError):
        roundtrip(schema, records, new_schema)


def test_aliases_in_reader_schema():
    schema = {
        "type": "record",
        "name": "test_aliases_in_reader_schema",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_aliases_in_reader_schema",
        "fields": [{"name": "newtest", "type": "int", "aliases": ["test"]}],
    }

    records = [{"test": 1}]

    assert roundtrip(schema, records, new_schema) == [{"newtest": 1}]


def test_aliases_with_default_value_and_field_added():
    """https://github.com/fastavro/fastavro/issues/225"""
    schema = {
        "type": "record",
        "name": "test_aliases_with_default_value",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_aliases_with_default_value",
        "fields": [
            {"name": "newtest", "type": "int", "default": 0, "aliases": ["test"]},
            {"name": "test2", "type": "int", "default": 100},
        ],
    }

    records = [{"test": 1}]

    new_records = roundtrip(schema, records, new_schema)
    assert new_records == [{"newtest": 1, "test2": 100}]


def test_record_name_alias():
    schema = {
        "type": "record",
        "name": "test_record_name_alias",
        "fields": [{"name": "test", "type": "int"}],
    }

    new_schema = {
        "type": "record",
        "name": "test_record_name_alias_new",
        "aliases": "test_record_name_alias",
        "fields": [{"name": "test", "type": "int"}],
    }

    records = [{"test": 1}]

    assert roundtrip(schema, records, new_schema) == [{"test": 1}]


def test_fixed_name_alias():
    schema = {"type": "fixed", "name": "test_fixed_name_alias", "size": 4}

    new_schema = {
        "type": "fixed",
        "name": "test_fixed_name_alias_new",
        "aliases": "test_fixed_name_alias",
        "size": 4,
    }

    records = [b"1234"]

    assert roundtrip(schema, records, new_schema) == [b"1234"]


def test_enum_name_alias():
    schema = {"type": "enum", "name": "test_enum_name_alias", "symbols": ["FOO"]}

    new_schema = {
        "type": "enum",
        "name": "test_enum_name_alias_new",
        "aliases": "test_enum_name_alias",
        "symbols": ["FOO"],
    }

    records = ["FOO"]

    assert roundtrip(schema, records, new_schema) == ["FOO"]
