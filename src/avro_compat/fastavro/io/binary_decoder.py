import cavro
from functools import partial


SCHEMAS = {
    "null": cavro.Schema("null", parse_json=False),
    "boolean": cavro.Schema("boolean", parse_json=False),
    "int": cavro.Schema("int", parse_json=False),
    "long": cavro.Schema("long", parse_json=False),
}


class BinaryDecoder:
    def __init__(self, fo):
        self._reader = cavro.FileReader(fo)


def decode_sch(schema, reader):
    return schema.binary_read(reader)


for name, schema in SCHEMAS.items():
    meth = partial(decode_sch, schema)
    meth.__name__ = f"decode_{name}"
    setattr(BinaryDecoder, meth.__name__, meth)
