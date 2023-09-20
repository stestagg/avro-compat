import cavro

PRIMITIVES = tuple(cavro.PRIMITIVE_TYPES.keys())


UnknownType = cavro.UnknownType


class SchemaParseException(Exception):
    pass
