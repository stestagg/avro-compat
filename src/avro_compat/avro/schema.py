from typing import Any
import cavro
import json

import avro_compat.avro.constants



class AvroTypeMeta(type):
    def __instancecheck__(cls, inst) -> bool:
        if not isinstance(inst, (cavro.Schema, cavro.LogicalType)):
            return False
        if not isinstance(inst.type, cls.TYPE):
            return False
        if logical_type := getattr(cls, 'LOGICAL_TYPE', None):
            if not isinstance(inst.type.logical_type, logical_type):
                return False
        return True


class DecimalLogicalSchema(metaclass=AvroTypeMeta):
    TYPE = (cavro.BytesType, cavro.FixedType)
    LOGICAL_TYPE = cavro.DecimalType


class ArraySchema(metaclass=AvroTypeMeta):
    TYPE = cavro.ArrayType


class BooleanSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.BoolType


class BytesSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.BytesType


class DoubleSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.DoubleType


class EnumSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.EnumType


class FixedSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.FixedType


class FloatSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.FloatType


class IntSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.IntType


class LongSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.LongType


class MapSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.MapType


class NullSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.NullType


class RecordSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.RecordType


class StringSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.StringType


class UnionSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.UnionType


class Schema(cavro.Schema):
    def __init__(self, source, *args, **kwargs):
        try:
            super().__init__(source, *args, **kwargs)
        except json.decoder.JSONDecodeError as e:
            raise avro_compat.avro.errors.SchemaParseException(f"Error parsing JSON: {source}, error = {e}") from e

    def __getattr__(self, name):
        return getattr(self.type, name)

    @property
    def logicalType(self):
        return self.type.logical_type.logical_name

    def get_prop(self, prop):
        if self.type.logical_type is not None:
            try:
                return getattr(self.type.logical_type, prop)
            except AttributeError:
                pass
        return getattr(self, prop)
        


def parse(json_string: str, validate_enum_symbols: bool = True, validate_names: bool = True) -> Schema:
    options = cavro.Options(
        enum_symbols_must_be_unique=validate_enum_symbols,
        enforce_enum_symbol_name_rules=validate_enum_symbols,
        types_str_to_schema=True,
        fingerprint_returns_digest=True,
    )
    return Schema(json_string, options=options)
