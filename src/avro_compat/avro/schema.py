from packaging import version as _version
import warnings
from typing import Any
import cavro
import json
from pathlib import Path
from inspect import signature

from avro_compat.avro import avro_version, OPTIONS
import avro_compat.avro.constants
from avro_compat.avro.errors import SchemaParseException

import avro.schema

try:
    from avro_compat.avro.constants import PRIMITIVE_TYPES
except ImportError:
    from avro.schema import PRIMITIVE_TYPES

if avro_version >= _version.parse("1.11.0"):
    from avro.name import Names
else:
    from avro.schema import Names


def instantiate_type(cls, *args, validate_names=True, **kwargs):
    if isinstance(cls.TYPE, tuple):
        raise NotImplementedError("Multiple types not supported")

    if args:
        init_sig = signature(getattr(avro.schema, cls.__name__).__init__)
        kwargs = init_sig.bind(None, *args, **kwargs).arguments

    source = {"type": cls.TYPE.type_name, **kwargs}
    schema = parse(source, validate_names=validate_names)
    return schema.type


class AvroTypeMeta(type):
    def __new__(cls, name, bases, classdict):
        result = type.__new__(cls, name, bases, dict(classdict))
        result.__new__ = instantiate_type
        return result

    def __instancecheck__(cls, inst) -> bool:
        if not isinstance(inst, (cavro.Schema, cavro.LogicalType)):
            return False
        if not isinstance(inst.type, cls.TYPE):
            return False
        if logical_type := getattr(cls, "LOGICAL_TYPE", None):
            if not any(isinstance(a, logical_type) for a in inst.type.value_adapters):
                return False
        return True


class DecimalLogicalSchema(metaclass=AvroTypeMeta):
    TYPE = (cavro.BytesType, cavro.FixedType)
    LOGICAL_TYPE = cavro.DecimalType


class FixedDecimalSchema(metaclass=AvroTypeMeta):
    TYPE = cavro.FixedType
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
        self._hash = None

    def __getattr__(self, name):
        return getattr(self.type, name)

    def __str__(self):
        return self.schema_str

    def __eq__(self, other):
        if isinstance(other, Schema):
            return self.canonical_form == other.canonical_form
        return False

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(self.fingerprint())
        return self._hash

    @property
    def logicalType(self):
        for adapter in self.type.value_adapters:
            if hasattr(adapter, "logical_name"):
                return adapter.logical_name
        raise AttributeError("Logical type not found")

    def get_prop(self, prop):
        for adapter in self.type.value_adapters:
            try:
                return getattr(adapter, prop)
            except AttributeError:
                pass
        return getattr(self, prop)

    def to_json(self):
        return self.schema

    @property
    def other_props(self):
        return self.metadata

    @property
    def schemas(self):
        if isinstance(self.type, cavro.UnionType):
            return self.type.union_types
        raise AttributeError("schemas")


class _NullNameType:
    type = None
    name = None
    effective_namespace = None


class Name:
    def __init__(self, name_attr=None, space_attr=None, default_space=None, validate_name=True):
        schema = parse('"null"', validate_names=validate_name)
        if name_attr is None:
            self._type = _NullNameType
        else:
            self._type = cavro._NamedType(schema, {"name": name_attr, "namespace": space_attr}, default_space)

    def __eq__(self, other):
        if not isinstance(other, Name):
            return False
        return self._type.type == other._type.type

    @property
    def fullname(self):
        return self._type.type

    @property
    def name(self):
        return self._type.name

    @property
    def space(self):
        return self._type.effective_namespace


def parse(
    json_string: str,
    validate_enum_symbols: bool = True,
    validate_names: bool = True,
    return_record_name=False,
    return_record_name_override=False,
    handle_unicode_errors="strict",
    return_named_type=False,
    return_named_type_override=False,
) -> Schema:
    try:
        return Schema(
            json_string,
            options=OPTIONS.replace(
                enum_symbols_must_be_unique=validate_enum_symbols,
                enforce_enum_symbol_name_rules=validate_enum_symbols,
                enforce_type_name_rules=validate_names,
                enforce_namespace_name_rules=validate_names,
            ),
        )
    except (ValueError, TypeError, KeyError) as e:
        raise SchemaParseException(str(e)) from e


def Parse(*a, **kw):
    warnings.warn("`Parse` is deprecated, use `parse` instead", DeprecationWarning)
    return parse(*a, **kw)


def from_path(path, **kwargs) -> Schema:
    path = Path(path)
    return parse(path.read_text(), **kwargs)
