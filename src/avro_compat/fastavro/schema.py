import datetime
import functools
import cavro
import copy
import json
from pathlib import Path
import re
import hashlib
import fastavro.repository.base
from avro_compat.avro.schemanormalization import FingerprintAlgorithmNames, Fingerprint
from avro_compat.fastavro._schema_common import SchemaParseException, UnknownType
from avro_compat.fastavro import read
from avro_compat.fastavro import write
import avro_compat.fastavro.repository.flat_dict

FINGERPRINT_ALGORITHMS = FingerprintAlgorithmNames()

epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


class LocalTimestampMicros(cavro.CustomLogicalType):
    logical_name = "local-timestamp-micros"
    underlying_types = (cavro.LongType,)

    @classmethod
    def _for_type(cls, underlying_type):
        return cls()

    def custom_encode_value(self, value):
        if isinstance(value, int):
            return value
        delta = value.replace(tzinfo=datetime.timezone.utc) - epoch
        return int(delta.total_seconds() * 1_000_000)

    def custom_decode_value(self, value):
        return (epoch + datetime.timedelta(microseconds=value)).replace(tzinfo=None)


class LocalTimestampMillis(cavro.CustomLogicalType):
    logical_name = "local-timestamp-millis"
    underlying_types = (cavro.LongType,)

    @classmethod
    def _for_type(cls, underlying_type):
        return cls()

    def custom_encode_value(self, value):
        if isinstance(value, int):
            return value
        delta = value.replace(tzinfo=datetime.timezone.utc) - epoch
        return int(delta.total_seconds() * 1_000)

    def custom_decode_value(self, value):
        return (epoch + datetime.timedelta(microseconds=(value * 1_000))).replace(tzinfo=None)


class ReadPrepareType(cavro.CustomLogicalType):
    def __init__(self, avro_type, logical_name, preparer, reader):
        avro_type_cls = cavro.TYPES_BY_NAME[avro_type]
        self.logical_name = logical_name
        self.underlying_types = (avro_type_cls,)
        self._preparer = preparer
        self._reader = reader
        self._schema = None

    def copy(self):
        return ReadPrepareType(self.underlying_types[0].type_name, self.logical_name, self._preparer, self._reader)

    @property
    def equality_key(self):
        return (self.logical_name, self.underlying_types, self._preparer, self._reader)

    def __eq__(self, other):
        if not isinstance(other, ReadPrepareType):
            return False
        return self.equality_key == other.equality_key

    def __repr__(self):
        return f"<Logical {self.logical_name} for {self.underlying_types[0].type_name}>"

    def _for_type(self, underlying):
        inst = self.copy()
        schema = cavro.Schema._wrap_type(underlying, options=underlying.options)
        inst._schema = _wrap_type(underlying.get_schema(set()), schema)
        return inst

    def custom_encode_value(self, value):
        return self._preparer(value, self._schema)

    def custom_decode_value(self, value):
        return self._reader(value, self._schema, self._schema)


_OPTIONS = cavro.Options(
    allow_error_type=True,
    raise_on_invalid_logical=True,
    record_decodes_to_dict=True,
    allow_aliases_to_be_string=True,
    container_fill_blocks=True,
    enforce_namespace_name_rules=False,
    missing_values_can_be_null=True,  # Conflict
    return_uuid_object=False,
    defer_schema_promotion_errors=True,
    date_type_accepts_string=True,
    record_values_type_hint=True,
    record_allow_extra_fields=True,
    decimal_check_exp_overflow=True,
    alternate_timestamp_millis_encoding=True,
    allow_tuple_notation=True,
    invalid_value_include_array_index=False,
    invalid_value_include_map_key=False,
).with_logical_types(
    LocalTimestampMicros,
    LocalTimestampMillis,
)


def _get_options(
    base=None,
    return_record_name=None,
    return_record_name_override=None,
    handle_unicode_errors=None,
    return_named_type=None,
    return_named_type_override=None,
    strict=None,
    strict_allow_default=None,
    disable_tuple_notation=False,
    _ignore_default_error=False,
    write_union_type=None,
    **kwargs,
):
    if base is None:
        base = _OPTIONS
    if kwargs:
        base = base.replace(**kwargs)

    # Add any new custom logical types from fastavro
    all_logical = read.LOGICAL_READERS.keys() | write.LOGICAL_WRITERS.keys()
    func_based_logical = []
    existing_logical = {l.equality_key for l in base.logical_types if isinstance(l, ReadPrepareType)}

    for key in all_logical:
        reader_func = read.LOGICAL_READERS.get(key)
        writer_func = write.LOGICAL_WRITERS.get(key)
        if read._ORIG_LOGICAL_READERS.get(key) is reader_func and write._ORIG_LOGICAL_WRITERS.get(key) is writer_func:
            continue
        type_name, logical_name = key.split("-", 1)
        new_logical = ReadPrepareType(type_name, logical_name, writer_func, reader_func)
        if new_logical.equality_key not in existing_logical:
            func_based_logical.append(new_logical)
    base = base.with_logical_types(*func_based_logical)

    updates = {}
    if return_record_name is not None:
        if return_record_name:
            if return_record_name_override:
                updates["union_decodes_to"] = cavro.UnionDecodeOption.TYPE_TUPLE_IF_RECORD_AMBIGUOUS
            else:
                updates["union_decodes_to"] = cavro.UnionDecodeOption.TYPE_TUPLE_IF_RECORD
        else:
            updates["union_decodes_to"] = cavro.UnionDecodeOption.RAW_VALUES

    if return_named_type is not None:
        updates.setdefault("union_decodes_to", cavro.UnionDecodeOption.RAW_VALUES)
        if return_named_type:
            if return_named_type_override:
                updates["union_read_val"] = cavro.UnionDecodeOption.TYPE_TUPLE_IF_AMBIGUOUS
            else:
                updates["union_read_val"] = cavro.UnionDecodeOption.TYPE_TUPLE_ALWAYS
    if strict is not None:
        # Strict mode disallows extra fields or defaults
        updates["record_allow_extra_fields"] = not strict
        updates["record_encode_use_defaults"] = not strict
    if strict_allow_default:
        # Strict mode disallows extra fields but allows defaults
        updates["record_allow_extra_fields"] = False
        updates["record_encode_use_defaults"] = True
    if disable_tuple_notation:
        updates["allow_tuple_notation"] = False
    if _ignore_default_error:
        updates["allow_union_default_any_member"] = True
        updates["allow_invalid_default_values"] = True
    if write_union_type is not None:
        updates["union_json_encodes_type_name"] = write_union_type
    if handle_unicode_errors is not None:
        updates["unicode_errors"] = handle_unicode_errors
    return base.replace(**updates)


_ERROR_MAP = {
    re.compile(r"Scale must be a positive integer"): "decimal scale must be a positive integer",
    re.compile(r"Precision must be an integer between"): "decimal precision must be a positive integer",
    re.compile(r"Precision must be greater than scale"): "decimal scale must be less than or equal to precision",
    re.compile(
        r"Precision is too large for fixed size (\d+). Precision: (\d+)"
    ): r"decimal precision of \2 doesn't fit into array of length \1",
    re.compile(r"Name '(.*?)' appears multiple times in schema"): r"redefined named type: \1",
    re.compile(
        r"Enum symbol( \'|s must be a list of strings)"
    ): "Every symbol must match the regular expression [A-Za-z_][A-Za-z0-9_]*",
    re.compile(r"Enum symbols must be unique"): "All symbols in an enum must be unique",
    re.compile(r"Default value .*? not in enum symbols"): "Default value for enum must be in symbols list",
    re.compile(
        r"Default value (.*?)(:? for field .*?)? is not valid for union"
    ): r"Default value <\1> must match first schema in union",
    re.compile(r"Default value (.*?) is not valid for field:"): r"Default value <\1> must match schema type",
}


def _substitute_parse_error(value, e):
    err_val = str(e)
    for k, v in _ERROR_MAP.items():
        if match := k.match(err_val):
            return match.expand(v)
    return f"Error parsing schema: {value}, error = {e}"


def _get_cschema(schema):
    return schema._SchemaAnnotation__schema


def _unwrap_schema(schema):
    return schema._SchemaAnnotation__orig


class SchemaAnnotation:
    def __new__(cls, value, schema, orig=None):
        if orig is None:
            orig = value
        inst = super().__new__(cls, value)
        inst._SchemaAnnotation__schema = schema
        inst._SchemaAnnotation__orig = orig
        return inst

    def __init__(self, value, schema=None, orig=None):
        if not isinstance(self, str):
            super().__init__(value)

    @functools.lru_cache(maxsize=32)
    def __reader_for_writer(self, writer_schema):
        return self.__schema.reader_for_writer(writer_schema)


    def __contains__(self, name):
        if name in {"__fastavro_parsed"}:
            return True
        return super().__contains__(name)

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, SchemaAnnotation):
            other = parse_schema(other, _options=self.__schema.options)
        return _get_cschema(self).canonical_form == _get_cschema(other).canonical_form

    def pop(self, name):
        if name in {"__fastavro_parsed"}:
            return
        return super().pop(name)

    def __copy__(self):
        copy_val = copy.copy(self.__orig)
        return type(self)(copy_val, self.__schema)

    def __deepcopy__(self, memo):
        copy_val = copy.deepcopy(self.__orig, memo)
        return type(self)(copy_val, self.__schema)


def reader_for_writer(reader, writer):
    return reader._SchemaAnnotation__schema.reader_for_writer(writer)


_annotated_types = {}


def _get_type(base_ty):
    if base_ty not in _annotated_types:
        new_ty = type(f"Wrapped{base_ty.__name__}", (SchemaAnnotation, base_ty), {})
        _annotated_types[base_ty] = new_ty
    return _annotated_types[base_ty]


def _wrap_type(orig_value, schema):
    value = orig_value
    ty = _get_type(type(value))
    if isinstance(value, list):
        subtypes = schema.type.union_types
        value = [_wrap_type(v, schema._wrap_type(subtype)) for subtype, v in zip(subtypes, value)]
    return ty(value, schema, orig=orig_value)


def load_schema(schema_path, *, repo=None, named_schemas=None, _write_hint=True, _injected_schemas=None):
    schema = _load_schema(schema_path, repo=repo, named_schemas=named_schemas, _write_hint=_write_hint)
    resolved_schema = _get_cschema(schema)
    return _wrap_type(resolved_schema.schema, resolved_schema)


def _load_schema(schema_path, *, repo=None, named_schemas=None, _write_hint=True, _injected_schemas=None):
    if named_schemas is None:
        named_schemas = {}
    if _injected_schemas is None:
        _injected_schemas = set()
    if repo is None:
        if not isinstance(schema_path, Path):
            schema_path = Path(schema_path)
        assert schema_path.suffix == ".avsc"
        repo = avro_compat.fastavro.repository.flat_dict.FlatDictRepository(str(schema_path.parent))
        schema_path = schema_path.stem
    try:
        schema_ob = repo.load(schema_path)
    except fastavro.repository.base.SchemaRepositoryError as e:
        raise UnknownType(f"Unknown schema {schema_path}") from e
    new_named = named_schemas.copy()
    try:
        schema = parse_schema(
            schema_ob,
            named_schemas=new_named,
            _write_hint=_write_hint,
            _options=_OPTIONS.replace(inline_namespaces=True),
        )
    except UnknownType as e:
        missing_name = e.name
        if missing_name in _injected_schemas:
            raise
        _injected_schemas.add(missing_name)
        _load_schema(
            missing_name,
            repo=repo,
            named_schemas=named_schemas,
            _write_hint=_write_hint,
            _injected_schemas=_injected_schemas,
        )
        return _load_schema(
            schema_path,
            repo=repo,
            named_schemas=named_schemas,
            _write_hint=_write_hint,
            _injected_schemas=_injected_schemas,
        )
    named_schemas.update(new_named)
    return schema


def parse_schema(
    schema,
    named_schemas=None,
    *,
    expand=False,
    _write_hint=True,
    _force=False,
    _options=None,
    _unknown_named_types=True,
    **kwargs,
):
    if _options is None:
        _options = _OPTIONS

    options = _get_options(_options, **kwargs) if kwargs else _options

    if isinstance(schema, SchemaAnnotation):
        c_schema = _get_cschema(schema)
        if not _force and (
            c_schema.options is options
            or c_schema.options.equals(
                options, ignore=["invalid_value_includes_record_name", "externally_defined_types"]
            )
        ):
            return schema
        schema = _unwrap_schema(schema)

    named_types = None
    if named_schemas is not None and named_schemas:
        named_types = {}
        for k, v in named_schemas.items():
            if _unknown_named_types:
                v = parse_schema(
                    v, named_schemas=None, _write_hint=_write_hint, _unknown_named_types=False, _options=options
                )
            named_schemas[k] = v
            named_types[k] = _get_cschema(v).type
        options = options.with_external_types(named_types)
    try:
        cavro_schema = cavro.Schema(schema, parse_json=False, options=options)
    except UnknownType:
        raise
    except (KeyError, ValueError, TypeError, cavro.CavroException) as e:
        msg = _substitute_parse_error(schema, e)
        raise SchemaParseException(msg) from e

    if named_schemas is not None:
        for key, value in cavro_schema.named_types.items():
            named_schemas[key] = _wrap_type(value.get_schema(), cavro_schema._wrap_type(value))

    return _wrap_type(schema, cavro_schema)


def schema_name(schema, parent_ns):
    schema = parse_schema(schema)
    ty = _get_cschema(schema).type
    return ty.effective_namespace, ty.type


def fullname(schema):
    ns, name = schema_name(schema, None)
    return name


def expand_schema(schema):
    options = _OPTIONS
    if isinstance(schema, SchemaAnnotation):
        options = _get_cschema(schema).options
        schema = _get_cschema(schema).schema
    schema = parse_schema(
        schema, _force=True, _options=options.replace(expand_types_in_schema=True, inline_namespaces=True)
    )
    ty = _get_cschema(schema).type
    return ty.get_schema(set())


def load_schema_ordered(ordered_schemas, *, _write_hint=True):
    named_schemas = {}
    for schema in ordered_schemas[:-1]:
        load_schema(schema, named_schemas=named_schemas, _write_hint=False)
    return load_schema(ordered_schemas[-1], named_schemas=named_schemas, _write_hint=_write_hint)


def to_parsing_canonical_form(schema):
    schema = parse_schema(schema)
    return _get_cschema(schema).canonical_form


def fingerprint(parsing_canonical_form, algorithm):
    # algorithm = FINGERPRINT_ALGORITHMS[algorithm]
    try:
        return Fingerprint(parsing_canonical_form, algorithm, _hex=True)
    except ValueError as e:
        raise ValueError(f"Unknown schema fingerprint algorithm {algorithm}") from e
