import decimal
import cavro
import re
from . import schema as schema_
from ._logical_writers import LOGICAL_WRITERS
from . import _write
from .validation import ValidationError

from fastavro._write_common import _is_appendable
from fastavro.json_write import AvroJSONEncoder, json_writer as fa_json_writer


class HybridError(TypeError, ValueError, ValidationError):
    pass


_ERROR_MAP = {
    re.compile(r"required field '(.*?)' missing"): (
        ValueError,
        r"Field \1 is specified in the schema but missing from the record (no value and no default)",
    ),
    re.compile(r"Invalid value (.*?) for type string at (.*)"): (TypeError, r"must be string on field \2"),
    re.compile(r"Invalid value '\.\.\.' for type (.*?) at (.*)"): (
        HybridError,
        r"record contains more fields than the schema specifies: \2",
    ),
    re.compile(r"Invalid value '<missing>' for type (.*?) at (.*)"): (
        HybridError,
        r"\2 is specified in the schema but missing from the record",
    ),
    re.compile(r"Invalid value (.*?) for type (.*?) at (.*)"): (
        HybridError,
        r"Invalid value \1 for type \2 on field \3",
    ),
    re.compile(r"Invalid value (.*?) for type (.*?)"): (HybridError, r"Invalid value \1 for type \2"),
    re.compile(r"Field (\w+) is required for record (.*)"): (
        HybridError,
        r"\1 is specified in the schema but missing from the record \2",
    ),
}

_ORIG_LOGICAL_WRITERS = LOGICAL_WRITERS.copy()


def _substitute_write_error(value, e):
    err_val = str(e)
    for k, (exc, v) in _ERROR_MAP.items():
        if match := k.match(err_val):
            return exc(match.expand(v))
    return e


class Writer:
    def __init__(
        self,
        fo,
        schema,
        codec="null",
        sync_interval=1000 * 16,
        metadata=None,
        validator=None,
        sync_marker=None,
        compression_level=None,
        options={},
    ):
        self.fo = fo
        self.schema = schema
        self.codec = codec
        self.sync_interval = sync_interval
        self.metadata = metadata
        self.validator = validator
        self.sync_marker = sync_marker
        self.compression_level = compression_level
        self.options = options

        base = None
        if isinstance(schema, schema_.SchemaAnnotation):
            base = schema_._get_cschema(schema).options

        schema_options = schema_._get_options(base, **options)

        write_header = True

        if _is_appendable(fo):
            write_header = False
            fo.seek(0)
            reader = cavro.ContainerReader(fo, options=schema_options)
            reader._read_marker()
            schema = reader.schema
            sync_marker = reader.marker
            codec = reader.codec_name
            fo.seek(0, 2)
        else:
            schema = schema_._get_cschema(schema_.parse_schema(schema, _options=schema_options))

        try:
            self._container = cavro.ContainerWriter(
                fo,
                schema,
                codec,
                max_blocksize=sync_interval,
                metadata=metadata,
                marker=sync_marker,
                write_header=write_header,
                options=schema_options,
            )
        except cavro.CodecUnavailable as e:
            raise ValueError(f"unrecognized codec: {codec}") from e

    @property
    def block_count(self):
        return self._container.num_pending

    def dump(self):
        self._container.flush()

    def flush(self):
        self._container.flush()

    def write(self, record):
        self._container.write_one(record)

    def write_block(self, block):
        items = list(block)
        self._container.write_many(items)
        self._container.flush()


def writer(
    fo,
    schema,
    records,
    codec="null",
    sync_interval=1000 * 16,
    metadata=None,
    validator=None,
    sync_marker=None,
    codec_compression_level=None,
    *,
    strict=False,
    strict_allow_default=False,
    disable_tuple_notation=False,
):
    if isinstance(records, dict):
        raise ValueError('"records" argument should be an iterable, not dict')
    output = Writer(
        fo,
        schema,
        codec,
        sync_interval,
        metadata,
        validator,
        sync_marker,
        codec_compression_level,
        options={
            "strict": strict,
            "strict_allow_default": strict_allow_default,
            "disable_tuple_notation": disable_tuple_notation,
        },
    )
    try:
        output._container.write_many(records)
    except ValueError as e:
        raise _substitute_write_error(records, e) from e
    output._container.flush(True)


def schemaless_writer(fo, schema, record, **kwargs):
    schema = schema_.parse_schema(schema, **kwargs)
    schema = schema_._get_cschema(schema)
    writer = cavro.FileWriter(fo)
    try:
        schema.binary_write(writer, record)
    except (decimal.InvalidOperation, cavro.ExponentTooLarge) as e:
        raise ValueError(str(e)) from e
    except ValueError as e:
        raise _substitute_write_error(record, e) from e


def json_writer(fo, schema, records, *, validator=False, encoder=AvroJSONEncoder, **kwargs):
    if encoder is not AvroJSONEncoder:
        return fa_json_writer(fo, schema, records, validator=validator, encoder=encoder, **kwargs)
    kwargs.setdefault("coerce_values_to_str", True)
    schema = schema_.parse_schema(schema, **kwargs)
    schema = schema_._get_cschema(schema)
    for record in records:
        try:
            fo.write(schema.json_encode(record))
        except (decimal.InvalidOperation, cavro.ExponentTooLarge) as e:
            raise ValueError(str(e)) from e
        except ValueError as e:
            raise _substitute_write_error(record, e) from e
        fo.write("\n")
