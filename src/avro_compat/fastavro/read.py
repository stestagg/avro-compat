import cavro
import warnings
from . import schema as schema_
from ._read_common import SchemaResolutionError, HEADER_SCHEMA
from ._logical_readers import LOGICAL_READERS
from . import _read
from .write import _substitute_write_error

from fastavro.read import is_avro, json_reader as fa_json_reader
from fastavro.json_read import AvroJSONDecoder

_ORIG_LOGICAL_READERS = LOGICAL_READERS.copy()


class reader:
    def __init__(self, fo, reader_schema=None, **kwargs):
        reader_cschema = None
        if reader_schema is not None:
            reader_schema = schema_.parse_schema(reader_schema, **kwargs)
            reader_cschema = schema_._get_cschema(reader_schema)
        try:
            self._container = cavro.ContainerReader(
                fo, reader_schema=reader_cschema, options=schema_._get_options(**kwargs)
            )
        except cavro.CodecUnavailable as e:
            raise ValueError("Unrecognized codec") from e
        except EOFError:
            raise ValueError("cannot read header - is it an avro file?")

        self.reader_schema = reader_schema
        self.writer_schema = schema_._wrap_type(self._container.writer_schema.schema, self._container.writer_schema)

    @property
    def schema(self):
        warnings.warn("schema is deprecated, use reader_schema instead", DeprecationWarning)
        return self.reader_schema

    @property
    def _schema(self):
        return schema_._unwrap_schema(self.writer_schema)

    @property
    def codec(self):
        return self._container.codec_name

    @property
    def metadata(self):
        return {k: v.decode() for k, v in self._container.metadata.items()}

    def __iter__(self):
        return self

    def __next__(self):
        return self._container.__next__()


class json_reader:
    pass


class AnyOffset:
    def __eq__(self, other):
        return True


class Block:
    def __init__(self, items, reader):
        self._items = items
        self._reader = reader
        self.num_records = len(items)
        self.offset = AnyOffset()
        self.size = AnyOffset()

    @property
    def codec(self):
        return self._reader._container.codec_name

    @property
    def reader_schema(self):
        return self._reader.reader_schema

    @property
    def writer_schema(self):
        return self._reader.writer_schema

    def __iter__(self):
        return iter(self._items)


class block_reader(reader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_done = False

    def __iter__(self):
        return self

    def __next__(self):
        rec = next(self._container)  # Ensure the next block is read
        n_left = self._container.objects_left_in_block
        items = [rec]
        for _ in range(n_left):
            items.append(next(self._container))
        return Block(items, self)


def schemaless_reader(fo, writer_schema, reader_schema=None, **kwargs):
    if writer_schema == reader_schema:
        reader_schema = None
    writer_schema = schema_._get_cschema(schema_.parse_schema(writer_schema, **kwargs))
    schema = writer_schema
    if reader_schema is not None:
        reader_schema = schema_.parse_schema(reader_schema, **kwargs)
        schema = schema_.reader_for_writer(reader_schema, writer_schema)
    reader = cavro.FileReader(fo)
    return schema.binary_read(reader)


def json_reader(fo, schema, reader_schema=None, *, decoder=AvroJSONDecoder):
    if decoder is not AvroJSONDecoder:
        return fa_json_reader(fo, schema, reader_schema, decoder=decoder)
    
    writer_schema = schema_.parse_schema(schema)
    if reader_schema is None:
        reader_schema = writer_schema
        cschema = schema_._get_cschema(writer_schema)
    else:
        reader_schema = schema_.parse_schema(reader_schema)
        cschema = schema_.reader_for_writer(reader_schema, schema_._get_cschema(writer_schema))

    for line in fo:
        if not line.strip():
            continue
        try:
            yield cschema.json_decode(line)
        except ValueError as e:
            raise _substitute_write_error(line, e) from e
