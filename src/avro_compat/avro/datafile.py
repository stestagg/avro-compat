from typing import IO, AnyStr, Any, Optional
from .schema import Schema
from .io import BinaryEncoder
from .codecs import KNOWN_CODECS
from avro_compat.avro import OPTIONS
import cavro

from io import TextIOBase


NULL_CODEC = "null"
DEFLATE_CODEC = "deflate"
BZIP2_CODEC = "bzip2"
SNAPPY_CODEC = "snappy"
XZ_CODEC = "xz"
ZSTANDARD_CODEC = "zstandard"

VALID_CODECS = frozenset(KNOWN_CODECS.keys())
VALID_ENCODINGS = ["binary"]
CODEC_KEY = "avro.codec"
SCHEMA_KEY = "avro.schema"


class DataFileWriter:
    def __init__(
        self,
        writer: IO[AnyStr],
        datum_writer: Any,
        writers_schema: Optional[Schema] = None,
        codec: str = "null",
        writer_schema=None,
    ):
        self.writer = writer
        write_header = True
        marker = None

        if writers_schema is None:
            writers_schema = writer_schema

        # Trash the python io model to get a non-text writer if we're given one
        if isinstance(writer, TextIOBase) and hasattr(writer, "buffer"):
            writer = writer.buffer

        if writers_schema is None:
            write_header = False
            if hasattr(writer, "seek") and hasattr(writer, "tell"):
                cur_pos = writer.tell()
                writer.seek(0)
                reader = cavro.ContainerReader(writer, options=OPTIONS)
                reader._read_marker()
                writers_schema = reader.schema
                marker = reader.marker
                codec = reader.codec_name
                writer.seek(cur_pos)
            else:
                raise ValueError("When writers_schema is None, writer must be a seekable file-like object.")
        self.writers_schema = writers_schema  # TODO
        self.container = cavro.ContainerWriter(writer, writers_schema, codec, write_header=write_header, marker=marker)
        self.codec = codec
        self.encoder = BinaryEncoder(writer)

    @property
    def sync_marker(self):
        return self.container.marker

    def __enter__(self):
        self.container.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        rv = self.container.__exit__(exc_type, exc_value, traceback)
        self.writer.close()
        return rv

    def flush(self):
        self.container.flush(force=True)

    def append(self, record):
        self.container.write_one(record)

    def set_meta(self, key, value):
        assert self.container.blocks_written == 0, "Can't set metadata after writing data"
        if isinstance(value, str):
            value = value.encode()
        self.container.metadata[key] = value

    SetMeta = set_meta


class DataFileReader:
    def __init__(self, reader: IO[AnyStr], datum_reader: Any):
        self.reader = reader
        self.container = cavro.ContainerReader(reader, options=OPTIONS)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reader.close()
        return False

    def get_meta(self, key):
        return self.container.metadata[key]

    GetMeta = get_meta

    def __iter__(self):
        return self

    def __next__(self):
        return self.container.__next__()

    @property
    def schema(self):
        return self.container.schema.schema_str
