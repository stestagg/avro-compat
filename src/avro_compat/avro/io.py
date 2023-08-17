import cavro
from avro_compat import avro
import avro_compat.avro.errors
import avro_compat.avro.schema  # Needed to replicate avro lib

from typing import Optional, IO


class BinaryEncoder:
    def __init__(self, writer: IO[bytes]) -> None:
        self.writer = writer

    @property
    def cavro_writer(self):
        return cavro.FileObjWriter(self.writer)


class BinaryDecoder:
    def __init__(self, reader: IO[bytes]) -> None:
        self.reader = reader

    @property
    def cavro_reader(self):
        return cavro.FileReader(self.reader)


class DatumReader:
    def __init__(
        self, writers_schema: Optional[cavro.Schema] = None, readers_schema: Optional[cavro.Schema] = None
    ) -> None:
        self.readers_schema = readers_schema
        self.writers_schema = writers_schema

    @property
    def _writers_schema(self):
        return self.writers_schema

    @property
    def _readers_schema(self):
        return self.readers_schema

    def read(self, decoder: "BinaryDecoder") -> object:
        if self.writers_schema is None:
            raise avro.errors.IONotReadyException("Cannot read without a writer's schema.")
        if self.readers_schema is None:
            self.readers_schema = self.writers_schema
        return self.read_data(self.writers_schema, self.readers_schema, decoder)

    def read_data(self, writers_schema: cavro.Schema, readers_schema: cavro.Schema, decoder: "BinaryDecoder") -> object:
        return readers_schema.binary_read(decoder.cavro_reader)


class DatumWriter:
    def __init__(self, writers_schema: Optional[cavro.Schema] = None) -> None:
        self.writers_schema = writers_schema

    @property
    def _writers_schema(self):
        return self.writers_schema

    def write(self, datum: object, encoder: BinaryEncoder) -> None:
        if self.writers_schema is None:
            raise avro.errors.IONotReadyException("Cannot write without a writer's schema.")
        self.write_data(self.writers_schema, datum, encoder)

    def write_data(self, writers_schema: cavro.Schema, datum: object, encoder: BinaryEncoder) -> None:
        writers_schema.binary_write(encoder.cavro_writer, datum)
