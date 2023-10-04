import cavro
from avro_compat import avro
import avro_compat.avro.errors
from avro_compat.avro.errors import SchemaResolutionException, AvroTypeException
import avro_compat.avro.schema  # Needed to replicate avro lib
from avro_compat.avro import OPTIONS

from typing import Optional, IO

LONG_SCHEMA = cavro.Schema('"long"', options=OPTIONS)


class BinaryEncoder:
    def __init__(self, writer: IO[bytes]) -> None:
        self.writer = writer

    @property
    def cavro_writer(self):
        return cavro.FileWriter(self.writer)

    def write_long(self, value: int) -> None:
        LONG_SCHEMA.binary_write(self.cavro_writer, value)


class BinaryDecoder:
    def __init__(self, reader: IO[bytes]) -> None:
        self.reader = reader

    @property
    def cavro_reader(self):
        return cavro.FileReader(self.reader)

    def skip_long(self):
        LONG_SCHEMA.binary_read(self.cavro_reader)


class DatumReader:
    def __init__(
        self, writers_schema: Optional[cavro.Schema] = None, readers_schema: Optional[cavro.Schema] = None
    ) -> None:
        self.readers_schema = readers_schema
        self.writers_schema = writers_schema
        self._resolved = {}

    @property
    def _writers_schema(self):
        return self.writers_schema

    @property
    def _readers_schema(self):
        return self.readers_schema

    def _reader_for_writer(self, reader, writer):
        if reader is writer:
            return reader
        key = (reader, writer)
        if key not in self._resolved:
            try:
                self._resolved[key] = reader.reader_for_writer(writer)
            except cavro.CannotPromoteError as e:
                raise avro_compat.avro.errors.SchemaResolutionException(str(e), writer, reader) from e
        return self._resolved[key]

    def read(self, decoder: "BinaryDecoder") -> object:
        if self.writers_schema is None:
            raise avro.errors.IONotReadyException("Cannot read without a writer's schema.")
        if self.readers_schema is None:
            self.readers_schema = self.writers_schema  # To match avro lib behavior
        return self.read_data(self.writers_schema, self.readers_schema, decoder)

    def read_data(self, writers_schema: cavro.Schema, readers_schema: cavro.Schema, decoder: "BinaryDecoder") -> object:
        if isinstance(writers_schema, cavro.AvroType):
            writers_schema = cavro.Schema._wrap_type(writers_schema, OPTIONS)
        if isinstance(readers_schema, cavro.AvroType):
            readers_schema = cavro.Schema._wrap_type(readers_schema, OPTIONS)

        reader = self._reader_for_writer(readers_schema, writers_schema)
        try:
            return reader.binary_read(decoder.cavro_reader)
        except EOFError:
            raise avro_compat.avro.errors.InvalidAvroBinaryEncoding("Not enough data to read value.")
        except cavro.CannotPromoteError as e:
            raise avro_compat.avro.errors.SchemaResolutionException(str(e), writers_schema, readers_schema) from e


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
        if isinstance(writers_schema, cavro.AvroType):
            writers_schema = cavro.Schema._wrap_type(writers_schema, OPTIONS)
        try:
            writers_schema.binary_write(encoder.cavro_writer, datum)
        except cavro.ExponentTooLarge as e:
            raise avro.errors.AvroOutOfScaleException(str(e)) from e
        except cavro.InvalidValue as e:
            msg = f'The datum "{e.value}"'
            if e.schema_path:
                msg += f' provided for "{e.schema_path[-1]}"'
            msg += f' is not an example of the schema "{e.dest_type.get_schema()}"'
            raise avro.errors.AvroTypeException(msg) from e


def validate(expected_schema: avro_compat.avro.schema.Schema, datum: object, raise_on_error: bool = False) -> bool:
    if not expected_schema.can_encode(datum):
        if raise_on_error:
            raise avro.errors.AvroTypeException(
                f'The datum "{datum}" is not an example of the schema "{expected_schema.get_schema()}"'
            )
        return False
    return True


def Validate(*a, **kw):
    return validate(*a, **kw)
