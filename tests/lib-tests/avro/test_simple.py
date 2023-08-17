import avro_compat.avro as avro
import avro_compat.avro.io 
from io import BytesIO


def test_simple():
    schema = avro.schema.parse('{"type": "string"}')
    reader = avro.io.DatumReader(schema, schema)
    writer = avro.io.DatumWriter(schema)
    buf = BytesIO()
    encoder = avro.io.BinaryEncoder(buf)
    writer.write("hello world", encoder)
    buf.seek(0)
    decoder = avro.io.BinaryDecoder(buf)
    assert "hello world" == reader.read(decoder)
