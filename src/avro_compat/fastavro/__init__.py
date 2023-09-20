import cavro

from avro_compat.fastavro import read
from avro_compat.fastavro import write
from avro_compat.fastavro import schema
from avro_compat.fastavro import validation

reader = read.reader
json_reader = read.json_reader
block_reader = read.block_reader
schemaless_reader = read.schemaless_reader
writer = write.writer
json_writer = write.json_writer
schemaless_writer = write.schemaless_writer
is_avro = read.is_avro
validate = validation.validate
parse_schema = schema.parse_schema
