from packaging import version as _version
import avro_compat.avro


if avro_compat.avro.avro_version >= _version.parse("1.11.0"):
    from avro.constants import *
