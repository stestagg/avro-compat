import cavro

AvroException = cavro.CavroException


class InvalidAvroBinaryEncoding(AvroException):
    pass


SchemaParseException = AvroException
# class SchemaParseException(AvroException):
#     pass


InvalidName = cavro.InvalidName


class InvalidDefault(SchemaParseException):
    pass


class AvroWarning(UserWarning):
    pass


class IgnoredLogicalType(AvroWarning):
    pass


class AvroTypeException(AvroException):
    pass


class InvalidDefaultException(AvroTypeException):
    pass


class AvroOutOfScaleException(AvroTypeException):
    pass


class SchemaResolutionException(AvroException):
    def __init__(self, fail_msg, writers_schema=None, readers_schema=None, *args):
        super().__init__(fail_msg, *args)


class DataFileException(AvroException):
    pass


class IONotReadyException(AvroException):
    pass


class AvroRemoteException(AvroException):
    pass


class ConnectionClosedException(AvroException):
    pass


class ProtocolParseException(AvroException):
    pass


class UnsupportedCodec(NotImplementedError, AvroException):
    pass


class UsageError(RuntimeError, AvroException):
    pass


class AvroRuntimeException(RuntimeError, AvroException):
    pass


UnknownFingerprintAlgorithmException = cavro.InvalidHasher
