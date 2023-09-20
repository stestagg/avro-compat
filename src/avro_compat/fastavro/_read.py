from .schema import parse_schema, _get_cschema, _wrap_type, _OPTIONS
import cavro

CYTHON_MODULE = True


def match_types(writer_type, reader_type, named_schemas):
    if isinstance(writer_type, list) and isinstance(reader_type, list):
        return True
    try:
        return match_schemas(writer_type, reader_type, named_schemas)
    except cavro.CavroException:
        return False
    return False


def match_schemas(w_schema, r_schema, named_schemas):
    if named_schemas is None:
        named_schemas = {}

    w_schema = _get_cschema(
        parse_schema(w_schema, named_schemas=named_schemas.get("writer", None), defer_schema_promotion_errors=False)
    )
    r_schema = _get_cschema(
        parse_schema(r_schema, named_schemas=named_schemas.get("reader", None), defer_schema_promotion_errors=False)
    )
    resolved = r_schema.reader_for_writer(w_schema)
    return _wrap_type(resolved.schema, resolved)


def skip_record(fo, writer_schema, named_schemas):
    read_schema = _get_cschema(parse_schema(writer_schema, named_schemas=named_schemas))
    reader = cavro.FileReader(fo)
    _ = read_schema.binary_read(reader)


def _make_primitive_reader(defn):
    schema = cavro.Schema(defn, parse_json=False, options=_OPTIONS)

    def read_fn(fo):
        reader = cavro.FileReader(fo)
        return schema.binary_read(reader)

    read_fn.__name__ = f"read_{defn}"
    return read_fn


def _skipper(reader):
    def skip_fn(fo, *args, **kwargs):
        reader(fo, *args, **kwargs)

    skip_fn.__name__ = f"skip_{reader.__name__}"
    return skip_fn


def _make_complex_reader(ty):
    def read_fn(fo, writer_schema, named_schemas=None, read_schema=None, options={}, **kwargs):
        schema = _get_cschema(parse_schema(writer_schema, named_schemas=named_schemas, **options, **kwargs))
        assert isinstance(schema.type, ty)
        if read_schema is not None:
            r_schema = _get_cschema(parse_schema(read_schema, named_schemas=named_schemas, **options, **kwargs))
            schema = r_schema.reader_for_writer(schema)
        reader = cavro.FileReader(fo)
        return schema.binary_read(reader)

    read_fn.__name__ = f"read_{ty}"
    return read_fn


for name, ty in cavro.PRIMITIVE_TYPES.items():
    globals()[f"read_{name}"] = _make_primitive_reader(name)
    globals()[f"skip_{name}"] = _skipper(globals()[f"read_{name}"])

read_utf8 = read_string
skip_utf8 = skip_string

_read_enum = _make_complex_reader(cavro.EnumType)


def read_enum(fo, writer_schema, reader_schema):
    return _read_enum(fo, writer_schema, read_schema=reader_schema)


skip_enum = skip_long

read_fixed = _make_complex_reader(cavro.FixedType)


def skip_fixed(fo, writer_schema):
    len = writer_schema["size"]
    fo.seek(len, 1)


read_array = _make_complex_reader(cavro.ArrayType)
skip_array = _skipper(read_array)

read_map = _make_complex_reader(cavro.MapType)
skip_map = _skipper(read_map)

read_union = _make_complex_reader(cavro.UnionType)
skip_union = _skipper(read_union)

read_record = _make_complex_reader(cavro.RecordType)
skip_record = _skipper(read_record)

_read_data = _make_complex_reader(cavro.AvroType)
_skip_data = _skipper(_read_data)
