from collections import namedtuple
import cavro

from fastavro.validation import ValidationErrorData, ValidationError

from . import schema as schema_


def validate(
    datum, schema, named_schemas=None, field="", raise_errors=True, strict=False, disable_tuple_notation=False
):
    return validate_many(
        [datum],
        schema,
        named_schemas=named_schemas,
        raise_errors=raise_errors,
        strict=strict,
        disable_tuple_notation=disable_tuple_notation,
    )


def validate_many(
    records,
    schema,
    named_schemas=None,
    raise_errors=True,
    strict=False,
    disable_tuple_notation=False,
    _field="",
):
    try:
        sch = schema_.parse_schema(
            schema,
            named_schemas=named_schemas,
            strict=strict,
            disable_tuple_notation=disable_tuple_notation,
            invalid_value_includes_record_name=True,
        )
    except Exception as e:
        if raise_errors:
            raise ValidationError(ValidationErrorData(records, schema, _field)) from e
        return False
    try:
        for datum in records:
            schema_._get_cschema(sch).binary_encode(datum)
    except cavro.InvalidValue as e:
        if raise_errors:
            path = ".".join(str(x) for x in e.schema_path)
            raise ValidationError(ValidationErrorData(e.value, e.dest_type.get_schema(), path)) from e
        return False
    except Exception as e:
        if raise_errors:
            raise ValidationError(ValidationErrorData(datum, schema, _field)) from e
        return False
    return True
