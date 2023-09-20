from packaging import version

try:
    import avro
except ImportError:
    __version__ = "1.12.0"
else:
    __version__ = avro.__version__

avro_version = version.parse(__version__)

import cavro

if avro_version >= version.parse("1.11.0"):
    OPTIONS = cavro.Options(
        enum_symbols_must_be_unique=True,
        enforce_enum_symbol_name_rules=True,
        types_str_to_schema=True,
        fingerprint_returns_digest=True,
        canonical_form_repeat_fixed=True,
        record_decodes_to_dict=True,
        decimal_check_exp_overflow=True,
        return_uuid_object=False,
        bytes_default_value_utf8=True,
        allow_primitive_names_in_namespaces=True,  # This appears to contravene the spec,  but is how the avro library works
        allow_error_type=True,  # Support for protocols is embedded in the schema parser
        allow_leading_dot_in_names=False,  # Spec is a bit ambiguous here, but generally this is allowed, except in python library
        alternate_timestamp_millis_encoding=True,
    )
else:
    OPTIONS = cavro.Options(
        enum_symbols_must_be_unique=True,
        enforce_enum_symbol_name_rules=True,
        types_str_to_schema=True,
        fingerprint_returns_digest=True,
        canonical_form_repeat_fixed=False,
        record_decodes_to_dict=True,
        decimal_check_exp_overflow=True,
        return_uuid_object=False,
        allow_primitive_names_in_namespaces=True,  # This appears to contravene the spec,  but is how the avro library works
        allow_error_type=True,  # Support for protocols is embedded in the schema parser
        allow_leading_dot_in_names=True,
        string_types_default_unchanged=True,
    )
