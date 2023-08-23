try:
    from avro import __version__
except ImportError:
    __version__ = '1.11.2'

import cavro
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
    allow_error_type=True, # Support for protocols is embedded in the schema parser
    allow_leading_dot_in_names=False, # Spec is a bit ambiguous here, but generally this is allowed, except in python library
)