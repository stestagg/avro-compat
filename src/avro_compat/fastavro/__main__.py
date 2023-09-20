# We want to import the fastavro __main__, but we need the imports in that file to actually result in using avro-compat.
import sys
import importlib.util

import avro_compat.fastavro

# Get a bunch of compat modules imported so we can patch them in
from avro_compat.fastavro import schema, write, read, validation, _read, _write
from avro_compat.fastavro.io import json_decoder, json_encoder, binary_decoder

fastavro_main = importlib.util.find_spec("fastavro.__main__")

_ORIG_MODULES = sys.modules.copy()
try:
    for key in _ORIG_MODULES.keys():
        if key.startswith("avro_compat.fastavro"):
            new_name = key.replace("avro_compat.fastavro", "fastavro")
    #            sys.modules[new_name] = sys.modules[key]
    mod = importlib.util.module_from_spec(fastavro_main)
    fastavro_main.loader.exec_module(mod)
finally:
    sys.modules.clear()
    sys.modules.update(_ORIG_MODULES)

main = mod.main
CleanJSONEncoder = mod.CleanJSONEncoder

if __name__ == "__main__":
    main()
