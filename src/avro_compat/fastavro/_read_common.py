import cavro

HEADER_SCHEMA = {
    "type": "record",
    "name": "org.apache.avro.file.Header",
    "fields": [
        {
            "name": "magic",
            "type": {"type": "fixed", "name": "magic", "size": len(cavro.OBJ_MAGIC_BYTES)},
        },
        {"name": "meta", "type": {"type": "map", "values": "bytes"}},
        {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": 16}},
    ],
}

SchemaResolutionError = cavro.CannotPromoteError
