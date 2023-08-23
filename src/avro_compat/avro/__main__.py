import avro.__main__

if __name__ == '__main__':
    import avro_compat.avro
    import avro_compat.avro.datafile
    import avro_compat.avro.errors
    import avro_compat.avro.io
    import avro_compat.avro.schema

    avro.__main__.avro = avro_compat.avro
    raise SystemExit(avro.__main__.main())
