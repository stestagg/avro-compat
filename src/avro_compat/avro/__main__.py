try:
    import avro.__main__
except ImportError:
    pass
else:

    def main():
        import avro_compat.avro
        import avro_compat.avro.datafile
        import avro_compat.avro.errors
        import avro_compat.avro.io
        import avro_compat.avro.schema

        avro.__main__.avro = avro_compat.avro
        return avro.__main__.main()


if __name__ == "__main__":
    raise SystemExit(main())
