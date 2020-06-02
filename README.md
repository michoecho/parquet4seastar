# parquet4seastar

parquet4sestar is an implementation of the Apache Parquet format
for Seastar projects.

The project consists mainly of the `parquet4seastar` library.
See `examples/example.cc` for a usage example of the basic library
functionality, which is writing/reading `.parquet` files in batches of
(definition level, repetition level, value) triplets to/from chosen columns.
See `src/cql_reader.cc` and `apps/main.cc` for a usage of the record reader
functionality (consuming assembled records by providing callbacks for each part
of the record).

The `apps` directory contains a `parquet2cql` tool which can be used to
print `.parquet` files to CQL. It can be invoked with:
```
BUILDDIR/apps/parquet2cql/parquet2cql --table TABLENAME --pk ROW_INDEX_COLUMN_NAME --file PARQUET_FILE_PATH
```

This project is not battle-tested and should not yet be considered stable.
The interface of the library is subject to change.

## Build instructions

The library follows standard CMake practices.

First, build Seastar.
Then, install the dependencies: GZIP, Snappy and Thrift >= 0.11.
Then, assuming that Seastar was built in DIR/build/dev, invoke
```
mkdir build
cd build
cmake \
-DCMAKE_PREFIX_PATH=DIR/build/dev \
-DCMAKE_MODULE_PATH=DIR/cmake ..
make
```

`libparquet4seastar.a`, tests and apps will be then be built in `build`.

The library can then be optionally installed with `make install` or consumed
directly from the build directory. Use of CMake for consuming the library
is recommended.

GZIP and Snappy are the only compression libraries used by default.
Support for other compression libraries used in Parquet files
can be added by merging #2.
