# parquet4seastar

parquet4sestar is an implementation of the Apache Parquet format
for Seastar projects.

See `tests/column_chunk_writer_test.cc` and `tests/file_writer_test.cc`
for basic usage examples.

TODO: provide a more user-friendly demo.

## Build instructions

First, build Seastar.

Then, install the dependencies: GZIP, Snappy and Thrift >= 0.13.

Then, assuming that Seastar was built in DIR/build/dev, invoke
```
mkdir build
cd build
cmake \
-DCMAKE_PREFIX_PATH=DIR/build/dev \
-DCMAKE_MODULE_PATH=DIR/cmake ..
make
```

`libparquet4seastar.a` will be then be built in build/dev.

TODO: add installation/consumption instructions.
