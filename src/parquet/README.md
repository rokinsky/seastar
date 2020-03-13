### Parquet ###

Parquet support for Seastar is an R&D project aimed at providing
a high performance, low latency library for reading and writing
files in Parquet format.

Source files residing in this directory will be compiled only
by setting an appropriate flag.
ninja: ./configure.py --enable-experimental-parquet
CMake: -DSeastar\_EXPERIMENTAL\_PARQUET=ON

#### File description:

The files are listed in the order from the lowest to the highest level of dependency.

rle_encoding.h and bpacking.h were copied from the Arrow and contain
some needed bit-packing routines. Together they are fully self-contained.

parquet.thrift contains the definition of parquet metadata structures.
parquet_types.h and parquet_types.cpp contain the corresponding C++ declarations
and deserializers generated from parquet.thrift by the thrift compiler.

overloaded.hh (extremely small) defines some functional programming utilities.

compression.hh and compression.cc wrap (TODO) all the compression libraries used
in parquet.

exception.hh defines the exception type thrown by the parquet library code.
Note that exceptions coming from the lower level
(i.e. from libstdc++, Thrift or compression libraries) might also be emitted.

schema.hh and schema.cc are responsible for converting the metadata written
in the parquet file to a workable form. In particular, they convert the node list
to a node tree, compute the definition and repetition levels of resulting nodes,
and assign logical types (LIST, MAP, DECIMAL, UUID and such) to them,
applying all the boring rules from
https://github.com/apache/parquet-format/blob/master/LogicalTypes.md

file_reader.hh and file_reader.cc contain the low-level core of the library.
They interface with the upper layers through column_chunk_reader and file_reader.
A column_chunk_reader takes a column chunk (the sequential IO unit of parquet)
and its associated metadata and decodes it to a stream of
(definition level, repetition level, value) triplets.
file_reader provides an interface for opening parquet files, reading their metadata
and spawning column_chunk_readers.

record_reader.hh and record_reader.cc provide the algorithm to assemble the outputs
of column_chunk_readers into complex records. They are used by providing them a
Consumer type which implements all the possible callbacks
("start record", "start field", "end list", "append value", etc.).

cql_reader.hh and cql_reader.cc provide an interface for exporting a parquet file
to CQL statements. They convert parquet types to CQL types, print the parquet
schema as CQL CREATE statements, and provide a Consumer to the record_reader,
which interprets the parquet records as CQL INSERT statements.
