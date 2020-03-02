### Kafka ###

Kafka support for Seastar is an R&D project aimed at providing
high performance, low-latency Kafka producer and consumer
implementations.

Source files residing in this directory will be compiled only
by setting an appropriate flag.
ninja: ./configure.py --enable-experimental-kafka
CMake: -DSeastar\_EXPERIMENTAL\_KAFKA=ON
