Kafka JSON Mutating
===================

In this example we start off with a stream of JSON data being written to a Kafka
topic. Our task is to consume the stream, mutate the data into a new format,
filter certain items, and write the stream to a new topic.

The main factor of concern in this example is throughput, it needs to be as high
as possible, but we also need to preserve at-least-once delivery guarantees.

Since the pipeline is mostly going to be throttled by CPU the example places
mutation logic on dedicated processing threads that match the number of logical
CPUs. In order to keep the processing threads busy with traffic there are
parameters in the config for tuning the number of concurrent readers.

TODO

[example]: ./kafka-json-mutating.yaml
