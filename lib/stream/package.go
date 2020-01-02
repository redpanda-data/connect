/*
Package stream creates and manages a full Benthos stream pipeline, consisting
of an input layer of consumers, an optional buffer layer, a processing pipelines
layer, and an output layer of producers:

	Inputs -> Buffer -> Processing Pipelines -> Outputs

The number of parallel input consumers, processing pipelines, and output
producers depends on the configuration of the stream.

Custom Stream Processors

It is possible to construct a stream with your own custom processor
implementations embedded within it. This results in your processors being
executed for each discrete message batch at the end of any other configured
processors.

Your custom processors will be constructed once per pipeline processing thread,
e.g. with four pipeline processing threads the pipeline would look like this:

	Inputs -> Buffer -> Processing Pipeline -> Custom Processor -> Outputs
	                 \  Processing Pipeline -> Custom Processor /
	                 \  Processing Pipeline -> Custom Processor /
	                 \  Processing Pipeline -> Custom Processor /

Plugins

Benthos components (inputs, processors, conditions, outputs, etc) are pluggable
by design, and can be complemented with your custom implementations by calling
RegisterPlugin on a component package.

This method is more complicated than simply adding a custom stream processor,
but allows you to use your custom implementations in the same flexible way that
native Benthos types can be used.

Message Batches

In Benthos every message is a batch, and it is the configuration of a stream
that determines the size of each batch (usually 1.) Therefore all processors,
including your custom implementations, support batches.

Sometimes your custom processors will require batches of a certain size in order
to function. It is recommended that you perform message batching using the
standard Benthos batch or combine processors, as it will ensure resiliency
throughout the stream pipeline. For example, you can add a batch processor to
your input layer:

	conf := NewConfig()

	conf.Input.Type = input.TypeKafka
	conf.Input.Kafka.Addresses = []string{"localhost:9092"}
	conf.Input.Kafka.Topic = "example_topic_one"

	conf.Input.Processors = append(conf.Input.Processors, processor.NewConfig())
	conf.Input.Processors[0].Type = processor.TypeBatch
	conf.Input.Processors[0].Batch.ByteSize = 10000000 // 10MB

Horizontal Scaling

The standard set of processors of a Benthos stream are stateless and can
therefore be horizontally scaled without impacting the results. Horizontal
scaling therefore only depends on the sources of data of a stream.

Most message queues/protocols provide mechanisms to automatically distribute
messages horizontally across consumers. Horizontally scaling Benthos is
therefore as simple as applying those means.

Kafka, for example, allows you to distribute messages across partitions, which
can either be statically distributed across consumers or, using the
kafka_balanced input type, can be dynamically distributed across consumers.

Vertical Scaling

Vertically scaled message processing can be done in Benthos with parallel
processing pipelines, where the number of threads is configurable in the
pipeline second of a stream configuration. However, in order to saturate those
processing threads your configuration needs one of two things: multiple parallel
inputs or a memory buffer.

Adding a memory buffer is a simple way of scaling a single input consumer across
processing threads, but this removes the automatic delivery guarantees that
Benthos provides.

Instead, it is recommended that you create parallel input sources, the number of
which should at least match the number of processing threads. This retains the
delivery guarantees of your sources and sinks by keeping them tightly coupled
and is done by configuring a broker input type, for example, processing across
four threads with eight parallel consumers:

	// Create a Kafka input with automatic partition balancing
	inputConf := input.NewConfig()

	inputConf.Type = input.TypeKafkaBalanced
	inputConf.KafkaBalanced.Addresses = []string{"localhost:9092"}
	inputConf.KafkaBalanced.Topics = []string{"example_topic_one"}

	// Create a decompression processor (default gzip)
	processorConf := processor.NewConfig()
	processorConf.Type = processor.TypeDecompress

	// Create a stream with eight parallel consumers and four processing threads
	conf := NewConfig()

	conf.Input.Type = input.TypeBroker
	conf.Input.Broker.Inputs = append(conf.Input.Broker.Inputs, inputConf)
	conf.Input.Broker.Copies = 8

	conf.Pipeline.Processors = append(conf.Pipeline.Processors, processorConf)
	conf.Pipeline.Threads = 4

Delivery Guarantees

A Benthos stream, without a buffer (the default), guarantees at-least-once
message delivery matching the source and sink protocols used. Meaning if you are
consuming a Kafka stream and producing to a Kafka stream then Benthos matches
the at-least-once delivery guarantees of Kafka.

If you configure a stream with a buffer then your delivery guarantees will
depend on the resiliency of the buffer method you have chosen.

Processor Idempotency

Benthos processors are usually stateless operations that are idempotent by their
nature, meaning duplicate messages travelling the pipeline do not impact the
result of the processor itself.

If your custom processors are stateful and exhibit side effects you will need to
implement your own tooling in order to guarantee exactly-once processing of
messages.
*/
package stream
