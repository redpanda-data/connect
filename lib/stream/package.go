// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
