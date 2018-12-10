Metrics
=======

Benthos exposes lots of metrics, and depending on your configuration can target
either Statsd, Prometheus, or for debugging purposes implements an HTTP endpoint
where metrics are returned as a JSON structure. By default the debugging
endpoint is chosen.

This document lists some of the most useful metrics exposed by Benthos, there
are lots of more granular metrics available that may not appear here.

## Input

- `input.count`: The number of times the input has attempted to read messages.
- `input.received`: The number of messages received by the input.
- `input.batch.received`: The number of message batches received by the input.
- `input.connection.up`
- `input.connection.failed`
- `input.connection.lost`
- `input.latency`: Measures the roundtrip latency from the point at which a
  message is read up to the moment the message has either been acknowledged by
  an output or has been stored within an external buffer.

## Buffer

- `buffer.backlog`: The (sometimes estimated) size of the buffer backlog in
  bytes.
- `buffer.write.count`
- `buffer.write.error`
- `buffer.read.count`
- `buffer.read.error`
- `buffer.latency`: Measures the roundtrip latency from the point at which a
  message is read from the buffer up to the moment it has been acknowledged by
  the output.

## Processors

Processor metrics are prefixed by the area of the Benthos stream they reside in
and their index. For example, processors in the `pipeline` section will be
prefixed with `pipeline.processor.N`, where N is the index.

- `pipeline.processor.0.count`
- `pipeline.processor.0.sent`
- `pipeline.processor.0.batch.sent`
- `pipeline.processor.0.error`

## Conditions

- `condition.count`
- `condition.true`
- `condition.false`

## Output

- `output.count`: The number of times the output has attempted to send messages.
- `output.sent`: The number of messages sent.
- `output.batch.sent`: The number of message batches sent.
- `output.connection.up`
- `output.connection.failed`
- `output.connection.lost`
