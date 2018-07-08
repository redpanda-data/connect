Metrics
=======

Benthos exposes lots of metrics, and depending on your configuration can target
either Statsd, Prometheus, or for debugging purposes implements an HTTP endpoint
where metrics are returned as a JSON structure. By default the debugging
endpoint is chosen.

This document lists some of the most useful metrics exposed by Benthos, there
are lots of more granular metrics available that may not appear here.

## Input

- `input.count`: Measures the number of messages read by the input.
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

- `processor.<type>.count`
- `processor.<type>.dropped`

## Output

- `output.count`
- `output.send.success`
- `output.send.error`
- `output.connection.up`
- `output.connection.failed`
- `output.connection.lost`
