Metrics
=======

Benthos exposes lots of metrics, and depending on your configuration can target
either Statsd, Prometheus, or for debugging purposes implements an HTTP endpoint
where metrics are returned as a JSON structure. By default the debugging
endpoint is chosen.

This document lists some of the most useful metrics exposed by Benthos, there
are lots of more granular metrics available that may not appear here.

## Input

- `input.count`
- `input.connection.up`
- `input.connection.failed`
- `input.connection.lost`
- `input.latency`

## Buffer

- `buffer.backlog`
- `buffer.write.count`
- `buffer.write.error`
- `buffer.read.count`
- `buffer.read.error`
- `buffer.latency`

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
