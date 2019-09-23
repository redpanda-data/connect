Buffers
=======

This document was generated with `benthos --list-buffers`

Benthos uses a transaction based model for guaranteeing delivery of messages
without the need for a buffer. This ensures that messages are never acknowledged
from a source until the message has left the target sink.

However, sometimes the transaction model is undesired, in which case there are a
range of buffer options available which decouple input sources from the rest of
the Benthos pipeline.

Buffers can therefore solve a number of typical streaming problems but come at
the cost of weakening the delivery guarantees of your pipeline. Common problems
that might warrant use of a buffer are:

- Input sources can periodically spike beyond the capacity of your output sinks.
- You want to use parallel [processing pipelines](../pipeline.md).
- You have more outputs than inputs and wish to distribute messages across them
  in order to maximize overall throughput.
- Your input source needs occasional protection against back pressure from your
  sink, e.g. during restarts. Please keep in mind that all buffers have an
  eventual limit.

If you believe that a problem you have would be solved by a buffer the next step
is to choose an implementation based on the throughput and delivery guarantees
you need. In order to help here are some simplified tables outlining the
different options and their qualities:

#### Performance

| Type      | Throughput | Consumers | Capacity |
| --------- | ---------- | --------- | -------- |
| Memory    | Highest    | Parallel  | RAM      |

#### Delivery Guarantees

| Event     | Shutdown  | Crash     | Disk Corruption |
| --------- | --------- | --------- | --------------- |
| Memory    | Flushed\* | Lost      | Lost            |

\* Makes a best attempt at flushing the remaining messages before closing
  gracefully.

### Contents

1. [`memory`](#memory)
2. [`none`](#none)

## `memory`

``` yaml
type: memory
memory:
  batch_policy:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 0
    enabled: false
    period: ""
  limit: 524288000
```

The memory buffer stores messages in RAM. During shutdown Benthos will make a
best attempt at flushing all remaining messages before exiting cleanly.

This buffer has a configurable limit, where consumption will be stopped with
back pressure upstream if the total size of messages in the buffer reaches this
amount. Since this calculation is only an estimate, and the real size of
messages in RAM is always higher, it is recommended to set the limit
significantly below the amount of RAM available.

### Batching

It is possible to batch up messages sent from this buffer using a
[batch policy](../batching.md#batch-policy).

This is a more powerful way of batching messages than the
[`batch`](../processors/README.md#batch) processor, as it does not
rely on new messages entering the pipeline in order to trigger the conditions.

## `none`

``` yaml
type: none
none: {}
```

Selecting no buffer (default) means the output layer is directly coupled with
the input layer. This is the safest and lowest latency option since
acknowledgements from at-least-once protocols can be propagated all the way from
the output protocol to the input protocol.

If the output layer is hit with back pressure it will propagate all the way to
the input layer, and further up the data stream. If you need to relieve your
pipeline of this back pressure consider using a more robust buffering solution
such as Kafka before resorting to alternatives.
