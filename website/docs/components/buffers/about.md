---
title: Buffers
sidebar_label: About
---

Benthos uses a transaction based model for guaranteeing delivery of messages without the need for a buffer. This ensures that messages are never acknowledged from a source until the message has left the target sink.

However, sometimes the transaction model is undesired, in which case there are a range of buffer options available which decouple input sources from the rest of the Benthos pipeline.

Buffers can therefore solve a number of typical streaming problems but come at the cost of weakening the delivery guarantees of your pipeline. Common problems that might warrant use of a buffer are:

- Input sources can periodically spike beyond the capacity of your output sinks.
- Your input source needs occasional protection against back pressure from your sink, e.g. during restarts. Please keep in mind that all buffers have an eventual limit.

If you believe that a problem you have would be solved by a buffer the next step is to choose an implementation based on the throughput and delivery guarantees you need. In order to help here are some simplified tables outlining the different options and their qualities:

#### Performance

| Type      | Throughput | Consumers | Capacity |
| --------- | ---------- | --------- | -------- |
| Memory    | Highest    | Parallel  | RAM      |

#### Delivery Guarantees

| Event     | Shutdown  | Crash     | Disk Corruption |
| --------- | --------- | --------- | --------------- |
| Memory    | Flushed\* | Lost      | Lost            |

\* Makes a best attempt at flushing the remaining messages before closing gracefully.
