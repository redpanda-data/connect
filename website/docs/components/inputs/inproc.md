---
title: inproc
type: input
---

```yaml
inproc: ""
```

Directly connect to an output within a Benthos process by referencing it by a
chosen ID. This allows you to hook up isolated streams whilst running Benthos in
[`--streams` mode](/docs/guides/streams_mode/about), it is NOT
recommended that you connect the inputs of a stream with an output of the same
stream, as feedback loops can lead to deadlocks in your message flow.

It is possible to connect multiple inputs to the same inproc ID, but only one
output can connect to an inproc ID, and will replace existing outputs if a
collision occurs.


