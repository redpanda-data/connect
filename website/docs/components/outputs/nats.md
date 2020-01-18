---
title: nats
type: output
---

```yaml
nats:
  max_in_flight: 1
  subject: benthos_messages
  urls:
  - nats://127.0.0.1:4222
```

Publish to an NATS subject. NATS is at-most-once, so delivery is not guaranteed.
For at-least-once behaviour with NATS look at NATS Stream.

This output will interpolate functions within the subject field, you
can find a list of functions [here](/docs/configuration/interpolation#functions).

This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field `max_in_flight`.


