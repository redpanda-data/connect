---
title: gcp_pubsub
type: output
---

```yaml
gcp_pubsub:
  max_in_flight: 1
  project: ""
  topic: ""
```

Sends messages to a GCP Cloud Pub/Sub topic. Metadata from messages are sent as
attributes.

This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field `max_in_flight`.


