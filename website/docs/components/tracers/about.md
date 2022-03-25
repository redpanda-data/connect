---
title: Tracers
sidebar_label: About
---

A tracer type represents a destination for Benthos to send tracing events to such as [Jaeger][jaeger].

When a tracer is configured all messages will be allocated a root span during ingestion that represents their journey through a Benthos pipeline. Many Benthos processors create spans, and so tracing is a great way to analyse the pathways of individual messages as they progress through a Benthos instance.

Some inputs, such as `http_server` and `http_client`, are capable of extracting a root span from the source of the message (HTTP headers). This is
a work in progress and should eventually expand so that all inputs have a way of doing so.

A tracer config section looks like this:

```yaml
tracer:
  jaeger:
    agent_address: localhost:6831
    sampler_type: const
    sampler_param: 1
```

WARNING: Although the configuration spec of this component is stable the format of spans, tags and logs created by Benthos is subject to change as it is tuned for improvement.

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="tracers" singular="tracing target"></ComponentSelect>


[jaeger]: https://www.jaegertracing.io/
