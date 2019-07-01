Monitoring
==========

Health Checks
-------------

Benthos serves two HTTP endpoints for health checks:

- `/ping` can be used as a liveness probe as it always returns a 200.
- `/ready` can be used as a readiness probe as it serves a 200 only when both the input and output are connected, otherwise a 503 is returned.

Metrics
-------

Benthos [exposes lots of metrics](./metrics/paths.md) either to Statsd, Prometheus or for debugging purposes an HTTP endpoint that returns a JSON formatted object.

The target destination of Benthos metrics is configurable from the [metrics section](./metrics/README.md), where it's also possible to rename, whitelist or blacklist certain metric paths.

Tracing
-------

Benthos also [emits opentracing events](./tracers/README.md) to a tracer of your choice, which can be used to visualise the processors within a pipeline.
