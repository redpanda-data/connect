---
title: Streams Mode
sidebar_label: About
description: Find out about Benthos Streams mode
---

A Benthos stream consists of four components; an input, an optional buffer, processor pipelines and an output. Under normal use a Benthos instance is a single stream, and these components are configured within the service config file.

Alternatively, Benthos can be run in `streams` mode, where a single running Benthos instance is able to run multiple entirely isolated streams. Adding streams in this mode can be done in two ways:

1. [Static configuration files][static-files] allows you to maintain a directory of static stream configuration files that will be traversed by Benthos.

2. An [HTTP REST API][rest-api] allows you to dynamically create, read the status of, update, and delete streams at runtime.

These two methods can be used in combination, i.e. it's possible to update and delete streams that were created with static files.

When running Benthos in streams mode it is still necessary to provide a general service wide configuration with the `-c`/`--config` flag that specifies observability configuration such as the `metrics`, `logger` and `tracing` sections, as well the `http` section for configuring how the HTTP server should behave.

You can import resources either in the general configuration, or using the `-r`/`--resources` flag, the same as when running Benthos in regular mode.

```sh
benthos -r "./prod/*.yaml" -c ./config.yaml streams
```

## HTTP Endpoints

A Benthos config can contain components such as an `http_server` input that register endpoints to the service-wide HTTP server. When these components are created from within a named stream in streams mode the endpoint will be prefixed with the streams identifier by default. For example, a stream with the identifier `foo` and the config:

```yaml
input:
  http_server:
    path: /meow
pipeline:
  processors:
    - mapping: 'root = "meow " + content()'
output:
  sync_response: {}
```

Will register an endpoint `/meow`, which will be prefixed with the name `foo` to become `/foo/meow`. This behaviour is intended to make a clearer distinction between endpoints registered by different streams, and prevent collisions of those endpoints. However, you can disable this behaviour by setting the flag `--prefix-stream-endpoints` to `false` (`benthos streams --prefix-stream-endpoints=false ./streams/*.yaml`).

## Resources

When running Benthos in streams mode [resource components][resources] are shared across all streams. The streams mode HTTP API also provides an endpoint for modifying and adding resource configurations dynamically.

## Metrics

Metrics from all streams are aggregated and exposed via the method specified in [the config][metrics] of the Benthos instance running in `streams` mode, with their metrics enriched with the tag `stream` containing the stream name.

For example, a Benthos instance running in streams mode running a stream named `foo` would have metrics from `foo` registered with the label `stream` with the value of `foo`.

This can cause problems if your streams are short lived and uniquely named as the number of metrics registered will continue to climb indefinitely. In order to avoid this you can use the `mapping` field to filter metric names.

```yaml
# Only register metrics for the stream `foo`. Others will be ignored.
metrics:
  mapping: if meta("stream") != "foo" { deleted() }
  prometheus: {}
```

[static-files]: /docs/guides/streams_mode/using_config_files
[rest-api]: /docs/guides/streams_mode/using_rest_api
[metrics]: /docs/components/metrics/about
[resources]: /docs/configuration/resources
