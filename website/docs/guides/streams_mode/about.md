---
title: Streams Mode
sidebar_label: About
description: Find out about Benthos Streams mode
---

A Benthos stream consists of four components; an input, an optional buffer,
processor pipelines and an output. Under normal use a Benthos instance is a
single stream, and these components are configured within the service config
file.

Alternatively, Benthos can be run in `--streams` mode, where a single running
Benthos instance is able to run multiple entirely isolated streams. Adding
streams in this mode can be done in two ways:

1. [Static configuration files][static-files] allows you to maintain a directory
   of static stream configuration files that will be traversed by Benthos.

2. An [HTTP REST API][rest-api] allows you to dynamically create, read the
   status of, update, and delete streams at runtime.

These two methods can be used in combination, i.e. it's possible to update and
delete streams that were created with static files.

## Resources

The `resource` section of a Benthos config defines named resources (`caches`,
`rate_limits`, etc) that can be referenced throughout a stream configuration.
When running in streams mode these resources are also shared across streams.

An individual stream config should not define any resources. Instead, they
should be defined in a service-wide config:

```yaml
resources:
  caches:
    foo:
      memory:
        ttl: 3600
```

```sh
benthos --streams -c ./config.yaml
```

This means it is not currently possible to modify resources dynamically. If this
is blocking you from using streams mode then consider
[raising a ticket](https://github.com/Jeffail/benthos/issues).

## Metrics

Metrics from all streams are aggregated and exposed via the method specified in
[the config][metrics] of the Benthos instance running in `--streams` mode, with
their metrics prefixed by their respective stream name.

For example, a Benthos instance running in streams mode with the configured
prefix `benthos` running a stream named `foo` would have metrics from `foo`
registered with the prefix `benthos.foo`.

This can cause problems if your streams are short lived and uniquely named as
the number of metrics registered will continue to climb indefinitely. In order
to avoid this you should either limit the streams that register their metrics
with [`whitelist`][whitelist] or [`blacklist`][blacklist] rules:

``` yaml
# Only register metrics for the stream `foo`. Others will be ignored.
metrics:
  whitelist:
    paths:
    - foo
    child:
      type: prometheus
      prefix: benthos
```

Or use [`rename`][rename] rules to set the same prefix for groups of streams:

``` yaml
# Rename all stream metric prefixes of the form `foo_<uuid_v4>` to just `foo`.
metrics:
  rename:
    by_regexp:
    - pattern: "foo_[0-9\\-a-zA-Z]+\\.(.*)"
      value: "foo.$1"
    child:
      type: prometheus
      prefix: benthos
```

[static-files]: using_config_files
[rest-api]: using_rest_api
[metrics]: /docs/components/metrics/about
[whitelist]: /docs/components/metrics/whitelist
[blacklist]: /docs/components/metrics/blacklist
[rename]: /docs/components/metrics/rename
