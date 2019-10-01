Metric Target Types
===================

This document was generated with `benthos --list-metrics`

A metrics type represents a destination for Benthos metrics to be aggregated
such as Statsd, Prometheus, or for debugging purposes an HTTP endpoint that
exposes a JSON object of metrics.

A metrics config section looks like this:

``` yaml
metrics:
  type: statsd
  statsd:
    prefix: foo
    address: localhost:8125
    flush_period: 100ms
    network: udp
```

Benthos exposes lots of metrics and their paths will depend on your pipeline
configuration. However, there are some critical metrics that will always be
present that are outlined in [this document](paths.md).

## `blacklist`

``` yaml
type: blacklist
blacklist:
  child: {}
  paths: []
  patterns: []
```

Blacklist metric paths within Benthos so that they are not aggregated by a child
metric target.

Blacklists can either be path prefixes or regular expression patterns, if either
a path prefix or regular expression matches a metric path it will be excluded.

Metrics must be matched using dot notation even if the chosen output uses a
different form. For example, the path would be 'foo.bar' rather than 'foo_bar'
even when sending metrics to Prometheus.

### Paths

An entry in the `paths` field will check using prefix matching. This
can be used, for example, to allow none of the child specific metrics paths from
an output broker with the path `output.broker`.

### Patterns

An entry in the `patterns` field will be parsed as an RE2 regular
expression and tested against each metric path. This can be used, for example,
to allow none of the latency based metrics with the pattern
`.*\.latency`.

### Debugging

In order to see logs breaking down which metrics are registered and whether they
are blocked by your blacklists enable logging at the TRACE level.

## `http_server`

``` yaml
type: http_server
http_server:
  prefix: benthos
```

It is possible to expose metrics without an aggregator service by having Benthos
serve them as a JSON object at the endpoints `/stats` and `/metrics`.
This is useful for quickly debugging a pipeline.

The object takes the form of a hierarchical representation of the dot paths for
each metric combined. So, for example, if Benthos exposed two metric counters
`foo.bar` and `bar.baz` then the resulting object might look like
this:

``` json
{
	"foo": {
		"bar": 9
	},
	"bar": {
		"baz": 3
	}
}
```

## `prometheus`

``` yaml
type: prometheus
prometheus:
  prefix: benthos
  push_interval: ""
  push_job_name: benthos_push
  push_url: ""
```

Host endpoints (`/metrics` and `/stats`) for Prometheus scraping.
Metrics paths will differ from [the list](paths.md) in that dot separators will
instead be underscores.

### Push Gateway

The field `push_url` is optional and when set will trigger a push of
metrics to a [Prometheus Push Gateway](https://prometheus.io/docs/instrumenting/pushing/)
once Benthos shuts down. It is also possible to specify a
`push_interval` which results in periodic pushes.

The Push Gateway This is useful for when Benthos instances are short lived. Do
not include the "/metrics/jobs/..." path in the push URL.

## `rename`

``` yaml
type: rename
rename:
  by_regexp: []
  child: {}
```

Rename metric paths as they are registered.

Metrics must be matched using dot notation even if the chosen output uses a
different form. For example, the path would be 'foo.bar' rather than 'foo_bar'
even when sending metrics to Prometheus.

### `by_regexp`

An array of objects of the form:

```yaml
  - pattern: "foo\\.([a-z]*)\\.([a-z]*)"
    value: "foo.$1"
    to_label:
      bar: $2
```

Where each pattern will be parsed as an RE2 regular expression, these
expressions are tested against each metric path, where all occurrences will be
replaced with the specified value. Inside the value $ signs are interpreted as
submatch expansions, e.g. $1 represents the first submatch.

The field `to_label` may contain any number of key/value pairs to be
added to a metric as labels, where the value may contain submatches from the
provided pattern. This allows you to extract (left-most) matched segments of the
renamed path into the label values.

For example, in order to replace the paths 'foo.bar.0.zap' and 'foo.baz.1.zap'
with 'zip.bar' and 'zip.baz' respectively, and store the respective values '0'
and '1' under the label key 'index' we could use this config:

```yaml
rename:
  by_regexp:
  - pattern: "foo\\.([a-z]*)\\.([a-z]*)\\.zap"
    value: "zip.$1"
    to_label:
      index: $2
```

These labels will only be injected into metrics registered without pre-existing
labels. Therefore it's currently not possible to combine labels registered from
the [`metric` processor](../processors/README.md#metric) with labels
set via renaming.

### Debugging

In order to see logs breaking down which metrics are registered and whether they
are renamed enable logging at the TRACE level.

## `statsd`

``` yaml
type: statsd
statsd:
  address: localhost:4040
  flush_period: 100ms
  network: udp
  prefix: benthos
```

Push metrics over a TCP or UDP connection using the
[StatsD protocol](https://github.com/statsd/statsd).

## `stdout`

``` yaml
type: stdout
stdout:
  flush_metrics: false
  push_interval: ""
  static_fields:
    '@service': benthos
```

EXPERIMENTAL: This component is considered experimental and is therefore subject
to change outside of major version releases.

It is possible to expose metrics without an aggregator service while running in
serverless mode by having Benthos output metrics as JSON objects to stdout.  This
is useful if you do not have Prometheus or Statsd endpoints and you cannot query
the Benthos API for statistics (due to the short lived nature of serverless
invocations).

A series of JSON objects are emitted (one per line) grouped by the
input/processor/output instance.  Separation into individual JSON objects instead
of a single monolithic object allows for easy ingestion into document stores such
as Elasticsearch.

If defined, metrics are pushed at the configured push_interval, otherwise they
are emitted when Benthos closes.

flush_metrics dictates whether counter and timing metrics are reset to 0 after
they are pushed out.


## `whitelist`

``` yaml
type: whitelist
whitelist:
  child: {}
  paths: []
  patterns: []
```

Whitelist metric paths within Benthos so that only matching metric paths are
aggregated by a child metric target.

Whitelists can either be path prefixes or regular expression patterns, if either
a path prefix or regular expression matches a metric path it will be included.

Metrics must be matched using dot notation even if the chosen output uses a
different form. For example, the path would be 'foo.bar' rather than 'foo_bar'
even when sending metrics to Prometheus.

### Paths

An entry in the `paths` field will check using prefix matching. This
can be used, for example, to allow the child specific metrics paths from an
output broker with the path `output.broker`.

### Patterns

An entry in the `patterns` field will be parsed as an RE2 regular
expression and tested against each metric path. This can be used, for example,
to allow all latency based metrics with the pattern `.*\.latency`.

### Debugging

In order to see logs breaking down which metrics are registered and whether they
pass your whitelists enable logging at the TRACE level.
