Metric Target Types
===================

This document was generated with `benthos --list-metrics`

A metrics type represents a destination for Benthos metrics to be aggregated
such as Statsd, Prometheus, or for debugging purposes an HTTP endpoint that
exposes a JSON object of metrics.

A metrics config section looks like this:

``` yaml
metrics:
  type: foo
  prefix: benthos
  foo:
    bar: baz
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
prefix: benthos
```

Blacklist metric paths within Benthos so that they are not aggregated by a child
metric target.

Blacklists can either be path prefixes or regular expression patterns, if either
a path prefix or regular expression matches a metric path it will be excluded.

Metrics must be matched using dot notation even if the chosen output uses a
different form. For example, the path would be 'foo.bar' rather than 'foo_bar'
even when sending metrics to Prometheus.

The `prefix` field in a metrics config is ignored by this type. Please
configure a prefix at the child level.

### Paths

An entry in the `paths` field will check using prefix matching. This
can be used, for example, to allow none of the child specific metrics paths from
an output broker with the path `output.broker`.

### Patterns

An entry in the `patterns` field will be parsed as an RE2 regular
expression and tested against each metric path. This can be used, for example,
to allow none of the latency based metrics with the pattern
`.*\.latency`.

## `http_server`

``` yaml
type: http_server
http_server: {}
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
prefix: benthos
prometheus:
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
prefix: benthos
rename:
  by_regexp: []
  child: {}
```

Rename metric paths as they are registered.

Metrics must be matched using dot notation even if the chosen output uses a
different form. For example, the path would be 'foo.bar' rather than 'foo_bar'
even when sending metrics to Prometheus.

The `prefix` field in a metrics config is ignored by this type. Please
configure a prefix at the child level.

### `by_regexp`

An array of objects of the form `{"pattern":"foo","value":"bar"}`
where each pattern will be parsed as RE2 regular expressions, these expressions
are tested against each metric path, where all occurrences will be replaced with
the specified value. Inside the value $ signs are interpreted as submatch
expansions, e.g. $1 represents the first submatch.

To replace the paths 'foo.bar.zap' and 'foo.baz.zap' with 'zip.bar' and
'zip.baz' respectively we could use this config:

``` yaml
type: rename
rename:
  by_regexp:
  - pattern: "foo\\.([a-z]*)\\.zap"
    value: "zip.$1"
```

## `statsd`

``` yaml
type: statsd
prefix: benthos
statsd:
  address: localhost:4040
  flush_period: 100ms
  network: udp
```

Push metrics over a TCP or UDP connection using the
[StatsD protocol](https://github.com/statsd/statsd).

## `whitelist`

``` yaml
type: whitelist
prefix: benthos
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

The `prefix` field in a metrics config is ignored by this type. Please
configure a prefix at the child level.

### Paths

An entry in the `paths` field will check using prefix matching. This
can be used, for example, to allow the child specific metrics paths from an
output broker with the path `output.broker`.

### Patterns

An entry in the `patterns` field will be parsed as an RE2 regular
expression and tested against each metric path. This can be used, for example,
to allow all latency based metrics with the pattern `.*\.latency`.
