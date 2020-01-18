---
title: log
type: processor
---

```yaml
log:
  fields: {}
  level: INFO
  message: ""
```

Log is a processor that prints a log event each time it processes a batch. The
batch is then sent onwards unchanged. The log message can be set using function
interpolations described [here](/docs/configuration/interpolation#functions) which
allows you to log the contents and metadata of a messages within a batch.

In order to print a log message per message of a batch place it within a
[`for_each`](for_each) processor.

For example, if we wished to create a debug log event for each message in a
pipeline in order to expose the JSON field `foo.bar` as well as the
metadata field `kafka_partition` we can achieve that with the
following config:

``` yaml
for_each:
- log:
    level: DEBUG
    message: "field: ${!json_field:foo.bar}, part: ${!metadata:kafka_partition}"
```

The `level` field determines the log level of the printed events and
can be any of the following values: TRACE, DEBUG, INFO, WARN, ERROR.

### Structured Fields

It's also possible to output a map of structured fields, this only works when
the service log is set to output as JSON. The field values are function
interpolated, meaning it's possible to output structured fields containing
message contents and metadata, e.g.:

``` yaml
log:
  level: DEBUG
  message: "foo"
  fields:
    id: "${!json_field:id}"
    kafka_topic: "${!metadata:kafka_topic}"
```


