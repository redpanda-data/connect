---
title: Interpolation
---

Benthos allows you to dynamically set config fields with environment variables anywhere within a config file using the syntax `${<variable-name>}` (or `${<variable-name>:<default-value>}` in order to specify a default value). This is useful for setting environment specific fields such as addresses:

```yaml
input:
  kafka:
    addresses: [ "${BROKERS}" ]
    consumer_group: benthos_bridge_consumer
    topics: [ "haha_business" ]
```

```sh
BROKERS="foo:9092,bar:9092" benthos -c ./config.yaml
```

If a literal string is required that matches this pattern (`${foo}`) you can escape it with double brackets. For example, the string `${{foo}}` is read as the literal `${foo}`.

## Bloblang Queries

Some Benthos fields also support [Bloblang][bloblang] function interpolations, which are much more powerful expressions that allow you to query the contents of messages and perform arithmetic. The syntax of a function interpolation is `${!<bloblang expression>}`, where the contents are a bloblang query (the right-hand-side of a bloblang map) including a range of [functions][bloblang_functions]. For example, with the following config:

```yaml
output:
  kafka:
    addresses: [ "TODO:6379" ]
    topic: 'dope-${! json("topic") }'
```

A message with the contents `{"topic":"foo","message":"hello world"}` would be routed to the Kafka topic `dope-foo`.

If a literal string is required that matches this pattern (`${!foo}`) then, similar to environment variables, you can escape it with double brackets. For example, the string `${{!foo}}` would be read as the literal `${!foo}`.

Bloblang supports arithmetic, boolean operators, coalesce and mapping expressions. For more in-depth details about the language [check out the docs][bloblang].

## Examples

### Reference Metadata

A common usecase for interpolated functions is dynamic routing at the output level using metadata:

```yaml
output:
  kafka:
    addresses: [ TODO ]
    topic: ${! meta("output_topic") }
    key: ${! meta("key") }
```

### Coalesce and Mapping

Bloblang supports coalesce and mapping, which makes it easy to extract values from slightly varying data structures:

```yaml
pipeline:
  processors:
    - cache:
        resource: foocache
        operator: set
        key: '${! json().message.(foo | bar).id }'
        value: '${! content() }'
```

Here's a map of inputs to resulting values:

```
{"foo":{"a":{"baz":"from_a"},"c":{"baz":"from_c"}}} -> from_a
{"foo":{"b":{"baz":"from_b"},"c":{"baz":"from_c"}}} -> from_b
{"foo":{"b":null,"c":{"baz":"from_c"}}}             -> from_c
```

### Delayed Processing

We have a stream of JSON documents each with a unix timestamp field `doc.received_at` which is set when our platform receives it. We wish to only process messages an hour _after_ they were received. We can achieve this by running the `sleep` processor using an interpolation function to calculate the seconds needed to wait for:

```yaml
pipeline:
  processors:
  - sleep:
      duration: '${! 3600 - ( timestamp_unix() - json("doc.created_at").number() ) }s'
```

If the calculated result is less than or equal to zero the processor does not sleep at all. If the value of `doc.created_at` is a string then our method `.number()` will attempt to parse it into a number.

[error_handling]: /docs/configuration/error_handling
[field_paths]: /docs/configuration/field_paths
[meta_proc]: /docs/components/processors/metadata
[bloblang]: /docs/guides/bloblang/about
[bloblang_functions]: /docs/guides/bloblang/about#functions