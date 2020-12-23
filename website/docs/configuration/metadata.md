---
title: Metadata
---

In Benthos each message has raw contents and metadata, which is a map of key/value pairs representing an arbitrary amount of complementary data.

When an input protocol supports attributes or metadata they will automatically be added to your messages, refer to the respective input documentation for a list of metadata keys. When an output supports attributes or metadata any metadata key/value pairs in a message will be sent (subject to service limits).

## Editing Metadata

Benthos allows you to add and remove metadata using the [`bloblang` processor][processors.bloblang]. For example, you can do something like this in your pipeline:

```yaml
pipeline:
  processors:
  - bloblang: |
      # Remove all existing metadata from messages
      meta = deleted()

      # Add a new metadata field `time` from the contents of a JSON
      # field `event.timestamp`
      meta time = event.timestamp
```

You can also use [Bloblang][guides.bloblang] to delete individual metadata keys with:

```coffee
meta foo = deleted()
```

Or do more interesting things like remove all metadata keys with a certain prefix:

```coffee
meta = meta().filter(!this.key.has_prefix("kafka_"))
```

## Using Metadata

Metadata values can be referenced in any field that supports [interpolation functions][interpolation]. For example, you can route messages to Kafka topics using interpolation of metadata keys:

```yaml
output:
  kafka:
    addresses: [ TODO ]
    topic: ${! meta("target_topic") }
```

Benthos also allows you to conditionally process messages based on their metadata with the [`switch` processor][processors.switch]:

```yaml
pipeline:
  processors:
  - switch:
    - check: meta("doc_type") == "nested"
      processors:
        - sql:
            driver: mysql
            dsn: foouser:foopassword@tcp(localhost:3306)/foodb
            query: "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
            args:
            - ${! json("document.foo") }
            - ${! json("document.bar") }
            - ${! meta("kafka_topic") }
```

Or, for more complex branches it might be best to use the [`awk` processor][processors.awk].

[interpolation]: /docs/configuration/interpolation
[processors.switch]: /docs/components/processors/switch
[processors.awk]: /docs/components/processors/awk
[processors.bloblang]: /docs/components/processors/bloblang
[guides.bloblang]: /docs/guides/bloblang/about
