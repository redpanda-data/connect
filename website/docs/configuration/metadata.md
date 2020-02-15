---
title: Metadata
---

In Benthos each message has raw contents and metadata, which is a map of key/value pairs representing an arbitrary amount of complementary data.

When an input protocol supports attributes or metadata they will automatically be added to your messages, refer to the respective input documentation for a list of metadata keys. When an output supports attributes or metadata any metadata key/value pairs in a message will be sent (subject to service limits).

## Editing Metadata

Benthos allows you to add and remove metadata using the [`metadata` processor][proc-meta]. For example, you can do something like this in your pipeline:

```yaml
pipeline:
  processors:
  # Remove all existing metadata from messages
  - metadata:
      operator: delete_all

  # Add a new metadata field `time` from the contents of a JSON
  # field `event.timestamp`
  - metadata:
      operator: set
      key: time
      value: ${!json_field:event.timestamp}
```

If there are lots of metadata fields you want to copy out of the payload then listing several `metadata` processors might be a bit of a pain. In that case you can shorten your config with the [`awk` processor][proc-awk], which provides a `metadata_set` function:

```yaml
pipeline:
  processors:
  - awk:
      program: |
        {
          metadata_set("time", json_get("event.time"));
          metadata_set("title", json_get("event.title"));
          metadata_set("host", json_get("event.host"));
        }
```

## Using Metadata

Metadata values can be referenced in any field that supports [interpolation functions][interpolation]. For example, you can route messages to Kafka topics using interpolation of metadata keys:

```yaml
output:
  kafka:
    addresses: [ TODO ]
    topic: ${!metadata:target_topic}
```

Benthos also allows you to conditionally process messages based on their metadata with the [`metadata` condition][cond-meta] and processors such as [`switch`][proc-switch]:

```yaml
pipeline:
  processors:
  - switch:
    - condition:
        metadata:
          key: doc_type
          operator: equals
          arg: nested
      processors:
      - json:
          operator: select
          path: nested.value
```

Or, for more complex branches it might be best to use the [`awk` processor][proc-awk].

[proc-meta]: /docs/components/processors/metadata
[proc-switch]: /docs/components/processors/switch
[cond-meta]: /docs/components/conditions/metadata
[proc-awk]: /docs/components/processors/awk
[interpolation]: /docs/configuration/interpolation