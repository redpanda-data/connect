---
title: metadata
type: processor
---

```yaml
metadata:
  key: example
  operator: set
  parts: []
  value: ${!hostname}
```

Performs operations on the metadata of a message. Metadata are key/value pairs
that are associated with message parts of a batch. Metadata values can be
referred to using configuration
[interpolation functions](/docs/configuration/interpolation#metadata),
which allow you to set fields in certain outputs using these dynamic values.

This processor will interpolate functions within both the
`key` and `value` fields, you can find a list of functions
[here](/docs/configuration/interpolation#functions). This allows you to set the
contents of a metadata field using values taken from the message payload.

Value interpolations are resolved once per batch. In order to resolve them per
message of a batch place it within a [`for_each`](for_each)
processor:

``` yaml
for_each:
- metadata:
    operator: set
    key: foo
    value: ${!json_field:document.foo}
```

### Operators

#### `set`

Sets the value of a metadata key.

#### `delete`

Removes all metadata values from the message where the key matches the value
provided. If the value field is left empty the key value will instead be used.

#### `delete_all`

Removes all metadata values from the message.

#### `delete_prefix`

Removes all metadata values from the message where the key is prefixed with the
value provided. If the value field is left empty the key value will instead be
used as the prefix.


