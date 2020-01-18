---
title: Field Paths
---

Many components within Benthos allow you to target certain fields using a JSON dot path. The syntax of a path within Benthos is similar to [JSON Pointers][json-pointers], except with dot separators instead of slashes (and no leading dot.) When a path is used to set a value any path segment that does not yet exist in the structure is created as an object.

For example, if we had the following JSON structure:

```json
{
  "foo": {
    "bar": 21
  }
}
```

The query path `foo.bar` would return `21`.

The characters `~` (%x7E) and `.` (%x2E) have special meaning in Benthos paths. Therefore `~` needs to be encoded as `~0` and `.` needs to be encoded as `~1` when these characters appear within a key.

For example, if we had the following JSON structure:

```json
{
  "foo.foo": {
    "bar~bo": {
      "": {
        "baz": 22
      }
    }
  }
}
```

The query path `foo~1foo.bar~0bo..baz` would return `22`.

## Arrays

When Benthos encounters an array whilst traversing a JSON structure it requires the next path segment to be either an integer of an existing index, or, depending on whether the path is used to query or set the target value, the character `*` or `-` respectively.

For example, if we had the following JSON structure:

```json
{
  "foo": [
    0, 1, { "bar": 23 }
  ]
}
```

The query path `foo.2.bar` would return `23`.

### Querying

When a query reaches an array the character `*` indicates that the query should return the value of the remaining path from each element of the array (within an array.)

### Setting

When an array is reached the character `-` indicates that a new element should be appended to the end of the existing elements, if this character is not the final segment of the path then an object is created.

[json-pointers]: https://tools.ietf.org/html/rfc6901