---
title: Bloblang Functions
sidebar_label: Functions
description: A list of Bloblang functions
---

Functions can be placed anywhere and allow you to extract information from your environment, generate values, or access data from the underlying message being mapped:

```coffee
doc.id = uuid_v4()
doc.received_at = timestamp_unix()
doc.host = hostname()
```

### `batch_index()`

Returns the index of the mapped message within a batch. This is useful for applying maps only on certain messages of a batch:

```coffee
# Only map the first message of a batch
root = match {
  batch_index() == 0 => this.apply("foo")
}
```

### `batch_size()`

Returns the size of the message batch.

### `content()`

Returns the full raw contents of the mapping target message as a string.

### `count(string)`

The `count` function is a counter starting at 1 which increments after each time it is called. Count takes an argument which is an identifier for the counter, allowing you to specify multiple unique counters in your configuration.

### `deleted()`

This is a special function indicating that the mapping target should be deleted. For example, it can be used to remove elements of an array within `for_each`:

```coffee
new_nums = nums.for_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)

# in:  {"nums":[3,11,4,17]}
# out: {"new_nums":[1,7]}
```

### `error()`

If an error has occurred during the processing of a message this function returns the reported cause of the error. For more information about error
handling patterns read [here][error_handling].

```coffee
doc.error = error()
```

### `hostname()`

Resolves to the hostname of the machine running Benthos.

### `json(string)`

Returns the value of a field within a JSON message located by a [dot path][field_paths] argument. This function always targets the entire source JSON document regardless of the mapping context.

```coffee
mapped = json("foo.bar")

# In:  {"foo":{"bar":"hello world"}}
# Out: {"mapped":"hello world"}
```

The path parameter is optional and if omitted the entire JSON payload is returned.

### `meta(string)`

Returns the value of a metadata key from a message identified by a key. Message metadata can be modified using the [metadata processor][meta_proc].

The parameter is optional and if omitted the entire metadata contents are returned as a JSON object.

### `timestamp(string)`

Prints the current time in a custom format specified by the argument. The format is defined by showing how the reference time, defined to be
`Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.

A fractional second is represented by adding a period and zeros to the end of the seconds section of layout string, as in `15:04:05.000` to format a time stamp with millisecond precision.

### `timestamp_unix(int)`

Resolves to the current unix timestamp in seconds. You can add fractional precision up to the nanosecond by specifying the precision as an argument, e.g. `timestamp_unix(3)` for millisecond precision.

### `timestamp_unix_nano()`

Resolves to the current unix timestamp in nanoseconds.

### `timestamp_utc(string)`

The equivalent of `timestamp` except the time is printed as UTC instead of the local timezone.

### `uuid_v4()`

Generates a new RFC-4122 UUID each time it is invoked and prints a string representation.

[error_handling]: /docs/configuration/error_handling
[field_paths]: /docs/configuration/field_paths
[meta_proc]: /docs/components/processors/metadata