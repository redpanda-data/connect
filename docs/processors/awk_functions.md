AWK Functions
=============

The Benthos [AWK processor][awk-processor] comes with custom functions listed in
this document. These functions can be overridden by functions in the program.

## Contents

1. [JSON Functions](#json-functions)
2. [Metadata Functions](#metadata-functions)
3. [Date Functions](#date-functions)
4. [Logging Functions](#logging-functions)

## JSON Functions

### `json_get(path)`

Attempts to find a JSON value in the input message payload by a
[dot separated path](../field_paths.md) and returns it as a string. This
function is always available even when the `json` codec is not used.

### `json_set(path, value)`

Attempts to set a JSON value in the input message payload identified by a
[dot separated path](../field_paths.md), the value argument will be interpreted
as a string. This function is always available even when the `json` codec is not
used.

In order to set non-string values use one of the following typed varieties:

- `json_set_int(path, value)`
- `json_set_float(path, value)`
- `json_set_bool(path, value)`

### `json_delete(path)`

Attempts to delete a JSON field from the input message payload identified by a
[dot separated path](../field_paths.md). This function is always available even
when the `json` codec is not used.

### `create_json_object(key1, val1, key2, val2, ...)`

Generates a valid JSON object of key value pair arguments. The arguments are
variadic, meaning any number of pairs can be listed. The value will always
resolve to a string regardless of the value type. E.g. the following call:

`create_json_object("a", "1", "b", 2, "c", "3")`

Would result in this string:

`{"a":"1","b":"2","c":"3"}`

### `create_json_array(val1, val2, ...)`

Generates a valid JSON array of value arguments. The arguments are variadic,
meaning any number of values can be listed. The value will always resolve to a
string regardless of the value type. E.g. the following call:

`create_json_array("1", 2, "3")`

Would result in this string:

`["1","2","3"]`

## Metadata Functions

### `metadata_set(key, value)`

Set a metadata key for the message to a value. The value will always resolve to
a string regardless of the value type.

### `metadata_get(key) string`

Get the value of a metadata key from the message.

## Date Functions

### `timestamp_unix() int`

Returns the current unix timestamp (the number of seconds since 01-01-1970).

### `timestamp_unix(date) int`

Attempts to parse a date string by detecting its format and returns the
equivalent unix timestamp (the number of seconds since 01-01-1970).

### `timestamp_unix(date, format) int`

Attempts to parse a date string according to a format and returns the equivalent
unix timestamp (the number of seconds since 01-01-1970).

The format is defined by showing how the reference time, defined to be
`Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.

### `timestamp_unix_nano() int`

Returns the current unix timestamp in nanoseconds (the number of nanoseconds
since 01-01-1970).

### `timestamp_unix_nano(date) int`

Attempts to parse a date string by detecting its format and returns the
equivalent unix timestamp in nanoseconds (the number of nanoseconds since
01-01-1970).

### `timestamp_unix_nano(date, format) int`

Attempts to parse a date string according to a format and returns the equivalent
unix timestamp in nanoseconds (the number of nanoseconds since 01-01-1970).

The format is defined by showing how the reference time, defined to be
`Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.

### `timestamp_format(unix, format) string`

Formats a unix timestamp. The format is defined by showing how the reference
time, defined to be `Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it
were the value.

The format is optional, and if omitted RFC3339 (`2006-01-02T15:04:05Z07:00`)
will be used.

### `timestamp_format_nano(unixNano, format) string`

Formats a unix timestamp in nanoseconds. The format is defined by showing how
the reference time, defined to be `Mon Jan 2 15:04:05 -0700 MST 2006` would be
displayed if it were the value.

The format is optional, and if omitted RFC3339 (`2006-01-02T15:04:05Z07:00`)
will be used.

## Logging Functions

### `print_log(message, level)`

Prints a Benthos log message at a particular log level. The log level is
optional, and if omitted the level `INFO` will be used.

[awk-processor]: ./README.md#awk
