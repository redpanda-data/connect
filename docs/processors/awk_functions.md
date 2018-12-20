AWK Functions
=============

The Benthos [AWK processor][awk-processor] comes with custom functions listed in
this document. These functions can be overridden by functions in the program.

## Contents

1. [JSON Functions](#json-functions)
2. [Metadata Functions](#metadata-functions)
3. [Date Functions](#date-functions)

## JSON Functions

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

[awk-processor]: ./README.md#awk
