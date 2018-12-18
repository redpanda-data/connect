AWK Functions
=============

The Benthos [AWK processor][awk-processor] comes with custom functions listed in
this document. These functions can be overridden by functions in the program.

## Contents

1. [JSON Functions](#json-functions)
2. [Date Functions](#date-functions)

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

## Date Functions

### `timestamp_unix()`

Returns the current unix timestamp (the number of seconds since 01-01-1970).

### `timestamp_unix(date)`

Attempts to parse a date string by detecting its format and returns the
equivalent unix timestamp (the number of seconds since 01-01-1970).

### `timestamp_unix(date, format)`

Attempts to parse a date string according to a format and returns the equivalent
unix timestamp (the number of seconds since 01-01-1970).

The format is defined by showing how the reference time, defined to be
`Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.

### `timestamp_unix_nano()`

Returns the current unix timestamp in nanoseconds (the number of nanoseconds
since 01-01-1970).

### `timestamp_unix_nano(date)`

Attempts to parse a date string by detecting its format and returns the
equivalent unix timestamp in nanoseconds (the number of nanoseconds since
01-01-1970).

### `timestamp_unix_nano(date, format)`

Attempts to parse a date string according to a format and returns the equivalent
unix timestamp in nanoseconds (the number of nanoseconds since 01-01-1970).

The format is defined by showing how the reference time, defined to be
`Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.

[awk-processor]: ./README.md#awk
