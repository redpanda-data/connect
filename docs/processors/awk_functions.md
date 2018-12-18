AWK Functions
=============

The Benthos [AWK processor][awk-processor] comes with custom functions listed in
this document. These functions can be overridden by functions in the program.

## Contents

1. [Date Functions](#date-functions)

## Date Functions

### `timestamp_unix()`

Returns the current unix timestamp (the number of seconds since 01-01-1970).

### `timestamp_unix(date)`

Attempts to parse a date string by detecting its format and returns the
equivalent unix timestamp (the number of seconds since 01-01-1970).

### `timestamp_unix(date, format)`

Attempts to parse a date string according to a format and returns the equivalent
unix timestamp (the number of seconds since 01-01-1970).

The format is defined by showing how the reference time, defined to be
`Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.

### `timestamp_unix_nano()`

Returns the current unix timestamp in nanoseconds (the number of nanoseconds
since 01-01-1970).

### `timestamp_unix_nano(date)`

Attempts to parse a date string by detecting its format and returns the
equivalent unix timestamp in nanoseconds (the number of nanoseconds since
01-01-1970).

### `timestamp_unix_nano(date, format)`

Attempts to parse a date string according to a format and returns the equivalent
unix timestamp in nanoseconds (the number of nanoseconds since 01-01-1970).

The format is defined by showing how the reference time, defined to be
`Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.

[awk-processor]: ./README.md#awk
