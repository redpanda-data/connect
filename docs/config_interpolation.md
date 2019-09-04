String Interpolation in Configs
===============================

Benthos is able to perform string interpolation on your config files. There are
two types of expression for this; functions and environment variables.

Environment variables are resolved and interpolated into the config only once at
start up.

Functions are resolved each time they are used. However, only certain fields in
a config will actually support and interpolate these expressions
(`insert_part.contents`, for example). If you aren't sure that a field in a
config section supports functions you should read its respective documentation.

## Environment Variables

You can use environment variables to replace Benthos config values using
`${variable-name}` or `${variable-name:default-value}` syntax. A good example of
this is the [environment variable config][env_var_config], which creates
environment variables for each default field value in a standard
single-in-single-out bridge config.

### Escaping

If a literal string is required that matches this pattern (`${foo}`) you can
escape it with double brackets. For example, the string `${{foo}}` is read as
the literal `${foo}`.

## Example

Let's say you plan to bridge a Kafka deployment to a RabbitMQ exchange but we
want to resolve the addresses of these respective services after deployment
using environment variables. In this case we can replace the broker list in a
Kafka config section with an environment variable, and do the same with the
RabbitMQ URL:

``` yaml
input:
  type: kafka_balanced
  kafka_balanced:
    addresses:
    - ${KAFKA_BROKERS}
    consumer_group: benthos_bridge_consumer
    topics:
    - haha_business
output:
  type: amqp
  amqp:
    url: amqp://${RABBITMQ}/
    exchange: kafka_bridge
    exchange_type: direct
    key: benthos-key
```

We can now write multiple brokers into `KAFKA_BROKERS` by separating them with
commas, Benthos will automatically split them. We can now run with our
environment variables:

``` sh
KAFKA_BROKERS="foo:9092,bar:9092" \
	RABBITMQ="baz:5672" \
	benthos -c ./our_config.yaml
```

## Functions

Some string fields within a Benthos config support function interpolations,
these are context specific functions that are executed every time the string is
used. To find out if a field supports function interpolation refer to its
documentation.

The syntax for functions is `${!function-name}`, or `${!function-name:arg}` if
the function takes an argument, where `function-name` should be replaced with a
valid function.

If a literal string is required that matches this pattern (`${!foo}`) then,
similarly to environment variables, you can escape it with double brackets. For
example, the string `${{!foo}}` would be read as the literal `${!foo}`.

Benthos supports the following functions:

### `content`

Resolves to the content of a message part. The message referred to will depend
on the context of where the function is called.

When applied to a batch of message parts this function targets the first message
part by default. It is possible to specify a target part index with an integer
argument, e.g. `${!content:2}` would print the contents of the third message
part.

### `error`

If an error has occurred during the processing of a message part this function
resolves to the reported cause of the error. For more information about error
handling patterns read [here](./error_handling.md).

When applied to a batch of message parts this function targets the first message
part by default. It is possible to specify a target part index with an integer
argument, e.g. `${!error:2}` would print the error of the third message part.

### `batch_size`

Resolves to the size of a message batch.

### `json_field`

Resolves to the value of a JSON field within the message payload located by a
[dot path](field_paths.md) specified as an argument. The message referred to
will depend on the context of where the function is called. With a message
containing `{"foo":{"bar":"hello world"}}` the function `${!json_field:foo.bar}`
would resolve to `hello world`.

When applied to a batch of message parts this function targets the first message
part by default. It is possible to specify a target part by following the path
with a comma and part number, e.g. `${!json_field:foo.bar,2}` would target the
field `foo.bar` within the third message part in the batch.

### `metadata`

Resolves to the value of a metadata key within the message payload. The message
referred to will depend on the context of where the function is called.
If a message contains the metadata key/value pair `foo: bar` the function
`${!metadata:foo}` would resolve to `bar`.

When applied to a batch of message parts this function targets the first message
part by default. It is possible to specify a target part by following the key
with a comma and part number, e.g. `${!metadata:foo,2}` would target the
key `foo` within the third message part in the batch.

Message metadata can be modified using the
[metadata processor](./processors/README.md#metadata).

### `metadata_json_object`

Resolves to all metadata key/value pairs of a payload as a JSON object. The
message referred to will depend on the context of where the function is called.

When applied to a batch of message parts this function targets the first message
part by default. It is possible to specify a target part with an integer
argument e.g. `${!metadata_json_object:2}` would target the metadata of the
third message part in the batch.

Message metadata can be modified using the
[metadata processor](./processors/README.md#metadata).

### `uuid_v4`

Generates a new RFC-4122 UUID each time it is invoked and prints a string
representation.

### `timestamp_unix_nano`

Resolves to the current unix timestamp in nanoseconds. E.g.
`foo ${!timestamp_unix_nano} bar` prints `foo 1517412152475689615 bar`.

### `timestamp_unix`

Resolves to the current unix timestamp in seconds. E.g.
`foo ${!timestamp_unix} bar` prints `foo 1517412152 bar`. You can add fractional
precision up to the nanosecond by specifying the precision as an argument, e.g.
`${!timestamp_unix:3}` for millisecond precision.

### `timestamp`

Prints the current time in a custom format specified by the argument. The format
is defined by showing how the reference time, defined to be
`Mon Jan 2 15:04:05 -0700 MST 2006` would be displayed if it were the value.

A fractional second is represented by adding a period and zeros to the end of
the seconds section of layout string, as in `15:04:05.000` to format a time
stamp with millisecond precision.

### `timestamp_utc`

The equivalent of `timestamp` except the time is printed as UTC instead of the
local timezone.

### `count`

The `count` function is a counter starting at 1 which increments after each time
it is called. Count takes an argument which is an identifier for the counter,
allowing you to specify multiple unique counters in your configuration.

### `hostname`

Resolves to the hostname of the machine running Benthos. E.g.
`foo ${!hostname} bar` might resolve to `foo glados bar`.

[env_var_config]: https://github.com/Jeffail/benthos/blob/master/config/env/default.yaml