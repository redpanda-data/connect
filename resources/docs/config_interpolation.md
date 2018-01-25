String Interpolation in Configs
===============================

Benthos is able to perform string interpolation on your config files. There are
two types of expression for this; functions and environment variables.

Environment variables are resolved and interpolated into the config only once at
start up.

Functions are resolved each time they are used. However, only certain fields in
a config will actually support and interpolate these expressions (
`prepend_part.contents`, for example). If you aren't sure that a field in a
config section supports functions you should read its respective documentation.

## Environment Variables

You can use environment variables to replace Benthos config values using
`${variable-name:default-value}` syntax. A good example of this is the
[default environment variable config](../../config/env/default.yaml), which
creates environment variables for each default field value in a standard
single-in-single-out bridge config.

## Example

Let's say you plan to bridge a Kafka deployment to a RabbitMQ exchange. You are
able to write a short config for this:

``` yaml
input:
  type: kafka_balanced
  kafka_balanced:
    addresses:
    - localhost:9092
    consumer_group: benthos_bridge_consumer
    topics:
    - haha_business
output:
  type: amqp
  amqp:
    url: amqp://localhost:5672/
    exchange: kafka_bridge
    exchange_type: direct
    key: benthos-key
```

But we might not know the addresses of our Kafka brokers until deployment, and
our deployment method makes it much easier to specify them through environment
variables than by generating config files. Let's replace the broker list with
an environment variable, and do the same with the RabbitMQ URL:

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

Which would run with a configuration equivalent to:

``` yaml
input:
  type: kafka_balanced
  kafka_balanced:
    addresses:
    - "foo:9092,bar:9092"
    consumer_group: benthos_bridge_consumer
    topics:
    - haha_business
output:
  type: amqp
  amqp:
    url: amqp://baz:5672/
    exchange: kafka_bridge
    exchange_type: direct
    key: benthos-key
```

## Functions

The syntax for functions is `${!function_name}`, where `function_name` should
be replaced with one of the following function names:

### `timestamp_unix`

The `timestamp_unix` function simply resolves to the current timestamp in
seconds. E.g. `foo ${!timestamp_unix} bar` might resolve to
`foo 1516899731 bar`.

### `hostname`

The `hostname` function resolves to the hostname of the machine running Benthos.
E.g. `foo ${!hostname} bar` might resolve to `foo glados bar`.

