Using Environment Variables
===========================

You can use environment variables to replace Benthos config values using
`${variable-name:default-value}` syntax. A good example of this is the
[default environment variable config][../../config/env/default.yaml], which
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
