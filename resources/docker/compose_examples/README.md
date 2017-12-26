Docker Compose Examples
=======================

This directory contains examples of using Benthos with [docker compose][0]. Each
instance of Benthos is configured using environment variables.

## HTTP To Kafka

[This config][1] sets up a Kafka broker and a Benthos instance that accepts HTTP
POST requests and sends the contents into a Kafka topic.

The HTTP port in the Benthos container is exposed, you can therefore send
messages through curl:

``` sh
curl http://<docker_host>:8080/post -d "example message"
```

## HTTP To Rabbit MQ

[This config][2] is the same as the previous except messages are sent to a
Rabbit MQ instance instead. You can send messages on port 8080 using curl the
same way.

## Kafka To Rabbit MQ

[This config][3] sets up a Kafka broker, a Rabbit MQ node, and a Benthos that
reads messages from the former and sends them through the latter.

[0]: https://docs.docker.com/compose/
[1]: ./compose-http-to-kafka.yaml
[2]: ./compose-http-to-rabbitmq.yaml
[3]: ./compose-kafka-to-rabbitmq.yaml
