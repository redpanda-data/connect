Schema Registry
===============

This is a neat little example of using a schema registry service with Benthos. Both the Kafka implementation and the schema registry service are being handled with [Redpanda](https://vectorized.io/redpanda/).

Video run through of this demo: [https://youtu.be/HzuqbNw-vMo](https://youtu.be/HzuqbNw-vMo)
More information about schema registry service: [https://docs.confluent.io/platform/current/schema-registry/index.html](https://docs.confluent.io/platform/current/schema-registry/index.html)
How to set up a schema registry with Redpanda: [https://vectorized.io/blog/schema_registry/](https://vectorized.io/blog/schema_registry/)

## Run

```sh
docker-compose up -d
```

## Register initial schema

```sh
./insert_schema.sh
```

## See generated messages

```sh
docker-compose logs -f benthos-out
```
