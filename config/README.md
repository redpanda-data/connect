Config
======

This directory shows some config examples. Some are real world applications, some are examples of [config unit tests][unit-tests].

If you're looking for specific config examples for a use case you have then try generating one with the `redpanda-connect create` subcommand. For example, to create a config that reads Kafka messages, decodes them with a schema registry service, and writes them to NATS JetStream you could use the following command:

```sh
rpk connect create kafka/schema_registry_decode/nats_jetstream > example.yaml
```

[unit-tests]: https://www.docs.redpanda.com/redpanda-connect/docs/configuration/unit_testing
