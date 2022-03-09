Implementations
===============

This is an internal package containing the implementations of Benthos component types (inputs, processors, outputs, etc) organised into sub categories.

If you intend to create a new component type then use the docs at [https://pkg.go.dev/github.com/benthosdev/benthos/v4/public/service](https://pkg.go.dev/github.com/benthosdev/benthos/v4/public/service), and there are some implementations that might be worth using as a reference:

- Input example: [./nats/input_jetstream.go](./nats/input_jetstream.go)
- Output example: [./nats/output_jetstream.go](./nats/output_jetstream.go)
- Processor example: [./confluent/processor_schema_registry_encode.go](./confluent/processor_schema_registry_encode.go)
- Buffer example: [./generic/buffer_system_window.go](./generic/buffer_system_window.go)
