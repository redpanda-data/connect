package service_test

import (
	"context"

	"github.com/Jeffail/benthos/v3/public/x/service"
)

// This example demonstrates how to use a stream builder to programmatically
// assemble a stream of Benthos components, and then execute it.
func Example_streamBuilderConfig() {
	builder := service.NewStreamBuilder()

	if err := builder.AddInputYAML(`
generate:
  count: 1
  interval: 1ms
  mapping: 'root = "hello world"'
`); err != nil {
		panic(err)
	}

	if err := builder.AddProcessorYAML(`bloblang: 'root = content().uppercase()'`); err != nil {
		panic(err)
	}

	if err := builder.AddOutputYAML(`stdout: {}`); err != nil {
		panic(err)
	}

	// Build a stream with our configured components.
	stream, err := builder.Build()
	if err != nil {
		panic(err)
	}

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	if err := stream.Run(context.Background()); err != nil {
		panic(err)
	}

	// Output: HELLO WORLD
}
