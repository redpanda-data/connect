package service_test

import (
	"context"

	"github.com/Jeffail/benthos/v3/public/x/service"
)

// This example demonstrates how to use a stream builder to parse and execute a
// full Benthos config.
func Example_streamBuilderConfig() {
	panicOnErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	builder := service.NewStreamBuilder()

	// Set the full Benthos configuration of the stream.
	panicOnErr(builder.SetYAML(`
input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = "hello world"'

pipeline:
  processors:
    - bloblang: 'root = content().uppercase()'

output:
  stdout: {}
`))

	// Build a stream with our configured components.
	stream, err := builder.Build()
	if err != nil {
		panic(err)
	}

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	panicOnErr(stream.Run(context.Background()))

	// Output: HELLO WORLD
}

// This example demonstrates how to use a stream builder to assemble a stream of
// Benthos components by adding snippets of configs for different component
// types, and then execute it. You can use the Add methods to append any number
// of components to the stream, following fan in and fan out patterns for inputs
// and outputs respectively.
func Example_streamBuilderConfigAddMethods() {
	panicOnErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	builder := service.NewStreamBuilder()

	panicOnErr(builder.AddInputYAML(`
generate:
  count: 1
  interval: 1ms
  mapping: 'root = "hello world"'
`))
	panicOnErr(builder.AddProcessorYAML(`bloblang: 'root = content().uppercase()'`))
	panicOnErr(builder.AddOutputYAML(`stdout: {}`))

	// Build a stream with our configured components.
	stream, err := builder.Build()
	if err != nil {
		panic(err)
	}

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	panicOnErr(stream.Run(context.Background()))

	// Output: HELLO WORLD
}
