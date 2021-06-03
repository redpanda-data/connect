package service_test

import (
	"context"
	"sync"

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
	panicOnErr(err)

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
	panicOnErr(err)

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	panicOnErr(stream.Run(context.Background()))

	// Output: HELLO WORLD
}

// This example demonstrates using the stream builder API to create and run two
// independent streams.
func Example_streamBuilderMultipleStreams() {
	panicOnErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	// Build the first stream pipeline. Note that we configure each pipeline
	// with its HTTP server disabled as otherwise we would see a port collision
	// when they both attempt to bind to the default address `0.0.0.0:4195`.
	//
	// Alternatively, we could choose to configure each with their own address
	// with the field `http.address`, or we could call `SetHTTPMux` on the
	// builder in order to explicitly override the configured server.
	builderOne := service.NewStreamBuilder()

	panicOnErr(builderOne.SetYAML(`
http:
  enabled: false

input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = "hello world one"'

pipeline:
  processors:
    - bloblang: 'root = content().uppercase()'

output:
  stdout: {}
`))

	streamOne, err := builderOne.Build()
	panicOnErr(err)

	builderTwo := service.NewStreamBuilder()

	panicOnErr(builderTwo.SetYAML(`
http:
  enabled: false

input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = "hello world two"'

pipeline:
  processors:
    - sleep:
        duration: 500ms
    - bloblang: 'root = content().capitalize()'

output:
  stdout: {}
`))

	streamTwo, err := builderTwo.Build()
	panicOnErr(err)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		panicOnErr(streamOne.Run(context.Background()))
	}()
	go func() {
		defer wg.Done()
		panicOnErr(streamTwo.Run(context.Background()))
	}()

	wg.Wait()

	// Output: HELLO WORLD ONE
	// Hello World Two
}
