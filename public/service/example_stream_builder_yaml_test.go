package service_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"

	// Import only required Benthos components, switch with `components/all` for
	// all standard components.
	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
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
	err := builder.SetYAML(`
input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = "hello world"'

  processors:
    - mapping: 'root = content().uppercase()'

output:
  stdout: {}

logger:
  level: none
`)
	panicOnErr(err)

	// Build a stream with our configured components.
	stream, err := builder.Build()
	panicOnErr(err)

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	err = stream.Run(context.Background())
	panicOnErr(err)

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

	err := builder.AddInputYAML(`
generate:
  count: 1
  interval: 1ms
  mapping: 'root = "hello world"'
`)
	panicOnErr(err)

	err = builder.AddProcessorYAML(`mapping: 'root = content().uppercase()'`)
	panicOnErr(err)

	err = builder.AddOutputYAML(`stdout: {}`)
	panicOnErr(err)

	err = builder.SetLoggerYAML(`level: off`)
	panicOnErr(err)

	// Build a stream with our configured components.
	stream, err := builder.Build()
	panicOnErr(err)

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	err = stream.Run(context.Background())
	panicOnErr(err)

	// Output: HELLO WORLD
}

// This example demonstrates how to use a stream builder to assemble a
// processing pipeline that you can push messages into and extract via closures.
func Example_streamBuilderPush() {
	panicOnErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	builder := service.NewStreamBuilder()
	err := builder.SetLoggerYAML(`level: off`)
	panicOnErr(err)

	err = builder.AddProcessorYAML(`mapping: 'root = content().uppercase()'`)
	panicOnErr(err)

	err = builder.AddProcessorYAML(`mapping: 'root = "check this out: " + content()'`)
	panicOnErr(err)

	// Obtain a closure func that allows us to push data into the stream, this
	// is treated like any other input, which also means it's possible to use
	// this along with regular configured inputs.
	sendFn, err := builder.AddProducerFunc()
	panicOnErr(err)

	// Define a closure func that receives messages as an output of the stream.
	// It's also possible to use this along with regular configured outputs.
	var outputBuf bytes.Buffer
	err = builder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
		msgBytes, err := m.AsBytes()
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(&outputBuf, "received: %s\n", msgBytes)
		return err
	})
	panicOnErr(err)

	stream, err := builder.Build()
	panicOnErr(err)

	go func() {
		perr := sendFn(context.Background(), service.NewMessage([]byte("hello world")))
		panicOnErr(perr)

		perr = sendFn(context.Background(), service.NewMessage([]byte("I'm pushing data into the stream")))
		panicOnErr(perr)

		perr = stream.StopWithin(time.Second)
		panicOnErr(perr)
	}()

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	err = stream.Run(context.Background())
	panicOnErr(err)

	fmt.Println(outputBuf.String())

	// Output: received: check this out: HELLO WORLD
	// received: check this out: I'M PUSHING DATA INTO THE STREAM
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

	err := builderOne.SetYAML(`
http:
  enabled: false

input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = "hello world one"'

  processors:
    - mapping: 'root = content().uppercase()'

output:
  stdout: {}
`)
	panicOnErr(err)

	streamOne, err := builderOne.Build()
	panicOnErr(err)

	builderTwo := service.NewStreamBuilder()

	err = builderTwo.SetYAML(`
http:
  enabled: false

input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = "hello world two"'

  processors:
    - sleep:
        duration: 500ms
    - mapping: 'root = content().capitalize()'

output:
  stdout: {}
`)
	panicOnErr(err)

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
