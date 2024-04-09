package service_test

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"

	// Import only pure Benthos components, switch with `components/all` for all
	// standard components.
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

type BlueOutput struct{}

func (b *BlueOutput) Connect(ctx context.Context) error {
	return nil
}

func (b *BlueOutput) Write(ctx context.Context, msg *service.Message) error {
	content, err := msg.AsBytes()
	if err != nil {
		return err
	}
	fmt.Printf("\033[01;34m%s\033[m\n", content)
	return nil
}

func (b *BlueOutput) Close(ctx context.Context) error {
	return nil
}

// This example demonstrates how to create an output plugin. This example is for
// an implementation that does not require any configuration parameters, and
// therefore doesn't defined any within the configuration specification.
func Example_outputPlugin() {
	// Register our new output, which doesn't require a config schema.
	err := service.RegisterOutput(
		"blue_stdout", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return &BlueOutput{}, 1, nil
		})
	if err != nil {
		panic(err)
	}

	// Use the stream builder API to create a Benthos stream that uses our new
	// output type.
	builder := service.NewStreamBuilder()

	// Set the full Benthos configuration of the stream.
	err = builder.SetYAML(`
input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = "hello world"'

output:
  blue_stdout: {}

logger:
  level: off
`)
	if err != nil {
		panic(err)
	}

	// Build a stream with our configured components.
	stream, err := builder.Build()
	if err != nil {
		panic(err)
	}

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	if err = stream.Run(context.Background()); err != nil {
		panic(err)
	}

	// Output: [01;34mhello world[m
}
