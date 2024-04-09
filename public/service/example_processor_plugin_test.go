package service_test

import (
	"bytes"
	"context"

	"github.com/benthosdev/benthos/v4/public/service"

	// Import only required Benthos components, switch with `components/all` for
	// all standard components.
	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

type ReverseProcessor struct {
	logger *service.Logger
}

func (r *ReverseProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	bytesContent, err := m.AsBytes()
	if err != nil {
		return nil, err
	}

	newBytes := make([]byte, len(bytesContent))
	for i, b := range bytesContent {
		newBytes[len(newBytes)-i-1] = b
	}

	if bytes.Equal(newBytes, bytesContent) {
		r.logger.Infof("Woah! This is like totally a palindrome: %s", bytesContent)
	}

	m.SetBytes(newBytes)
	return []*service.Message{m}, nil
}

func (r *ReverseProcessor) Close(ctx context.Context) error {
	return nil
}

// This example demonstrates how to create a processor plugin. This example is
// for an implementation that does not require any configuration parameters, and
// therefore doesn't defined any within the configuration specification.
func Example_processorPlugin() {
	// Register our new processor, which doesn't require a config schema.
	err := service.RegisterProcessor(
		"reverse", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return &ReverseProcessor{logger: mgr.Logger()}, nil
		})
	if err != nil {
		panic(err)
	}

	// Build a Benthos stream that uses our new output type.
	builder := service.NewStreamBuilder()

	// Set the full Benthos configuration of the stream.
	err = builder.SetYAML(`
input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root = "hello world"'

pipeline:
  processors:
    - reverse: {}

output:
  stdout: {}

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

	// Output: dlrow olleh
}
