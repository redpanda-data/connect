package service_test

import (
	"bytes"
	"context"

	"github.com/Jeffail/benthos/v3/public/x/service"
)

type ReverseProcessor struct {
	logger *service.Logger
}

func (r *ReverseProcessor) Process(ctx context.Context, m *service.Message) ([]*service.Message, error) {
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
	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return &ReverseProcessor{
			logger: mgr.Logger(),
		}, nil
	}

	err := service.RegisterProcessor("reverse", service.NewConfigSpec(), constructor)
	if err != nil {
		panic(err)
	}

	// And then execute Benthos with:
	// service.RunCLI()
}
