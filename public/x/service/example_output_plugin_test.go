package service_test

import (
	"context"
	"fmt"

	"github.com/Jeffail/benthos/v3/public/x/service"
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
	err := service.RegisterOutput(
		"blue_stdout", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return &BlueOutput{}, 1, nil
		})
	if err != nil {
		panic(err)
	}

	// And then execute Benthos with:
	// service.RunCLI()
}
