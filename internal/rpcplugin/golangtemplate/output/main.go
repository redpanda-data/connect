package main

import (
	"context"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/plugin/go/rpcn"
)

type config struct {
}

func main() {
	rpcn.OutputMain(func(cfg config) (output service.BatchOutput, maxInFlight int, batchPolicy service.BatchPolicy, err error) {
		output = &myOutput{cfg: cfg}
		maxInFlight = 1
		return
	})
}

type myOutput struct {
	cfg config
}

var _ service.BatchOutput = (*myOutput)(nil)

// Connect implements service.BatchOutput.
func (m *myOutput) Connect(context.Context) error {
	return nil
}

// WriteBatch implements service.BatchOutput.
func (m *myOutput) WriteBatch(context.Context, service.MessageBatch) error {
	return nil
}

// Close implements service.BatchOutput.
func (m *myOutput) Close(ctx context.Context) error {
	return nil
}
