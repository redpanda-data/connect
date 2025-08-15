package main

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/plugin/go/rpcn"
)

type config struct{}

func main() {
	rpcn.ProcessorMain(func(cfg config) (service.BatchProcessor, error) {
		return &myProcessor{cfg: cfg}, nil
	})
}

type myProcessor struct {
	cfg config
}

var _ service.BatchProcessor = (*myProcessor)(nil)

// ProcessBatch implements service.BatchProcessor.
func (*myProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	return []service.MessageBatch{batch}, nil
}

// Close implements service.BatchProcessor.
func (*myProcessor) Close(context.Context) error {
	return nil
}
