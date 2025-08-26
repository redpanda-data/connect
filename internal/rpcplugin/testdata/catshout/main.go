package main

import (
	"bytes"
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
	for _, m := range batch {
		mBytes, err := m.AsBytes()
		if err != nil {
			return nil, err
		}
		m.SetBytes(append(bytes.ToUpper(mBytes), []byte(" MEOW!")...))
	}
	return []service.MessageBatch{batch}, nil
}

// Close implements service.BatchProcessor.
func (*myProcessor) Close(context.Context) error {
	return nil
}
